#include <iostream>
#include <vector>
#include <string>
#include <thread>
#include <atomic>
#include <chrono>
#include <cstring> 
#include <algorithm>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h> 
#include <netinet/tcp.h>

#include "common/args.hpp"

#include "types.hpp"
#include "req_gen.hpp"

enum class Workload : uint8_t {
  UNKNOWN = 0,
  UNIFORM = 1,
  ZIPF = 2,
  CACHE_MISS = 3,
};

struct ClientConfig {
  Workload workload = Workload::UNKNOWN;
  int num_threads = 0;
  // optional
  uint64_t num_requests_to_send = 0;
  bool leap = false;
  bool cache_miss_rate = 0;
};

class ThroughputBenchmarkClient {
  
  static constexpr size_t DATABASE_SIZE = size_t(16) * size_t(1024) * size_t(1024) * size_t(1024);
  static constexpr size_t LOCAL_MEMORY_SIZE = size_t(1) * 1024 * 1024 * 1024;
  static constexpr size_t VALUE_SIZE = 8;
  static constexpr size_t RECORD_SIZE = 16;
  static constexpr size_t ON_WIRE_REQUEST_SIZE = sizeof(pd3::network_hashmap::Request);
  static constexpr size_t ON_WIRE_RESPONSE_SIZE = sizeof(pd3::network_hashmap::Response);
  static constexpr size_t CACHE_LINE_SIZE = 64;

  struct alignas(CACHE_LINE_SIZE) PerThreadState {
    int sock_fd;
    int sender_core_id;
    int receiver_core_id;
    char* requests;
    uint64_t num_requests;
  };

public:

  ThroughputBenchmarkClient() = default;
  
  ~ThroughputBenchmarkClient()
  {
    for (auto& per_thread : per_thread_states_) {
      close(per_thread.sock_fd);
      delete[] per_thread.requests;
    }
  }

  void Initialize(const ClientConfig& config)
  {
    workload_ = config.workload;
    leap_ = config.leap;

    if (config.workload == Workload::UNKNOWN) {
      throw std::invalid_argument("workload type must be one of UNIFORM, ZIPF or CACHE_MISS");
    }
    if (config.num_threads == 0) {
      throw std::invalid_argument("num_threads must be >1 and a power of 2");
    }
    if ((config.num_threads & (config.num_threads - 1)) != 0) {
      throw std::invalid_argument("num_threads must be a power of 2");
    }
    num_threads_ = config.num_threads;
    if (config.num_requests_to_send != 0) {
      num_requests_to_send_ = config.num_requests_to_send;
    }
    // initialize req gen
    pd3::ZipfParam params;
    if (config.workload == Workload::ZIPF) {
      params.zipf = true;
      params.total_requests = num_requests_to_send_;
    }
    size_t total_keys = DATABASE_SIZE / RECORD_SIZE;
    size_t local_keys = LOCAL_MEMORY_SIZE / RECORD_SIZE;
    std::cout << "total_keys: " << total_keys << " local_keys: " << local_keys << '\n';
    req_gen_.Initialize(total_keys, local_keys, params);

    for (auto i = 0; i < num_threads_; ++i) {
      PerThreadState state;
      state.sock_fd = ConnectToServer();
      if (state.sock_fd < 0) {
        throw std::runtime_error("Failed to connect to server");
      }
      state.num_requests = num_requests_to_send_ / num_threads_;
      state.requests = new char[ON_WIRE_REQUEST_SIZE * state.num_requests];
      switch (workload_) {
      case Workload::UNIFORM:
        req_gen_.GenerateYCSBUniformRequests(state.requests, state.num_requests, leap_);
        break;
      case Workload::CACHE_MISS:
        req_gen_.GenerateRequestsWithMisses(state.requests, state.num_requests, 0 /*todo*/);
        break;
      case Workload::ZIPF:
        req_gen_.GenerateYCSBZipfRequests(state.requests, state.num_requests, leap_);
        break;
      default:
        std::cerr << "ERROR: got unknown workload parameter\n";
        break;
      }
      state.sender_core_id = start_core_id_ + 2*i;
      state.receiver_core_id = start_core_id_ + 2*i + 1;
      per_thread_states_.push_back(std::move(state));
    }
  }

  void Run()
  {
    for (int i = 0; i < num_threads_; ++i) {
      auto& state = per_thread_states_[i];
      worker_threads_.emplace_back(&ThroughputBenchmarkClient::SenderThreadFunc, this, state.sock_fd, state.num_requests, state.requests, state.sender_core_id);
      worker_threads_.emplace_back(&ThroughputBenchmarkClient::ReceiverThreadFunc, this, state.sock_fd, state.num_requests, state.receiver_core_id);
    }
    auto start_time = std::chrono::high_resolution_clock::now();

    for (auto& thread : worker_threads_) {
      thread.join();
    }
    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::micro> duration_us = end_time - start_time;

    uint64_t total_responses = responses_received_count_.load();
    if (duration_us.count() > 0 && total_responses == num_requests_to_send_) {
      double throughput = (static_cast<double>(total_responses) / duration_us.count()); // Responses per microsecond
      throughput_ = throughput;
      // std::cout << "Client: Throughput: " << throughput << " MOps" << std::endl;
    } else if (total_responses != num_requests_to_send_) {
        std::cerr << "Client: Mismatch in sent requests and received responses." << std::endl;
    }
  }

  void PrintStatistics()
  {
    std::cout << "Client: Throughput: " << throughput_ << " MOps" << std::endl;
  }

private:
  
  int ConnectToServer()
  {
    int sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_fd < 0) {
      perror("Client: Socket creation failed");
      return -1;
    }

    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(server_port_);

    if (inet_pton(AF_INET, server_ip_.c_str(), &server_addr.sin_addr) <= 0) {
      perror("Client: Invalid address/ Address not supported");
      close(sock_fd);
      return -1;
    }

    if (connect(sock_fd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
      perror("Client: Connection Failed");
      close(sock_fd);
      return -1;
    }

    // Set TCP_NODELAY for the client socket
    int optval = 1;
    if (setsockopt(sock_fd, IPPROTO_TCP, TCP_NODELAY, &optval, sizeof(optval)) < 0) {
      perror("Client: setsockopt TCP_NODELAY failed");
    }

    std::cout << "Client: Connected to server " << server_ip_ << ":" << server_port_ << std::endl;
    return sock_fd;
  }

  void SenderThreadFunc(int sock_fd, uint64_t requests_to_send, char* send_buffer, int core_id)
  {
    // Pin this thread to a core
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);
    if (pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset) != 0) {
        std::cerr << "Failed to pin thread to core" << std::endl;
    }

    // Send requests in batches
    auto start_time = std::chrono::high_resolution_clock::now();
    const uint64_t total_size = requests_to_send * ON_WIRE_REQUEST_SIZE;
    uint64_t offset = 0;
    uint64_t timestamp_offset = 0;
    uint64_t count = 0;
    while (offset < total_size) {
      auto remaining_bytes = total_size - offset;
      auto batch_size = request_batch_size_ * sizeof(pd3::network_hashmap::Request);
      auto bytes_to_send = std::min(remaining_bytes, batch_size);
      auto ret = send(sock_fd, send_buffer + offset, bytes_to_send, 0);
      if (ret < 0) {
          perror("Client Sender Thread: send failed");
          return;
      }
      if (ret == 0) {
          std::cerr << "Client Sender Thread: send returned 0" << std::endl;
          return;
      }
      offset += ret;
      count++;
      std::this_thread::sleep_for(std::chrono::microseconds(sleep_micros_));
    }
    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::micro> duration_us = end_time - start_time;
    std::cout << "Client Sender Thread " << core_id << ": Finished sending all requests. Time taken: " << duration_us.count() << " us" << std::endl;
  }

  void ReceiverThreadFunc(int sock_fd, uint64_t requests_to_recv, int core_id)
  {
    // Pin this thread to a core
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);
    if (pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset) != 0) {
        std::cerr << "Failed to pin thread to core" << std::endl;
    }

    const uint64_t total_responses_size = requests_to_recv * ON_WIRE_RESPONSE_SIZE;
    char* recv_buffer = new char[requests_to_recv * ON_WIRE_RESPONSE_SIZE];
    uint64_t offset = 0;
    uint64_t responses_received = 0;

    while (offset < total_responses_size) {
      auto remaining_bytes = total_responses_size - offset;
      auto bytes_to_recv = std::min(remaining_bytes, static_cast<uint64_t>(request_batch_size_));
      auto ret = recv(sock_fd, recv_buffer + offset, bytes_to_recv, 0);
      if (ret < 0) {
          perror("Client Receiver Thread: recv failed");
          break;
      }
      if (ret == 0) {
          std::cout << "Client Receiver Thread: Server closed connection" << std::endl;
          break;
      }
      offset += ret;
      responses_received = offset / ON_WIRE_RESPONSE_SIZE;
    }
    responses_received_count_.fetch_add(responses_received);
  }

private:
  std::string server_ip_ = "10.10.1.101";
  int server_port_ = 12345;
  uint64_t num_requests_to_send_ = 10'000'000;
  int start_core_id_ = 4;
  Workload workload_;
  int num_threads_;
  bool leap_;
  uint64_t sleep_micros_ = 1;

  pd3::ReqGen req_gen_;

  std::vector<PerThreadState> per_thread_states_;
  std::vector<std::thread> worker_threads_;

  double throughput_ = 0.0;

  std::atomic_uint64_t responses_received_count_{0};

  // consts
  const size_t request_batch_size_ = 500;

};

Workload get_workload(const std::string& workload)
{
  std::string lowercase_wkld = workload;
  std::transform(lowercase_wkld.begin(), lowercase_wkld.end(), lowercase_wkld.begin(), ::tolower);
  if (lowercase_wkld == "uniform") {
    return Workload::UNIFORM;
  } else if (lowercase_wkld == "zipf") {
    return Workload::ZIPF;
  } else if (lowercase_wkld == "cache_miss") {
    return Workload::CACHE_MISS;
  } else {
    return Workload::UNKNOWN;
  }
}

int main(int argc, char** argv) 
{
  // cmdline params
  args::ArgumentParser parser("Latency benchmarking client for the hashmap application");
  args::HelpFlag help(parser, "help", "Display this help menu", {'h', "help"});
  args::ValueFlag<uint32_t> num_threads_flag(parser, "num_threads", "Number of client threads", {'t', "threads"});
  args::ValueFlag<std::string> workload_flag(parser, "workload", "Workload (UNIFORM, ZIPF or CACHE_MISS)", {'w', "workload"});  
  args::ValueFlag<uint32_t> sleep_time_flag(parser, "sleep_time", "Sleep Time", {'s', "sleep_time"});
  args::Flag leap_flag(parser, "leap", "Run with LEAP", {'l', "leap"});

  try {
    parser.ParseCLI(argc, argv);
  } catch (const args::Completion& e) {
    std::cerr << e.what() << '\n';
    return 0;
  } catch (const args::Help&) {
    std::cout << parser;
    return 0;
  } catch (const args::ParseError& e) {
    std::cerr << e.what() << '\n';
    std::cerr << parser;
    return 1;
  }
  uint32_t sleep_time = 3;
  ClientConfig config;
  if (num_threads_flag) {
    config.num_threads = args::get(num_threads_flag);
  }
  if (workload_flag) {
    std::string workload = args::get(workload_flag);
    config.workload = get_workload(workload);
  }
  if (leap_flag) {
    config.leap = true;
  }
  if (sleep_time_flag) {
    sleep_time = args::get(sleep_time_flag);
  }

  ThroughputBenchmarkClient client;
  client.Initialize(config);

  sleep(sleep_time);

  client.Run();
  client.PrintStatistics();
}
