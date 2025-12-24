#include <iostream>
#include <vector>
#include <string>
#include <thread>
#include <atomic>
#include <chrono>
#include <cstring> 
#include <mutex>
#include <random>
#include <algorithm>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h> 
#include <netinet/tcp.h>

#include "common/args.hpp"

#include "types.hpp"
#include "req_gen.hpp"
#include "zipf.hpp"

enum class Workload : uint8_t {
  UNKNOWN = 0,
  UNIFORM = 1,
  ZIPF = 2,
  CACHE_MISS = 3,
};

struct LatencyClientConfig {
  Workload workload = Workload::UNKNOWN;
  int num_threads = 0;
  // optional
  uint64_t num_requests_to_send = 0;
  bool leap = false;
  bool cache_miss_rate = 0;
};

class LatencyBenchmarkClient {

  static constexpr size_t DATABASE_SIZE = size_t(16) * size_t(1024) * size_t(1024) * size_t(1024);
  static constexpr size_t LOCAL_MEMORY_SIZE = size_t(1) * 1024 * 1024 * 1024;
  static constexpr size_t VALUE_SIZE = 8;
  static constexpr size_t RECORD_SIZE = 16;
  static constexpr size_t ON_WIRE_REQUEST_SIZE = sizeof(pd3::network_hashmap::Request);
  static constexpr size_t ON_WIRE_RESPONSE_SIZE = sizeof(pd3::network_hashmap::Response);
  static constexpr size_t CACHE_LINE_SIZE = 64;

  struct alignas(CACHE_LINE_SIZE) PerThreadState {
    int sock_fd;
    int core_id;
    char* requests;
    uint64_t num_requests;
    std::vector<uint64_t> latencies;
  };

public:
  
  LatencyBenchmarkClient() = default;

  ~LatencyBenchmarkClient()
  {
    for (auto& per_thread : per_thread_states_) {
      close(per_thread.sock_fd);
      delete[] per_thread.requests;
    }
  }

  void Initialize(const LatencyClientConfig& config)
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
    num_requests_to_send_ *= num_threads_;
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
    // initialize the state now + generate the workloads
    for (auto i = 0; i < num_threads_; ++i) {
      PerThreadState state;
      state.sock_fd = ConnectToServer();
      if (state.sock_fd < 0) {
        throw std::runtime_error("Failed to connect to server");
      }
      state.num_requests = num_requests_to_send_ / num_threads_;
      state.requests = new char[ON_WIRE_REQUEST_SIZE * state.num_requests];
      switch (workload_)
      {
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
      state.core_id = 4 + i; // start core id
      per_thread_states_.push_back(std::move(state));
    }
  }

  void Run()
  {
    for (int i = 0; i < num_threads_; ++i) {
      worker_threads_.emplace_back(&LatencyBenchmarkClient::SendRecvThread, this, std::ref(per_thread_states_[i]));
    }
    // wait for them to finish
    for (auto& t : worker_threads_) { t.join(); }
  }

  void PrintStatistics()
  {
    std::vector<uint64_t> all_latencies;
    for (auto&& per_thread : per_thread_states_) {
      all_latencies.insert(all_latencies.end(), per_thread.latencies.begin(),  per_thread.latencies.end());
    }
    std::sort(all_latencies.begin(), all_latencies.end());
    uint64_t p50_latency = 0;
    uint64_t p50_queue_latency = 0;
    uint64_t p50_rdma_latency = 0;
    if (!all_latencies.empty()) {
      size_t p50_index = all_latencies.size() / 2;
      p50_latency = all_latencies[p50_index];
    }
    // Find P99 latency
    uint64_t p99_latency = 0;
    if (!all_latencies.empty()) {
      size_t p99_index = (all_latencies.size() * 99) / 100;
      p99_latency = all_latencies[p99_index];
    }
    // Find P90 latency
    uint64_t p90_latency = 0;
    if (!all_latencies.empty()) {
      size_t p90_index = (all_latencies.size() * 90) / 100;
      p90_latency = all_latencies[p90_index];
    }
    std::cout << "P99 latency: " << p99_latency << " microseconds" << std::endl;
    std::cout << "P90 latency: " << p90_latency << " microseconds" << std::endl;
    std::cout << "P50 latency: " << p50_latency << " microseconds" << std::endl;
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

  void SendRecvThread(PerThreadState& per_thread_state)
  {
    // Pin this thread to a core
    auto& per_thread_latency = per_thread_state.latencies;
    auto requests_to_send = per_thread_state.num_requests;
    auto send_buffer = per_thread_state.requests;
    auto sock_fd = per_thread_state.sock_fd;

    per_thread_latency.resize(requests_to_send / request_batch_size_);

    static std::atomic<int> next_core_id{40};
    auto core_id = next_core_id++;
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);
    if (pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset) != 0) {
        std::cerr << "Failed to pin thread to core" << std::endl;
    }

    std::cout << "Client Sender Thread " << core_id << ": Starting to send " << requests_to_send 
              << " requests in batches of " << request_batch_size_ << "." << std::endl;

    // Send requests in batches
    auto start_time = std::chrono::high_resolution_clock::now();
    const uint64_t total_size = requests_to_send * ON_WIRE_REQUEST_SIZE;
    uint64_t offset = 0;
    uint64_t recv_offset = 0;
    char* recv_buffer = new char[requests_to_send * ON_WIRE_RESPONSE_SIZE];
    int count = 0;
    while (offset < total_size) {
      auto start = std::chrono::high_resolution_clock::now();
      auto batch_size = request_batch_size_ * sizeof(pd3::network_hashmap::Request);
      size_t sent = 0;
      while (sent < batch_size) {
        auto req = reinterpret_cast<pd3::network_hashmap::Request*>(send_buffer + offset);
        auto ret = send(sock_fd, send_buffer + offset, batch_size - sent, 0);
        if (ret < 0) {
          perror("Client Sender Thread: send failed");
          return;
        }
        if (ret == 0) {
          std::cerr << "Client Sender Thread: send returned 0" << std::endl;
          return;
        }
        sent += ret;
        offset += ret;
      }

      // receive
      uint64_t received = 0;
      auto response_batch_size = request_batch_size_ * ON_WIRE_RESPONSE_SIZE;
      while (received < response_batch_size) {
        auto ret = recv(sock_fd, recv_buffer + recv_offset, response_batch_size, 0);
        if (ret < 0) {
            perror("Client Receiver Thread: recv failed");
            break;
        }
        if (ret == 0) {
            std::cout << "Client Receiver Thread: Server closed connection" << std::endl;
            break;
        }
        recv_offset += ret;
        received += ret;
      }
      auto end = std::chrono::high_resolution_clock::now();
      auto latency = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
      // std::cout << "Latency (" << count << "): " << latency << '\n';
      per_thread_latency[count++] = latency;
      // done with one iteration
    }
    std::cout << "Client Send/Recv Thread " << core_id << " done\n";
  }

  std::string server_ip_ = "10.10.1.101";
  int server_port_ = 12345;
  uint64_t num_requests_to_send_ = 250 * 1024;
  size_t request_batch_size_ = 250;
  int start_core_id_ = 4;
  Workload workload_;
  int num_threads_;
  bool leap_;

  pd3::ReqGen req_gen_;

  std::vector<PerThreadState> per_thread_states_;
  std::vector<std::thread> worker_threads_;

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
  LatencyClientConfig config;
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

  LatencyBenchmarkClient client;
  client.Initialize(config);

  sleep(sleep_time);

  client.Run();
  client.PrintStatistics();
}