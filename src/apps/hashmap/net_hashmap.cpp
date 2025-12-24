#include "net_hashmap.hpp"
// #include "config.hpp"

#include "PD3/system/logger.hpp"
#include "types.hpp"

#include <iostream>
#include <cstring>
#include <pthread.h>
#include <sched.h>
#include <rdma/rdma_verbs.h>
#include <vector>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <sys/select.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <thread>
#include <mutex>
#include <atomic>
#include <netinet/tcp.h>

namespace pd3 {
namespace network_hashmap {

using namespace dpf;

NetworkHashMap::NetworkHashMap() : running_(false) {}

NetworkHashMap::~NetworkHashMap() 
{
  if (running_) {
    Stop();
  }
  if (local_store_) {
    delete[] local_store_;
  }
  for (auto&& thread_state: per_thread_states_) {
    if (thread_state.local_buffer) {
      free(thread_state.local_buffer);
    }
    if (thread_state.local_mr) {
      ibv_dereg_mr(thread_state.local_mr);
    }
  }
  if (rdma_cm_id_) {
    rdma_destroy_id(rdma_cm_id_);
  }
  if (comp_channel_) {
    ibv_destroy_comp_channel(comp_channel_);
  }
  if (cq_) {
    ibv_destroy_cq(cq_);
  }
  if (qp_) {
    ibv_destroy_qp(qp_);
  }
  if (listen_fd_ != -1) {
    close(listen_fd_);
  }
}

void NetworkHashMap::Configure(const Config& config) {
  config_ = config;
  local_store_ = new char[config_.local_capacity];
  std::memset(local_store_, 0, config_.local_capacity);
  local_store_size_ = config_.local_capacity;

  // initialize the local hashmap
  auto num_keys = std::min(config_.database_size, config_.local_capacity) / READ_SIZE; // 16 bytes for key + value
  std::cout << "num_keys: " << num_keys << std::endl;
  // for (auto i = 0; i < num_keys; i++) {
  //   size_t memory_addr = i * VALUE_SIZE;
  //   local_hashmap_.emplace(i, memory_addr);
  // }

#if CURRENT_CACHE_MISS_HANDLER == CACHE_MISS_PD3
  dpf::agent::AgentConfig agent_config;
  agent_config.pcie_address = "e1:00.0";
  agent_config.mmap_export_path = "/data/ssankhe/agent_export_desc";
  agent_config.buf_details_path = "/data/ssankhe/agent_buf_details.txt";
  dpf::agent::Initialize(agent_config);
  agent_buffer_ = dpf::agent::GetInstance().GetAgentBuffer();
  data_buffer_ = agent_buffer_->buffer();
#endif
}

bool NetworkHashMap::InitializeServerSocket() {
    listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd_ < 0) {
        perror("socket creation failed");
        return false;
    }

    int opt = 1;
    if (setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) {
        perror("setsockopt SO_REUSEADDR failed");
        close(listen_fd_);
        listen_fd_ = -1;
        return false;
    }

    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    if (inet_pton(AF_INET, config_.server_ip.c_str(), &server_addr.sin_addr) <= 0) {
        perror("inet_pton failed for server_ip");
        close(listen_fd_);
        listen_fd_ = -1;
        return false;
    }
    server_addr.sin_port = htons(config_.server_port);

    if (bind(listen_fd_, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
        perror("bind failed");
        close(listen_fd_);
        listen_fd_ = -1;
        return false;
    }

    if (listen(listen_fd_, 5) < 0) { // Listen queue size 5
        perror("listen failed");
        close(listen_fd_);
        listen_fd_ = -1;
        return false;
    }

    // Set listen_fd_ to non-blocking for the accept loop in NetworkThread
    int flags = fcntl(listen_fd_, F_GETFL, 0);
    if (flags == -1 || fcntl(listen_fd_, F_SETFL, flags | O_NONBLOCK) == -1) {
        perror("fcntl O_NONBLOCK on listen_fd_ failed");
        close(listen_fd_);
        listen_fd_ = -1;
        return false;
    }

    std::cout << "Server listening on " << config_.server_ip << ":" << config_.server_port << std::endl;
    return true;
}

void NetworkHashMap::Run() {
  if (!InitializeServerSocket()) {
      std::cerr << "Failed to initialize server socket. Aborting Run." << std::endl;
      return;
  }
  
  per_thread_states_.resize(config_.num_threads + 10);
  if (config_.database_size > config_.local_capacity) {
    use_rdma_ = true;
  #if CURRENT_CACHE_MISS_HANDLER == CACHE_MISS_RDMA_REDY_SINGLE
    // start the Redy thread
    redy_worker_ = std::thread([this]() {
      this->RedySingle();
    });
  #endif
  }
  running_ = true;
  
  // Start the acceptor thread
  threads_.emplace_back(&NetworkHashMap::AcceptorThread, this);
}

void NetworkHashMap::Stop() {
  running_ = false;
  if (listen_fd_ != -1) {
      shutdown(listen_fd_, SHUT_RDWR);
      close(listen_fd_);
      listen_fd_ = -1;
  }

  // Clean up all client connections
  {
    std::lock_guard<std::mutex> lock(client_states_mutex_);
    for (auto&& [fd, state] : client_states_) {
      shutdown(fd, SHUT_RDWR);
      close(fd);
    }
    client_states_.clear();
  }

  for (auto& thread : threads_) {
    if (thread.joinable()) {
      thread.join();
    }
  }
  threads_.clear();
}

void NetworkHashMap::AcceptorThread() {
  if (listen_fd_ == -1) {
    std::cerr << "AcceptorThread: Listen socket not initialized." << std::endl;
    return;
  }

  // Pin this thread to a core

  while (running_) {
    struct sockaddr_in client_addr;
    socklen_t client_addr_len = sizeof(client_addr);
    
    int client_fd = accept(listen_fd_, (struct sockaddr*)&client_addr, &client_addr_len);
    
    if (client_fd < 0) {
      if (errno == EWOULDBLOCK || errno == EAGAIN) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        continue;
      }
      if (!running_) break;
      perror("AcceptorThread: accept failed");
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      continue;
    }

    // Set client socket to non-blocking
    int flags = fcntl(client_fd, F_GETFL, 0);
    if (flags == -1 || fcntl(client_fd, F_SETFL, flags | O_NONBLOCK) == -1) {
      perror("AcceptorThread: fcntl O_NONBLOCK failed on client_fd");
      close(client_fd);
      continue;
    }

    // Set TCP_NODELAY for the client socket
    int optval = 1;
    if (setsockopt(client_fd, IPPROTO_TCP, TCP_NODELAY, &optval, sizeof(optval)) < 0) {
        perror("AcceptorThread: setsockopt TCP_NODELAY failed");
        // Continue without it, or close and log error
    }

    // Create client state
    {
      std::lock_guard<std::mutex> lock(client_states_mutex_);
      client_states_[client_fd] = std::make_unique<ClientState>();
      client_states_[client_fd]->client_fd = client_fd;
      client_states_[client_fd]->thread_state_idx = next_thread_state_idx_++ % (config_.num_threads + 10);
#if CURRENT_CACHE_MISS_HANDLER == CACHE_MISS_RDMA_SYNC
      if (use_rdma_) {
        auto sync_client = std::make_unique<dpf::SyncClient>();
        sync_client->Configure(config_.remote_memory_server_ip, config_.remote_memory_server_port);
        if (sync_client->Init(&pd_)) {
          // TODO: log
          std::cerr << "Error while initializing sync client\n";
          continue;
        }
        if (memory_regions_.empty()) {
          if (sync_client->InitializeMemoryRegions(memory_regions_)) {
            std::cerr << "Unable to initialize memory regions\n";
            continue;
          }
        }
        sync_client->SetMemoryRegions(memory_regions_);
        client_states_[client_fd]->sync_client = std::move(sync_client);
      }
#elif CURRENT_CACHE_MISS_HANDLER == CACHE_MISS_RDMA_REDY_SINGLE
      client_states_[client_fd]->completion_queue = new rigtorp::SPSCQueue<uint64_t>(1024);
#elif CURRENT_CACHE_MISS_HANDLER == CACHE_MISS_RDMA_REDY
      auto redy_fe = std::make_unique<RedyFrontend>();
      JSON config;
      config["server_addr"] = "10.10.1.100";
      config["server_port"] = "51216";
      queue_depth_ = 16;
      batch_size_ = 250;
      redy_fe->redy_client.Configure(config);
      redy_fe->redy_client.Init();
      for (int j = 0; j < queue_depth_; ++j) {
        RequestBatchFrontEnd batch(batch_size_);
        redy_fe->inflight_requests.push_back(std::move(batch));
      }
      auto ring_capacity = queue_depth_ * 64 * 2;
      redy_fe->preflight_requests = new RequestBatchRing(ring_capacity, batch_size_);
      auto state_ptr = client_states_[client_fd].get();
      size_t capacity = 128 * 1024 * 64;
      state_ptr->completion_queue = new rigtorp::SPSCQueue<uint64_t>(capacity);
      redy_fe->worker = std::thread([this, state_ptr]() {
       this->Redy(state_ptr);
      });
      state_ptr->redy_client = std::move(redy_fe);
#else 
      // TODO
#endif
    }

    // Start client connection thread
    {
      std::lock_guard<std::mutex> lock(threads_mutex_);
      threads_.emplace_back(&NetworkHashMap::ClientConnectionThread, this, client_fd);
    }

    char client_ip_str[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &client_addr.sin_addr, client_ip_str, INET_ADDRSTRLEN);
    std::cout << "AcceptorThread: Accepted new connection from " 
              << client_ip_str << ":" << ntohs(client_addr.sin_port)
              << " on fd: " << client_fd << std::endl;
  }
}

void NetworkHashMap::ClientConnectionThread(int client_fd) {
  // Pin this thread to a core
  static std::atomic<int> next_core_id{0}; // Start after acceptor thread
  auto core_id = 5 + (next_core_id++ % config_.num_threads);
  std::cout << "ClientConnectionThread: Pinning thread to core " << core_id << std::endl;
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(core_id, &cpuset);
  if (pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset) != 0) {
    std::cerr << "Failed to pin thread to core" << std::endl;
  }

  ClientState* client_state = nullptr;
  {
    std::lock_guard<std::mutex> lock(client_states_mutex_);
    client_state = client_states_[client_fd].get();
  }
  std::cout << "ClientConnectionThread: ThreadIDX: " << client_state->thread_state_idx << std::endl;
  size_t idx = 0;
  while (running_) {

#if CURRENT_CACHE_MISS_HANDLER == CACHE_MISS_RDMA_REDY_SINGLE
    while (running_) {
      uint64_t* ctx_ptr_ptr = nullptr;
      if (client_state->send_end + ON_WIRE_RESPONSE_SIZE > BUFFER_SIZE) {
        break;
      }
      ctx_ptr_ptr = client_state->completion_queue->front();
      if (ctx_ptr_ptr) {
        RequestContext* ctx = reinterpret_cast<RequestContext*>(*ctx_ptr_ptr);
        auto response = reinterpret_cast<Response*>(client_state->send_buffer.data() + client_state->send_end);
        client_state->send_end += ON_WIRE_RESPONSE_SIZE;
        response->key = ctx->key;
        client_state->remote_sent++;
        client_state->completion_queue->pop();
        delete ctx;
#ifdef MEASURE_LATENCY
        response->timestamp = ctx->timestamp;
#endif
        // auto addr = ctx->key * VALUE_SIZE;
        // auto copy_from = addr % (config_.local_capacity - VALUE_SIZE);
        // std::memcpy(response->value, local_store_ + addr, VALUE_SIZE); // mock memcpy
        // delete ctx;
      } else {
        break;
      }
    }
#elif CURRENT_CACHE_MISS_HANDLER == CACHE_MISS_RDMA_REDY
    while (running_) {
      uint64_t* ctx_ptr_ptr = nullptr;
      if (client_state->send_end + ON_WIRE_RESPONSE_SIZE > BUFFER_SIZE) {
        break;
      }
      ctx_ptr_ptr = client_state->completion_queue->front();
      if (ctx_ptr_ptr) {
        // RequestContext* ctx = reinterpret_cast<RequestContext*>(*ctx_ptr_ptr);
        auto response = reinterpret_cast<Response*>(client_state->send_buffer.data() + client_state->send_end);
        client_state->send_end += ON_WIRE_RESPONSE_SIZE;
        response->key = *ctx_ptr_ptr;
#ifdef MEASURE_LATENCY
        response->timestamp = ctx->timestamp;
#endif
        auto addr = response->key * VALUE_SIZE;
        auto copy_from = addr % (config_.local_capacity - VALUE_SIZE);
        std::memcpy(response->value, local_store_ + copy_from, VALUE_SIZE); // mock memcpy
        client_state->completion_queue->pop();
        //delete ctx;
      } else break;
    }
    // std::cout << "Done with phase 1\n";
  //   while (running_) {
  //     uint64_t* key_ptr = nullptr;
  //     if (client_state->send_end + ON_WIRE_RESPONSE_SIZE > BUFFER_SIZE) {
  //       break;
  //     }
  //     key_ptr = client_state->completion_queue->front();
  //     if (key_ptr) {
  //       auto response = reinterpret_cast<Response*>(client_state->send_buffer.data() + client_state->send_end);
  //       client_state->send_end += ON_WIRE_RESPONSE_SIZE;
  //       response->key = *key_ptr;
  //       auto addr = *key_ptr * VALUE_SIZE;
  //       auto copy_from = addr % (config_.local_capacity - VALUE_SIZE);
  //       std::memcpy(response->value, local_store_ + addr, VALUE_SIZE); // mock memcpy
  //       client_state->completion_queue->pop();
        
  //     } else break;
  //  }
#endif


    fd_set read_fds;
    fd_set write_fds;
    FD_ZERO(&read_fds);
    FD_ZERO(&write_fds);

    FD_SET(client_fd, &read_fds);

    // Check if we have data to send
    if (client_state->send_end > client_state->send_start) {
      FD_SET(client_fd, &write_fds);
    }
    struct timeval select_timeout;
    // select_timeout.tv_sec = 0;
    select_timeout.tv_usec = 100; // 0.1ms timeout 
    // select_timeout.tv_usec = 0;
    int activity = select(client_fd + 1, &read_fds, &write_fds, nullptr, &select_timeout);

    if (activity < 0 && errno != EINTR) {
      if (!running_) break;
      perror("ClientConnectionThread: select error");
      break;
    }

    if (!running_) break;

    // Handle incoming data
    bool error = false;
    if (activity > 0 && FD_ISSET(client_fd, &read_fds)) {
      HandleClientRead(client_state, error);
      if (error) {
        break;
      }
    }

    // Process any requests in the buffer
    HandleClientRequests(client_state);
    CompactReadBuffer(client_state);
    // std::cout << "Done with phase 2\n";

#if CURRENT_CACHE_MISS_HANDLER == CACHE_MISS_RDMA_REDY
    auto ring = client_state->redy_client->preflight_requests;
    auto batch = ring->GetAppBatch();
    if (batch->GetBatchSize() > 0) {
      // std::cout << "ClientConnectionThread: Batch size: " << batch->GetBatchSize() << std::endl;
      while (!ring->AppCursorIncrementable()) {
        std::this_thread::yield();
      }
      ring->IncrementAppCursor();
    }
    // std::cout << "Done with phase 3\n";
#endif

    // Handle outgoing data
    if (activity > 0 && FD_ISSET(client_fd, &write_fds)) {
      SendClientData(client_state, error);
      if (error) {
        std::cout << "Error sending data\n";
        break;
      }
      // std::cout << "Done with phase 4\n";
    }

  }

  {
    std::lock_guard<std::mutex> lock(client_states_mutex_);
    std::cout << "ClientConnectionThread: Client fd " << client_state->client_fd << " disconnected." << std::endl;
    std::cout << "Requests processed: " << client_state->total_processed_requests << std::endl;
    std::cout << "Requests sent: " << client_state->total_sent_responses << std::endl;
    // std::cout << "Remote requests: " << client_state->remote_requests << std::endl;
    // std::cout << "Remote sent: " << client_state->remote_sent << std::endl;
  }


  // Clean up
#if CURRENT_CACHE_MISS_HANDLER == CACHE_MISS_RDMA_REDY
  client_state->running = false;
  if (client_state->redy_client->worker.joinable()) {
    client_state->redy_client->worker.join();
  }
  delete client_state->redy_client->preflight_requests;
  delete client_state->completion_queue;
#endif
  {
    std::lock_guard<std::mutex> lock(client_states_mutex_);
    client_states_.erase(client_fd);
  }
  close(client_fd);
  std::cout << "Thread " << pthread_self() << " exiting." << std::endl;
}

void NetworkHashMap::HandleClientRead(ClientState* client_state, bool& error) {
  error = false;
  auto remaining_space = BUFFER_SIZE - client_state->recv_end;
  if (remaining_space == 0) {
    // std::cerr << "ClientConnectionThread: Receive buffer full for fd " << client_state->client_fd << std::endl;
    return;
  }

  auto bytes_read = recv(client_state->client_fd, client_state->recv_buffer.data() + client_state->recv_end, remaining_space, 0);
  if (bytes_read > 0) {
    client_state->total_received_requests += bytes_read / ON_WIRE_REQUEST_SIZE;
    client_state->recv_end += bytes_read;
  } else if (bytes_read == 0) {
    error = true;
  } else {
    if (errno != EWOULDBLOCK && errno != EAGAIN) {
      if (!running_) return;
      perror("ClientConnectionThread: recv error");
      error = true;
    }
    return;
  }
}

void NetworkHashMap::HandleClientRequests(ClientState* client_state) {
  uint64_t requests_processed = 0;
  while (running_) {
    auto remaining_bytes = client_state->recv_end - client_state->recv_start;
    if (remaining_bytes < ON_WIRE_REQUEST_SIZE) {
      break;
    }
    auto& per_thread_state = per_thread_states_[client_state->thread_state_idx];

    Request* current_request = reinterpret_cast<Request*>(client_state->recv_buffer.data() + client_state->recv_start);

    // Process the request
    if (current_request->local) {
      // auto addr = local_hashmap_[current_request->key] % config_.local_capacity;
      // auto addr = (current_request->key * READ_SIZE) % config_.local_capacity;
      auto addr = current_request->key * READ_SIZE;
      if (client_state->send_end + ON_WIRE_RESPONSE_SIZE > BUFFER_SIZE) {
        break;
      }
      auto response = reinterpret_cast<Response*>(client_state->send_buffer.data() + client_state->send_end);
#ifdef MEASURE_LATENCY
      response->timestamp = current_request->timestamp;
#endif
      client_state->send_end += ON_WIRE_RESPONSE_SIZE;
      response->key = current_request->key;
      std::memcpy(response->value, local_store_ + addr + KEY_SIZE, VALUE_SIZE);
    } else {

#if CURRENT_CACHE_MISS_HANDLER == CACHE_MISS_RDMA_SYNC
      if (client_state->send_end + ON_WIRE_RESPONSE_SIZE > BUFFER_SIZE) {
        break;
      }
      auto response = reinterpret_cast<Response*>(client_state->send_buffer.data() + client_state->send_end);
      client_state->send_end += ON_WIRE_RESPONSE_SIZE;
      response->key = current_request->key;
#ifdef MEASURE_LATENCY
      response->timestamp = current_request->timestamp;
#endif

      auto address = current_request->key * READ_SIZE;

      // kick off sync RDMA on this thread
      client_state->sync_client->ReadSync(address, (void*)response->value, READ_SIZE);
#elif CURRENT_CACHE_MISS_HANDLER == CACHE_MISS_RDMA_REDY
      // std::cout << "ClientConnectionThread: Handling request with key: " << current_request->key << std::endl;
      auto ring = client_state->redy_client->preflight_requests;
      RequestBatchFrontEnd* batch = nullptr;
      if (!ring->AppCursorIncrementable()) break;
      if ((batch = ring->GetAppBatch()) == nullptr) {
        break;
      }
      auto address = current_request->key * READ_SIZE;
      uint64_t segment = address / segment_size_;
      auto new_source = address % segment_size_;
      auto local_store_address = address % (config_.local_capacity - READ_SIZE);
      SingleRequestFrontEnd* req = batch->GetRequestByIndex(batch->GetBatchSize());
      req->is_read = true;
      req->segment_id = segment;
      req->offset = new_source;
      req->bytes = READ_SIZE;
      req->callback = nullptr;
      req->app_address = (uint64_t)(local_store_ + local_store_address);
      // auto context = new RequestContext();
      // context->key = current_request->key;
#ifdef MEASURE_LATENCY
      context->timestamp = current_request->timestamp;
#endif
      req->context = (void*)current_request->key;
      batch->IncrementBatchSize();
      if (batch->BatchFull()) {
        // std::cout << "ClientConnectionThread: Batch full, incrementing app cursor" << std::endl;
        while (!ring->AppCursorIncrementable()) {
          std::this_thread::yield();
        }
        ring->IncrementAppCursor();
      }
      client_state->outstanding_requests_++;

#elif CURRENT_CACHE_MISS_HANDLER == CACHE_MISS_RDMA_REDY_SINGLE
      auto address = current_request->key * RECORD_SIZE;
      uint64_t segment = address / segment_size_;
      uint64_t new_source = address % segment_size_;
      SingleRequestFrontEnd req;
      req.is_read = true;
      req.segment_id = segment;
      req.offset = new_source;
      req.bytes = RECORD_SIZE;
      req.callback = nullptr;
      auto context = new RequestContext();
      context->key = current_request->key;
      context->thread_id = client_state->client_fd;
#ifdef MEASURE_LATENCY
      context->timestamp = current_request->timestamp;
#endif
      req.context = context;

      request_queue_.push(std::move(req));

#elif CURRENT_CACHE_MISS_HANDLER == CACHE_MISS_PD3
    if (client_state->send_end + ON_WIRE_RESPONSE_SIZE > BUFFER_SIZE) {
      break;
    }
    auto response = reinterpret_cast<Response*>(client_state->send_buffer.data() + client_state->send_end);
    client_state->send_end += ON_WIRE_RESPONSE_SIZE;
    response->key = current_request->key; 
    bool found = false;
    // std::cout << "Waiting for key: " << current_request->key << std::endl;
    // std::cout << "local_consumer_idx: " << client_state->local_consumer_idx << std::endl;
    while (!found) {

      // auto internal_idx = client_state->local_consumer_idx;
      // internal_idx += sizeof(dpf::buffer::RecordHeader);
      // auto key = *reinterpret_cast<uint64_t*>(agent_buffer_->buffer() + dpf::buffer::AgentBuffer::GetBufferIdx(internal_idx));
      // if (key != current_request->key) {
      //   std::this_thread::yield();
      //   continue;
      // }
      // found = true;
      // client_state->local_consumer_idx += 32;
      
      auto internal_idx = client_state->local_consumer_idx;
      auto record_header = reinterpret_cast<dpf::buffer::RecordHeader*>(data_buffer_ + dpf::buffer::AgentBuffer::GetBufferIdx(internal_idx));
      auto curr_consumer_state = record_header->consumer_state.load(std::memory_order_acquire);
      // if (curr_consumer_state == dpf::buffer::CONSUMER_STATE_COMPLETED) {
      //   client_state->local_consumer_idx += record_header->size;
      //   continue;
      // }
      if (curr_consumer_state != dpf::buffer::CONSUMER_STATE_WRITTEN) {
        std::this_thread::yield();
        continue;
      }
      auto record_header_size = record_header->size;
      if (record_header_size != 32) {
        std::this_thread::yield();
        continue;
      }
      internal_idx += sizeof(dpf::buffer::RecordHeader);
      auto key = *reinterpret_cast<uint64_t*>(data_buffer_ + dpf::buffer::AgentBuffer::GetBufferIdx(internal_idx));
      internal_idx += sizeof(uint64_t);
      // TODO: make this the end pointer
      auto value = *reinterpret_cast<uint64_t*>(data_buffer_ + dpf::buffer::AgentBuffer::GetBufferIdx(internal_idx));
      if (value != 1) {
        std::this_thread::yield();
        continue;
      }
      if (key == 0) {
        std::this_thread::yield();
        continue;
      }
      if (key == current_request->key) {
        // std::cout << "Found key: " << key << std::endl;
        found = true;
      }
      client_state->local_consumer_idx += record_header_size;
      if (found) {
        break;
      }
    }
#else 
    // stuff 
#endif 

      // if (client_state->send_end + ON_WIRE_RESPONSE_SIZE > BUFFER_SIZE) {
      //   break;
      // }
      // auto response = reinterpret_cast<Response*>(client_state->send_buffer.data() + client_state->send_end);
      // client_state->send_end += ON_WIRE_RESPONSE_SIZE;
      // response->key = current_request->key;

      // uint64_t address = current_request->key * VALUE_SIZE;
      // uint64_t segment = address / REMOTE_SEGMENT_SIZE;
      // uint64_t offset = address % REMOTE_SEGMENT_SIZE;
      // uint32_t thread_id = client_state->thread_state_idx;
      // // std::cout << "thread id: " << thread_id << std::endl;
      // remote_request_queue_.push({current_request->key, offset, segment, thread_id});
      // uint64_t response_offset;
      // while (true) {
      //   if (per_thread_state.response_queue.try_pop(response_offset)) {
      //     break;
      //   }
      // }
      // // always at the start of the local buffer
      // std::memcpy(response->value, per_thread_state.local_buffer, VALUE_SIZE);
    }
    // std::cout << "incrementing recv start" << std::endl;
    client_state->recv_start += ON_WIRE_REQUEST_SIZE;
    requests_processed++;
  }
  client_state->total_processed_requests += requests_processed;

}

void NetworkHashMap::CompactReadBuffer(ClientState* client_state) {
  if (client_state->recv_start == client_state->recv_end) {
    client_state->recv_start = 0;
    client_state->recv_end = 0;
  } else if (client_state->recv_start > 0) {
    memmove(client_state->recv_buffer.data(), client_state->recv_buffer.data() + client_state->recv_start, client_state->recv_end - client_state->recv_start);
    client_state->recv_end -= client_state->recv_start;
    client_state->recv_start = 0;
  }
}

void NetworkHashMap::SendClientData(ClientState* client_state, bool& error) {
  if (client_state->send_end < BUFFER_SIZE) {
    // fill until we can
#if CURRENT_CACHE_MISS_HANDLER == CACHE_MISS_RDMA_REDY_SINGLE
    while (running_) {
      uint64_t* ctx_ptr_ptr = nullptr;
      if (client_state->send_end + ON_WIRE_RESPONSE_SIZE > BUFFER_SIZE) {
        break;
      }
      ctx_ptr_ptr = client_state->completion_queue->front();
      if (ctx_ptr_ptr) {
        RequestContext* ctx = reinterpret_cast<RequestContext*>(*ctx_ptr_ptr);
        auto response = reinterpret_cast<Response*>(client_state->send_buffer.data() + client_state->send_end);
        client_state->send_end += ON_WIRE_RESPONSE_SIZE;
        response->key = ctx->key;
        client_state->remote_sent++;
        client_state->completion_queue->pop();
        delete ctx;
#ifdef MEASURE_LATENCY
        response->timestamp = ctx->timestamp;
#endif
        // auto addr = ctx->key * RECORD_SIZE;
        // auto copy_from = addr % (config_.local_capacity - RECORD_SIZE);
        // std::memcpy(response->value, local_store_ + addr + KEY_SIZE, VALUE_SIZE); // mock memcpy
      } else {
        break;
      }
    }
#elif CURRENT_CACHE_MISS_HANDLER == CACHE_MISS_RDMA_REDY
    while (running_) {
      uint64_t* ctx_ptr_ptr = nullptr;
      if (client_state->send_end + ON_WIRE_RESPONSE_SIZE > BUFFER_SIZE) {
        break;
      }
      ctx_ptr_ptr = client_state->completion_queue->front();
      if (ctx_ptr_ptr) {
        // RequestContext* ctx = reinterpret_cast<RequestContext*>(*ctx_ptr_ptr);
        auto response = reinterpret_cast<Response*>(client_state->send_buffer.data() + client_state->send_end);
        client_state->send_end += ON_WIRE_RESPONSE_SIZE;
        response->key = *ctx_ptr_ptr;
#ifdef MEASURE_LATENCY
        response->timestamp = *ctx_ptr_ptr;
#endif
        auto addr = *ctx_ptr_ptr * VALUE_SIZE;
        auto copy_from = addr % (config_.local_capacity - VALUE_SIZE);
        std::memcpy(response->value, local_store_ + copy_from, VALUE_SIZE); // mock memcpy
        client_state->completion_queue->pop();
        // delete ctx;
      } else break;
   }
#endif
  }
  size_t current_send_data_len = client_state->send_end - client_state->send_start;

  if (current_send_data_len > 0) {
    ssize_t bytes_sent = send(client_state->client_fd, client_state->send_buffer.data() + client_state->send_start, current_send_data_len, 0);
    // ssize_t bytes_sent = current_send_data_len;
    if (bytes_sent > 0) {
      size_t num_responses_sent = bytes_sent / ON_WIRE_RESPONSE_SIZE;
      client_state->total_sent_responses += num_responses_sent;
      client_state->send_start += bytes_sent;
      if (client_state->send_start == client_state->send_end) {
        client_state->send_start = 0;
        client_state->send_end = 0;
      }
    } else if (bytes_sent < 0) {
      if (errno != EWOULDBLOCK && errno != EAGAIN) {
        if (!running_) return;
        perror("ClientConnectionThread: send error");
        error = true;
      }
      // If EWOULDBLOCK, can't send more now, will retry
    }
    // compact if needed
    if (client_state->send_start > 0) {
      memmove(client_state->send_buffer.data(), client_state->send_buffer.data() + client_state->send_start, client_state->send_end - client_state->send_start);
      client_state->send_end -= client_state->send_start;
      client_state->send_start = 0;
    }
  }
  // this buffer is managed by another thread for Redy

}

void NetworkHashMap::RdmaThread() {
  auto core_id = config_.num_threads + 8; // TODO: see if this is necessary
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(core_id, &cpuset);
  if (pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset) != 0) {
    std::cerr << "Failed to pin thread to core" << std::endl;
  }
  uint64_t remote_requests = 0;

  struct ibv_wc wc;
  struct ibv_send_wr wr, *bad_wr = nullptr;
  memset(&wr, 0, sizeof(wr));
  struct ibv_sge sge;

  while (running_) {
    RemoteRequest request;
    if (remote_request_queue_.try_pop(request)) {
      if (!running_) break;

      auto thread_id = request.thread_id;
      // std::cout << "Recieved request: " << request.key << " from thread " << thread_id << std::endl;
      
      wr.wr_id = 0;
      wr.opcode = IBV_WR_RDMA_READ;
      wr.num_sge = 1;
      wr.send_flags = IBV_SEND_SIGNALED;
      wr.wr.rdma.remote_addr = (uint64_t)(remote_buffers_[request.segment_id].address + request.offset);
      wr.wr.rdma.rkey = remote_buffers_[request.segment_id].rkey;
      sge.addr = (uint64_t)per_thread_states_[request.thread_id].local_buffer;
      sge.length = RECORD_SIZE;
      sge.lkey = per_thread_states_[request.thread_id].local_mr->lkey;

      wr.sg_list = &sge;
      wr.num_sge = 1;

      auto ret = ibv_post_send(rdma_cm_id_->qp, &wr, &bad_wr);
      if (ret < 0) {
        std::cerr << "RDMAServer: failed to post send" << std::endl;
        return;
      }

      while (running_) {
        ret = ibv_poll_cq(cq_, 1, &wc);
        if (ret < 0) {
          std::cerr << "RDMAServer: failed to poll CQ" << std::endl;
          return;
        }
         if (ret > 0) {
          break;
        }
        if (wc.status != IBV_WC_SUCCESS) {
          std::cerr << "RDMAServer: failed to poll CQ" << std::endl;
          return;
        }
      }
      per_thread_states_[thread_id].response_queue.push(request.offset);
      // std::cout << "Pushed response to thread " << thread_id << std::endl;
    }
  }
}

void NetworkHashMap::RedySingle()
{
  uint64_t executed_requests = 0;
  static std::atomic<int> next_redy_core{0};
  auto core_id = next_redy_core.fetch_add(1);
  core_id += (21 + config_.num_threads);
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(core_id, &cpuset);
  if (pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset) != 0) {
    std::cerr << "Failed to pin thread to core" << std::endl;
  }
  std::cout << "Pinned Redy thread to core: " << core_id << std::endl;
#if CURRENT_CACHE_MISS_HANDLER == CACHE_MISS_RDMA_REDY_SINGLE
  // initialize the rdma client
  uint64_t num_batches = 0;
  uint64_t recvd_batches = 0;
  JSON config;
  config["server_addr"] = "10.10.1.100";
  config["server_port"] = "51216";
  redy_client_.Configure(config);
  redy_client_.Init();
  // set up queues
  queue_depth_ = 16;
  batch_size_ = 128;
  preflight_size_ = queue_depth_ * 4;
  slots_ = preflight_size_;
  producer_ = 0;
  consumer_ = 0;
  for (int i = 0; i < queue_depth_; ++i) {
    RequestBatchFrontEnd fe(batch_size_);
    frontend_requests_.push_back(std::move(fe));
  }
  for (int i = 0; i < preflight_size_; ++i) {
    RequestBatchFrontEnd fe(batch_size_);
    preflight_requests_.push_back(std::move(fe));
  }
  while (running_.load(std::memory_order_relaxed)) {
    // try to fill frontend batches
    while (slots_ > 0) {
      SingleRequestFrontEnd req;
      auto popped = request_queue_.try_pop(req);
      if (popped) {
        auto& curr_batch = preflight_requests_[producer_];
        auto curr_req = curr_batch.GetRequestByIndex(curr_batch.GetBatchSize());
        curr_req->is_read = req.is_read;
        curr_req->segment_id = req.segment_id;
        curr_req->offset = req.offset;
        curr_req->callback = req.callback;
        curr_req->context = req.context;
        curr_req->app_address = req.app_address;
        curr_req->bytes = req.bytes;
        curr_batch.IncrementBatchSize();
        if (curr_batch.BatchFull()) {
          IncrementProducer();
        }
      } else {
        if (preflight_requests_[producer_].GetBatchSize() > 0) {
          IncrementProducer();
        }
        break;
      }
    }
    // poll batches and send out
    for (int i = 0; i < queue_depth_; ++i) {
      auto& dst_batch = frontend_requests_[i];
      if (dst_batch.GetBatchSize() == 0) {
        // this is available
        if (slots_ < preflight_size_) {
          // we have stuff to consume
          auto& src_batch = preflight_requests_[consumer_];
          int size = src_batch.GetBatchSize();
          dst_batch.SetBatchSize(size);
          std::memcpy(dst_batch.GetRequests(), src_batch.GetRequests(), size * sizeof(SingleRequestFrontEnd));
          src_batch.InitBatch();
          IncrementConsumer();

          for (int j = 0; j != size; ++j) {
            SingleRequestFrontEnd* req = dst_batch.GetRequestByIndex(j);
            redy_client_.BatchRequest(i, req->is_read, req->segment_id, req->offset, req->app_address, req->bytes);
          }
          redy_client_.ExecuteBatch(i);
        } 
      } else {
        if (redy_client_.PollMessageAt(i)) {
          executed_requests += dst_batch.GetBatchSize();
          char* data;
          uint32_t bytes_read;
          data = redy_client_.GetResponseAt(i, &bytes_read);
          int num_requests = dst_batch.GetBatchSize();
          uint32_t location = 0;
          for (int j = 0; j != num_requests; ++j) {
            auto req = dst_batch.GetRequestByIndex(j);
            if (req->is_read) {
              // std::memcpy((void*)req->app_address, data + location, req->bytes);
              location += req->bytes;
            }
            // send response back
            RequestContext* ctx = (RequestContext*)req->context;
            auto& client_state = client_states_[ctx->thread_id];
            client_state->completion_queue->push(uint64_t(req->context));
            client_state->remote_requests++;
            // delete ctx;
            // LOG_INFOF("SEnt response at index: {} with key: {}", j, ctx->key);
          }
          redy_client_.RemoveResponseAt(i);
          dst_batch.InitBatch();
        }
      }
    }
  }
#endif
  return;
}

void NetworkHashMap::Redy(ClientState* state)
{
  // pin to a core
  // static std::atomic<int> next_redy_core{0};
  // auto core_id = next_redy_core.fetch_add(1);
  // core_id += 21;
  // cpu_set_t cpuset;
  // CPU_ZERO(&cpuset);
  // CPU_SET(core_id, &cpuset);
  // if (pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset) != 0) {
  //   std::cerr << "Failed to pin thread to core" << std::endl;
  // }
  // std::cout << "Pinned Redy thread to core: " << core_id << std::endl;
#if CURRENT_CACHE_MISS_HANDLER == CACHE_MISS_RDMA_REDY
  size_t batch_count = 0;
  size_t executed_batches = 0;
  auto& redy_fe = state->redy_client;
  auto preflight_ring = redy_fe->preflight_requests;
  while (state->running.load(std::memory_order_relaxed)) {
    for (int i = 0; i < queue_depth_; ++i) {
      auto& dst_batch = redy_fe->inflight_requests[i];
      if (dst_batch.GetBatchSize() == 0) {
        // this is available
        // LOG_INFOF("Batch {} is available", i);
        RequestBatchFrontEnd* src_batch = preflight_ring->GetCacheBatch();
        if (src_batch != nullptr) {
          int size = src_batch->GetBatchSize();
          // for (int j = 0; j != size; ++j) {
          //   auto req = src_batch->GetRequestByIndex(j);
          //   RequestContext* ctx = (RequestContext*)req->context;
          //   state->completion_queue->push(ctx->key);
          //   delete ctx;
          // }
          // src_batch->InitBatch();
          // preflight_ring->IncrementCacheCursor();
          dst_batch.SetBatchSize(size);
          std::memcpy(dst_batch.GetRequests(), src_batch->GetRequests(), size * sizeof(SingleRequestFrontEnd));
          src_batch->InitBatch();
          preflight_ring->IncrementCacheCursor();

          int next_slot = 0;
          for (int j = 0; j != size; ++j) {
            SingleRequestFrontEnd* req = dst_batch.GetRequestByIndex(j);
            redy_fe->redy_client.BatchRequest(i, req->is_read, req->segment_id, req->offset, req->app_address, req->bytes);
            next_slot++;
          }
          // LOG_INFOF("Executed batch #{}", executed_batches++);
          redy_fe->redy_client.ExecuteBatch(i);
        }
      } else { // the size is not 0, we should see if completed
        if (redy_fe->redy_client.PollMessageAt(i)) {
          // LOG_INFOF("Received batch #{}", batch_count++);
          char* data;
          uint32_t bytes_read = 0;
          data = redy_fe->redy_client.GetResponseAt(i, &bytes_read);
          int num_requests = dst_batch.GetBatchSize();
          uint32_t location = 0;
          for (int j = 0; j != num_requests; ++j) {
            auto req = dst_batch.GetRequestByIndex(j);
            if (req->is_read) {
              // std::cout << "ClientConnectionThread: Copying data to app address: " << req->app_address << std::endl;
              std::memcpy((void*)req->app_address, data + location, req->bytes);
              location += req->bytes;
            }
            // RequestContext* ctx = (RequestContext*)req->context;
            // state->completion_queue.push(ctx->key);
            state->completion_queue->push(uint64_t(req->context));
            // LOG_INFO("After push");
          }
          redy_fe->redy_client.RemoveResponseAt(i);
          dst_batch.InitBatch();
        }
      }
    }
  }
#endif
  return;
}

int NetworkHashMap::AwaitCompletion() {
  int ret;
  struct ibv_cq *ev_cq;
  void *ev_ctx;

  ret = ibv_get_cq_event(comp_channel_, &ev_cq, &ev_ctx);
  if (ret) {
    std::cerr << "RDMAServer: failed to get CQ event" << std::endl;
    return ret;
  }
  ibv_ack_cq_events(ev_cq, 1);
  ret = ibv_req_notify_cq(cq_, 0);
  if (ret) {
    std::cerr << "RDMAServer: failed to request CQ notification" << std::endl;
    return ret;
  }
  return 0;
}

} // namespace network_hashmap
} // namespace pd3