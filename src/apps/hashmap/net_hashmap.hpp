///
/// @file net_hashmap.hpp
/// @brief The hashmap reads requests from the network and processes them
///
#pragma once

#include "types.hpp"

#include "PD3/literals/memory_literals.hpp"
#include "PD3/loading_zone/agent.hpp"
#include "garnet_devices/rdma_sync_device/sync_client.hpp"
#include "redy/rdma_client.hpp"
#include "garnet_devices/redy_device/types.hpp"
#include "garnet_devices/redy_device/redy_device.hpp"

#include <cstdint>
#include <cstddef>
#include <cassert>
#include <string>
#include <vector>
#include <queue>
#include <unordered_map>
#include <list>
#include <thread>
#include <atomic>
#include <array>
#include <mutex>

#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <sys/select.h>
#include <arpa/inet.h>
#include <fcntl.h>

#include <rigtorp/SPSCQueue.h>

#include <rdma/rdma_verbs.h>

#include <tbb/concurrent_queue.h>

#define CACHE_MISS_PD3 1
#define CACHE_MISS_RDMA_SYNC 2
#define CACHE_MISS_RDMA_REDY 3
#define CACHE_MISS_RDMA_REDY_SINGLE 4

#define CURRENT_CACHE_MISS_HANDLER CACHE_MISS_RDMA_SYNC

namespace pd3 {
namespace network_hashmap {

using namespace dpf::literals::memory_literals;
static constexpr size_t BUFFER_SIZE = 1_MiB;
static constexpr size_t KEY_SIZE = 8;
static constexpr size_t VALUE_SIZE = 8;
static constexpr size_t READ_SIZE = 16; // key + value size
static constexpr size_t REMOTE_SEGMENT_SIZE = 1_GiB;

// Define fixed sizes for on-wire structs from types.hpp
static constexpr size_t ON_WIRE_REQUEST_SIZE = sizeof(pd3::network_hashmap::Request);
static constexpr size_t ON_WIRE_RESPONSE_SIZE = sizeof(pd3::network_hashmap::Response);

enum class CacheMissHandler : uint8_t {
  NONE = 0,
  PD3,
  RDMA_SYNC,
  REDY,
};

struct Config {
  size_t local_capacity;
  size_t database_size;
  size_t num_threads;
  size_t batch_size;
  std::string server_ip = "0.0.0.0"; // Default IP
  int server_port = 12345;         // Default port
  std::string remote_memory_server_ip = "10.10.1.100";
  std::string remote_memory_server_port = "51216";
};

class NetworkHashMap {

  // forward declarations
  struct ClientState;

  struct RequestContext {
    uint64_t thread_id;
    uint64_t key;
    uint64_t timestamp;
  };

public:

  NetworkHashMap();
  ~NetworkHashMap();

  void Configure(const Config& config);

  void Run();

  void Stop();

  // Helper for network setup
  bool InitializeServerSocket();

  // Helper for RDMA setup
  bool InitializeRdma();


private:

  // New method to handle individual client connections
  void ClientConnectionThread(int client_fd);

  // Modified network thread to only handle accepting connections
  void AcceptorThread();

  // RDMA thread for handling remote requests
  void RdmaThread();

  // Redy worker thread, single core
  void RedySingle();

  // Redy worker thread, normal
  void Redy(ClientState* state);

  // Helper methods for client connection handling
  void HandleClientRead(ClientState* client_state, bool& error);
  void HandleClientRequests(ClientState* client_state);
  void CompactReadBuffer(ClientState* client_state);
  void SendClientData(ClientState* client_state, bool& error);

  int AwaitCompletion();

private:

  Config config_;

  std::mutex threads_mutex_;
  std::vector<std::thread> threads_;
  std::atomic<bool> running_;

  std::unordered_map<uint64_t, uint64_t> local_hashmap_;
  char* local_store_;
  size_t local_store_size_;

  // RDMA resources
  bool use_rdma_ = false;
  struct rdma_cm_id* rdma_cm_id_ = nullptr;
  struct ibv_comp_channel* comp_channel_ = nullptr;
  struct ibv_cq* cq_ = nullptr;
  struct ibv_qp* qp_ = nullptr;

  std::vector<MemoryBuffer> remote_buffers_;

  // remote worker request queue
  struct RemoteRequest {
    uint64_t key;
    uint64_t offset;
    uint64_t segment_id;
    uint32_t thread_id;
  };
  tbb::concurrent_queue<RemoteRequest> remote_request_queue_;

  // per thread state
  struct alignas(64) PerThreadState {
    char* local_buffer;
    struct ibv_mr* local_mr;
    tbb::concurrent_queue<uint64_t> response_queue;
  };
  std::vector<PerThreadState> per_thread_states_;
  uint64_t next_thread_state_idx_ = 0;

  int listen_fd_ = -1;
  struct ibv_pd* pd_;

  struct ClientState {
    int client_fd;
    std::array<char, BUFFER_SIZE * 16> send_buffer;
    size_t send_start = 0;
    size_t send_end = 0;
    std::array<char, BUFFER_SIZE> recv_buffer;
    size_t recv_start = 0;
    size_t recv_end = 0;
    size_t total_received_requests = 0;
    size_t total_processed_requests = 0;
    size_t total_sent_responses = 0;
    size_t thread_state_idx = 0;
    size_t num_keys_ = 0;
    // rdma sync
#if CURRENT_CACHE_MISS_HANDLER == CACHE_MISS_RDMA_SYNC
    std::unique_ptr<dpf::SyncClient> sync_client;
#elif CURRENT_CACHE_MISS_HANDLER == CACHE_MISS_RDMA_REDY_SINGLE
    rigtorp::SPSCQueue<uint64_t>* completion_queue;
    size_t remote_requests = 0;
    size_t remote_sent = 0;
#elif CURRENT_CACHE_MISS_HANDLER == CACHE_MISS_RDMA_REDY
    std::unique_ptr<RedyFrontend> redy_client;
    // tbb::concurrent_queue<uint64_t> completion_queue;
    rigtorp::SPSCQueue<uint64_t>* completion_queue;
    size_t max_outstanding_requests_;
    size_t outstanding_requests_;
    std::atomic_bool running{true};
#elif CURRENT_CACHE_MISS_HANDLER == CACHE_MISS_PD3
    size_t local_consumer_idx = 0;
#else
    // TODO:
#endif
  };
  std::mutex client_states_mutex_;
  std::unordered_map<int, std::unique_ptr<ClientState>> client_states_;

  std::vector<dpf::RemoteMemoryRegion> memory_regions_;

#if CURRENT_CACHE_MISS_HANDLER == CACHE_MISS_RDMA_REDY_SINGLE
  dpf::RdmaClient redy_client_;
  std::vector<RequestBatchFrontEnd> frontend_requests_;
  std::vector<RequestBatchFrontEnd> preflight_requests_;
  size_t producer_ = 0;
  size_t consumer_ = 0;
  size_t preflight_size_ = 0;
  size_t slots_ = 0;

  uint32_t queue_depth_ = 0;
  uint32_t batch_size_ = 0;

  size_t segment_size_ = 1_GiB;

  std::thread redy_worker_;

  tbb::concurrent_queue<SingleRequestFrontEnd> request_queue_;

  void IncrementProducer() {
    producer_ = (producer_ + 1) % preflight_size_;
    slots_--;
  }

  void IncrementConsumer() {
    consumer_ = (consumer_ + 1) % preflight_size_;
    slots_++;
  }
#elif CURRENT_CACHE_MISS_HANDLER == CACHE_MISS_RDMA_REDY
  size_t segment_size_ = 1_GiB;
  uint32_t queue_depth_ = 16;
  uint32_t batch_size_ = 128;
#elif CURRENT_CACHE_MISS_HANDLER == CACHE_MISS_PD3
  dpf::buffer::AgentBuffer* agent_buffer_ = nullptr;
  char* data_buffer_ = nullptr;
#else
    // TODO:
#endif

};

} // namespace network_hashmap
} // namespace pd3
