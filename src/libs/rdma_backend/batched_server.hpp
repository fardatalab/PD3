/**
 * The batched memory backend is a server that handles batched requests from the offload engine
 * This server requires some CPU to compute responses for requests
 */
#pragma once


#include "json.h"
#include "PD3/literals/memory_literals.hpp"
#include "types/rdma_conn.hpp"

#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>

#include <thread>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <atomic>
#include <vector>
#include <queue>
#include <set>
#include <signal.h>


namespace dpf {

class BatchedMemoryBackend {

  static constexpr size_t MEMORY_REGION_SIZE = 1_GiB;
  static constexpr size_t RDMA_BUFFER_SIZE = 8192 * 4;

public:
  BatchedMemoryBackend();
  ~BatchedMemoryBackend();

  void Configure(const JSON& config);

  void Run();
  void RunAync();

  void Stop();

private:

  void HandleClientDataPlane(RdmaDataSession* session);

  int SetupConnectionChannel();
  int RdmaListen();
  int SetupClientResources(struct rdma_cm_id* id);

  int CheckConnectionEvent(rdma_cm_event_type expected_event);
  int ListenAndDispatchConnectionEvents(struct rdma_cm_id*& id, rdma_cm_event_type expected_event);

  bool PostReceive(RdmaDataSession* session,
                   uint64_t request_id,
                   struct ibv_sge* sg_list,
                   size_t num_sge);

  bool PostSend(RdmaDataSession* session,
               uint64_t request_id,
               struct ibv_sge* sg_list,
               size_t num_sge,
               int flags);

  bool PostWrite(RdmaDataSession* session,
                uint64_t request_id,
                struct ibv_sge* sg_list,
                size_t num_sge,
                uint64_t remote_addr,
                uint32_t remote_rkey,
                int flags);

  bool WaitForCompletion(RdmaDataSession* session);

private:
  // configuration parameters
  std::string server_addr_ = "";
  std::string server_port_ = "";
  std::string server_name_ = "";
  uint32_t max_wr_ = 1024;
  uint32_t init_regions_ = 16;
  uint32_t num_cores_ = 1;
  uint64_t queue_depth_ = 16;

  std::atomic_bool running_;

  // RDMA server context
  struct rdma_addrinfo hints_ = {};
  struct rdma_addrinfo* res_ = nullptr;
  struct rdma_event_channel* cm_event_channel_ = nullptr;
  struct rdma_cm_id* listen_id_ = nullptr;
  struct ibv_pd* pd_ = nullptr; // shared amongst all clients

  std::vector<RdmaDataSession*> client_sessions_;

  // memory
  std::vector<ServerMemoryRegionContext*> memory_regions_;

  // CPU affinity
  std::unordered_set<uint32_t> occupied_cores_;
  std::mutex occupied_cores_lock_;

  // worker thread
  std::thread worker_thread_;
  std::vector<std::thread> data_threads_;
};

} // namespace dpf
