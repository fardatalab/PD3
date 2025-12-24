
/**
 * Implements a server where a precreated number of segments are created at start-up and sent over to the first connecting client
 */
#pragma once

#include "types.hpp"

#include "PD3/transfer_engine/types.hpp"

#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>
#include <rdma/rdma_verbs.h>

#include <thread>
#include <string>
#include <unordered_map>
#include <atomic>
#include <vector>
#include <queue>
#include <set>
#include <mutex>

#include <signal.h>

namespace dpf {

class MemoryPreallocatedServer
{

struct RemoteBuffer {
  char* ptr = nullptr;
  struct ibv_mr* mr = nullptr;
  
  RemoteBuffer() = default;
  int Init(struct ibv_pd* pd, size_t size);
  ~RemoteBuffer();
};

struct ClientState {
  struct rdma_cm_id* client_id = nullptr;
  struct ibv_cq* cq = nullptr;
  std::atomic_bool running{false};
  RemoteBuffer remote_buffer;
  remote_memory_details::RemoteMemoryDetails* remote_memory_details = nullptr;
};

struct MemoryRegionDetails {
  struct ibv_mr* mr;
  void* ptr;
};

public:
  
  MemoryPreallocatedServer();
  ~MemoryPreallocatedServer();

  void Configure(const std::string& server_addr,
                 const std::string& server_port,
                 const std::string& server_name,
                 uint32_t wax_wr,
                 size_t segment_size,
                 size_t num_segments);
  
  void Run();

  void Stop();

private:

  void ServerThread();
  void ClientHandlerThread(ClientState* client_state);

  // helpers
  int SetupConnectionChannel();
  int RdmaListen();
  int InitializeMemoryRegions();
  int SetupClientResources(struct rdma_cm_id* client_id);
  int ListenAndDispatchConnectionEvents(struct rdma_cm_id*& id, rdma_cm_event_type expected_event);
  int HandleCompletion(struct ibv_wc* wc, ClientState* client_state);

  // client communication
  int PostRecv(ClientState* client_state);
  int SendMemoryRegions(ClientState* client_state);

  // clean up 
  void CleanupGeneralResources();
  void CleanupClientResources(ClientState* state);
  
private:
  
  // details
  std::string server_addr_;
  std::string server_port_;
  std::string server_name_;
  uint32_t max_wr_;
  size_t segment_size_;
  size_t num_segments_;

  // running
  std::atomic_bool running_;

  struct rdma_event_channel* cm_event_channel_;

  // rdma connection context
  struct rdma_addrinfo hints_ = {};
  struct rdma_addrinfo* res_ = nullptr;
  struct rdma_cm_id* listen_id_ = nullptr;
  struct ibv_pd* pd_ = nullptr;

  std::vector<ClientState*> connected_clients_;
  
  std::vector<MemoryRegionDetails> segment_mr_list_;

  std::thread server_thread_;
  std::vector<std::thread> worker_threads_;

};

}
