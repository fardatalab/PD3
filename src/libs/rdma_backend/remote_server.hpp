#pragma once

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
#include <signal.h>

/**
 * Remote server that creates memory segments and handles requests from the offload server
 */

namespace dpf {

class RemoteServer {

  struct RequestContext {
    offload::RemoteSegmentRequest* request;
    struct ibv_mr* mr;
  };

  struct ResponseContext {
    offload::RemoteSegmentResponse* response;
    struct ibv_mr* mr;
  };

public:

  RemoteServer();
  ~RemoteServer();

  void Configure(const std::string& server_addr, 
                 const std::string& server_port,
                 const std::string& server_name,
                 uint32_t max_wr,
                 size_t segment_size);

  void Run();

  void Stop();

private:

  void WorkerThread();

  // connection set up
  int SetupConnectionChannel();
  int RdmaListen();
  int SetupClientResources();


  void PostRecvBuffer();

  int HandleCompletion(struct ibv_wc* wc);

  void HandleRequest(offload::RemoteSegmentRequest* request);
  void HandleCreateSegment(offload::RemoteSegmentRequest* request);
  void HandleRemoveSegment(offload::RemoteSegmentRequest* request);
  void HandleRemoveAll(offload::RemoteSegmentRequest* request);

  // client stuff
  void CleanupClientResources();

  void SendResponse(ResponseContext* response_context);

private:
  // details
  std::string server_addr_;
  std::string server_port_;
  std::string server_name_;
  uint32_t max_wr_;
  size_t segment_size_;

  // running
  std::atomic_bool running_;

  struct rdma_event_channel* cm_event_channel_;

  // rdma connection context
  struct rdma_addrinfo hints_ = {};
  struct rdma_addrinfo* res_ = nullptr;
  struct rdma_cm_id* listen_id_ = nullptr;
  struct rdma_cm_id* client_id_ = nullptr;
  struct ibv_pd* pd_ = nullptr;
  struct ibv_cq* cq_ = nullptr;

  struct ibv_comp_channel* comp_channel_;
  
  struct SegmentDetails {
    uint64_t details_idx;
    uint64_t offset;
  };
  std::unordered_map<uint64_t, SegmentDetails> segment_mr_map_;

  struct MemoryRegionDetails {
    struct ibv_mr* mr;
    std::queue<uint64_t> free_segments_;
  };

  std::vector<MemoryRegionDetails> segment_mr_list_;
  std::set<struct ibv_mr*> temp_mrs_;

  // private members here
  std::thread worker_thread_;
};

}