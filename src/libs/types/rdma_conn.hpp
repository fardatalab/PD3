#pragma once

#include "message_buffer.hpp"

#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>

#include <cstdint>
#include <vector>
#include <unordered_map>

namespace dpf {

static constexpr size_t CACHELINE_SIZE = 64;

struct alignas(CACHELINE_SIZE) RdmaRequest {
  // whether the request is a read or a write
  bool is_read; 

  // application address
  uint64_t app_addr;
  // remote address
  uint64_t remote_addr;

  // size of the request in bytes
  uint32_t bytes;
  
};

struct alignas(CACHELINE_SIZE) RdmaRequestBatch {
  // how many requests are in this batch, at most
  int capacity;

  // size in bytes
  uint64_t bytes;

  // size in the number of elements
  int size;

  // list of RDMA requests
  RdmaRequest* requests;
};

struct alignas(CACHELINE_SIZE) RdmaIOContext {
  // memory region for this slot
  struct ibv_mr* mr;
  
  // buffer allocated for this slot
  void* buf_address = nullptr;
  
  // buffer allocated for receiving responses
  void* recv_buf_address = nullptr;

  // scatter gather elements for this slot
  struct ibv_sge sge;
  struct ibv_sge recv_sge;

  // send work request for this slot
  struct ibv_send_wr wr;

  // request batch for this slot
  RdmaRequestBatch* request_batch;
  
  // receive context for this slot
  void* recv_ctxt = nullptr;
  
  // receive buffer
  ReceiveBuffer* recv_buffer;
};

struct alignas(CACHELINE_SIZE) RdmaIORequest {
  // if this a read or a write
  bool is_read;
  
  // size of the request in bytes
  uint32_t bytes; 

  // application address
  uint64_t app_addr;

  int slot_id;
};

struct alignas(CACHELINE_SIZE) RdmaConnection {
  // id of the connection
  uint32_t id;

  // id of the server
  uint32_t server_id;

  // whether  the connection is in 1-sided or 2-sided mode
  bool one_sided;

  // whether we use 2-sided verbs if in 2-sided mode
  bool two_sided_verbs;

  // RDMA state
  struct rdma_cm_id* conn_id;
  struct ibv_pd* pd; 
  struct ibv_cq* cq;
  struct ibv_qp* qp;
  struct rdma_event_channel* event_channel;

  // memory region
  struct ibv_mr* mr;
  
  // io contexts representing requests to the server
  std::vector<RdmaIOContext*> io_contexts;

  // inflight requests
  std::vector<RdmaIORequest*> inflight_requests;
  
  // map from remote address to the access token 
  std::unordered_map<uint64_t, uint32_t> address_to_token;
  // segment map
  std::unordered_map<uint32_t, uint64_t> segment_to_address;
  
  // available send slots
  uint32_t send_slots;

};

struct ServerMemoryRegionContext {
  struct ibv_mr* mr;
  void* base_address;
};

struct RdmaDataSession {
  // id of the session
  int id;

  // RDMA context
  struct ibv_pd* pd;
  struct rdma_cm_id* conn_id;
  struct ibv_cq* cq;
  struct ibv_comp_channel* comp_channel;
  
  struct ibv_mr* mr;
  std::vector<void*> buffers;
  std::vector<struct ibv_sge*> sges;
  std::vector<ReceiveBuffer*> recv_buffers;
  std::vector<void*> reply_buffers;
  std::vector<struct ibv_sge*> reply_sges;

  int queue_depth;

  int send_slots;

  std::vector<ServerMemoryRegionContext*> memory_regions;

  // client connected
  bool client_connected;
};


struct ServerClientState {

};


}