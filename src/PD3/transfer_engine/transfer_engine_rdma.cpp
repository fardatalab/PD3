#include "transfer_engine.hpp"

#include "PD3/system/logger.hpp"
#include "PD3/buffer/request_buffer.hpp"
#include "PD3/buffer/agent_buffer.hpp"
#include "PD3/buffer/response_buffer.hpp"
#include "types/rdma_protocol.hpp"

#include <infiniband/verbs.h>
#include <system_error>
#include <iostream>
#include <unistd.h>

#include <doca_buf.h>
#include <doca_buf_inventory.h>
#include <doca_dma.h>
#include <doca_error.h>
#include <doca_log.h>

#include <netdb.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <rdma/rdma_verbs.h>

namespace dpf {
namespace offload {

bool TransferEngine::InitializeRdma()
{
  struct rdma_addrinfo hints = {};
  hints.ai_port_space = RDMA_PS_TCP;

  LOG_INFOF("Connecting to server at: {} port: {}", config_.server_addr, config_.server_port);

  // prepare resources
  struct rdma_cm_event* event = nullptr;
  int ret = -1;

  event_channel_ = rdma_create_event_channel();
  if (!event_channel_) {
    LOG_ERROR("failed to create RDMA event channel");
    return false;
  }
  LOG_INFO("RDMA event channel created");

  ret = rdma_create_id(event_channel_, &conn_id_, nullptr, RDMA_PS_TCP);
  if (ret) {
    LOG_ERROR("failed to create RDMA connection id");
    return false;
  }
  LOG_INFO("RDMA connection id created");

  struct sockaddr_in server_addr;
  std::memset(&server_addr, 0, sizeof(server_addr));
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  server_addr.sin_port = htons(std::stoi(config_.server_port));

  struct addrinfo* res;
  ret = getaddrinfo(config_.server_addr.c_str(), nullptr, nullptr, &res);
  if (ret) {
    LOG_ERROR("getaddrinfo failed - invalid hostname or IP address");
    return false;
  }
  memcpy(&server_addr, res->ai_addr, sizeof(struct sockaddr_in));
  freeaddrinfo(res);
  server_addr.sin_port = htons(std::stoi(config_.server_port));

  ret = rdma_resolve_addr(conn_id_, nullptr, (struct sockaddr*)&server_addr, 2000);
  if (ret) {
    LOG_ERRORF("failed to resolve server address: {}", strerror(errno));
    return false;
  }

  ret = rdma_get_cm_event(event_channel_, &event);
  if (ret) {
    LOG_ERROR("failed to get RDMA CM event");
    return false;
  }

  if (event->event != RDMA_CM_EVENT_ADDR_RESOLVED) {
    rdma_ack_cm_event(event);
    LOG_ERRORF("expected RDMA_CM_EVENT_ADDR_RESOLVED, but got {}", rdma_event_str(event->event));
    return false;
  }
  ret = rdma_ack_cm_event(event);
  if (ret) {
    LOG_ERROR("failed to ack RDMA CM event");
    return false;
  }

  ret = rdma_resolve_route(conn_id_, 2000);
  if (ret) {
    LOG_ERROR("failed to resolve RDMA route");
    return false;
  }

  ret = rdma_get_cm_event(event_channel_, &event);
  if (ret) {
    LOG_ERROR("failed to get RDMA CM event");
    return false;
  }
  if (event->event != RDMA_CM_EVENT_ROUTE_RESOLVED) {
    rdma_ack_cm_event(event);
    LOG_ERRORF("expected RDMA_CM_EVENT_ROUTE_RESOLVED, but got {}", rdma_event_str(event->event));
    return false;
  }
  ret = rdma_ack_cm_event(event);
  if (ret) {
    LOG_ERROR("failed to ack RDMA CM event");
    return false;
  }

  LOG_INFOF("Trying to connect to server at: {} port: {}", inet_ntoa(server_addr.sin_addr), ntohs(server_addr.sin_port));

  pd_ = ibv_alloc_pd(conn_id_->verbs);
  if (!pd_) {
    LOG_ERROR("failed to allocate PD");
    return false;
  }
  LOG_INFO("PD allocated");
  // allocate completion queue
  cq_ = ibv_create_cq(conn_id_->verbs, 1024, nullptr, nullptr, 0);
  if (!cq_) {
    LOG_ERROR("failed to create CQ");
    return false;
  }
  LOG_INFO("CQ created");

  ret = ibv_req_notify_cq(cq_, 0);
  if (ret) {
    LOG_ERROR("failed to request notification for completion queue");
    return false;
  }
  LOG_INFO("Completion queue notified");
  
  uint32_t max_sge = 64;
  uint32_t max_send_wr = 64;

  struct ibv_qp_init_attr qp_attr = {};
  std::memset(&qp_attr, 0, sizeof(qp_attr));
  qp_attr.cap.max_recv_sge = 32;
  qp_attr.cap.max_send_sge = 32;
  qp_attr.cap.max_send_wr = 32;
  qp_attr.cap.max_recv_wr = 32;
  qp_attr.qp_type = IBV_QPT_RC;
  qp_attr.send_cq = cq_;
  qp_attr.recv_cq = cq_;

  ret = rdma_create_qp(conn_id_, pd_, &qp_attr);
  if (ret) {
    LOG_ERROR("failed to create QP");
    perror("rdma_create_qp");
    return false;
  }
  LOG_INFO("QP created");

  struct rdma_conn_param conn_param = {};
  std::memset(&conn_param, 0, sizeof(conn_param));
  conn_param.rnr_retry_count = 7;
  conn_param.retry_count = 7;
  conn_param.initiator_depth = 1;
  conn_param.responder_resources = 1;

  ret = rdma_connect(conn_id_, &conn_param);
  if (ret) {
    LOG_ERROR("failed to connect to server");
    return false;
  }
  ret = rdma_get_cm_event(event_channel_, &event);
  if (ret) {
    LOG_ERROR("failed to get RDMA CM event");
    return false;
  }
  if (event->event != RDMA_CM_EVENT_ESTABLISHED) {
    rdma_ack_cm_event(event);
    LOG_ERRORF("expected RDMA_CM_EVENT_ESTABLISHED, but got {}", rdma_event_str(event->event));
    return false;
  }
  ret = rdma_ack_cm_event(event);
  if (ret) {
    LOG_ERROR("failed to ack RDMA CM event");
    return false;
  }

  LOG_INFO("Connection to remote memory server established successfully");

  // create the memory region
  int flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
  mr_ = ibv_reg_mr(pd_, unified_buf_memory_, rdma_buf_mem_size_, flags);
  if (!mr_) {
    LOG_ERROR("failed to register memory region");
    return false;
  }
  LOG_INFO("Memory region registered");

  // create some IO contexts for all the slots
  for (int i = 0; i < config_.num_slots; ++i) {
    RdmaIOContext* io_ctx = new RdmaIOContext();

    void* sge_buf = (void*)unified_slots_[i].req;
    void* reply_sge_buf = (void*)unified_slots_[i].resp;

    io_ctx->buf_address = sge_buf;
    io_ctx->sge.addr = (uint64_t)sge_buf;
    io_ctx->sge.length = config_.slot_size;
    io_ctx->sge.lkey = mr_->lkey;

    io_ctx->recv_buf_address = reply_sge_buf;
    io_ctx->recv_sge.addr = (uint64_t)reply_sge_buf;
    io_ctx->recv_sge.length = config_.slot_size;
    io_ctx->recv_sge.lkey = mr_->lkey;

    // create and set up the receive buffer
    ReceiveBuffer* recv_buffer = new ReceiveBuffer();
    recv_buffer->max_size(config_.slot_size);
    recv_buffer->buffer(reply_sge_buf);
    memset(recv_buffer->buffer(), 0, config_.slot_size);
    io_ctx->recv_buffer = recv_buffer;

    // allocate a send request batch 
    RdmaRequestBatch* request_batch = new RdmaRequestBatch();
    request_batch->requests = new RdmaRequest[128]; // TODO: BATCH SIZE stuff
    request_batch->capacity = 128;
    request_batch->bytes = 0;
    request_batch->size = 0;
    io_ctx->request_batch = request_batch;
    transfer_io_contexts_.push_back(io_ctx);
  }
  // do the same for the prefetch slots
  for (int i = 0; i < config_.num_prefetch_slots; ++i) {
    RdmaIOContext* io_ctx = new RdmaIOContext();
    void* sge_buf = (void*)prefetch_slots_[i].req;
    void* reply_sge_buf = (void*)prefetch_slots_[i].resp;
    
    io_ctx->buf_address = sge_buf;
    io_ctx->sge.addr = (uint64_t)sge_buf;
    io_ctx->sge.length = config_.slot_size;
    io_ctx->sge.lkey = mr_->lkey;

    io_ctx->recv_buf_address = reply_sge_buf;
    io_ctx->recv_sge.addr = (uint64_t)reply_sge_buf;
    io_ctx->recv_sge.length = config_.slot_size;
    io_ctx->recv_sge.lkey = mr_->lkey;

    // create and set up the receive buffer
    ReceiveBuffer* recv_buffer = new ReceiveBuffer();
    recv_buffer->max_size(config_.slot_size);
    recv_buffer->buffer(reply_sge_buf);
    memset(recv_buffer->buffer(), 0, config_.slot_size);
    io_ctx->recv_buffer = recv_buffer;

    // allocate a send request batch
    RdmaRequestBatch* request_batch = new RdmaRequestBatch();
    request_batch->requests = new RdmaRequest[128]; // TODO: BATCH SIZE stuff
    request_batch->capacity = 128;
    request_batch->bytes = 0;
    request_batch->size = 0;
    io_ctx->request_batch = request_batch;
    prefetch_io_contexts_.push_back(io_ctx);
  }

  // exchange items with the server: memory regions, batch buffers, etc.
  LOG_INFOF("IO Contexts created, sending init request to server");
  // sleep for 1 second
  std::this_thread::sleep_for(std::chrono::seconds(1));
  
  // send a request to the server to get memory regions, using the buffer of the first slot
  transfer_io_contexts_[0]->sge.length = sizeof(ProtocolHeader);
  auto protocol_header = reinterpret_cast<ProtocolHeader*>(transfer_io_contexts_[0]->sge.addr);
  protocol_header->header = RequestHeader::C2S_INIT;
  // post the send request and wait for the completion
  if (!PostSend((uint64_t)RequestHeader::C2S_INIT, &transfer_io_contexts_[0]->sge, 1, IBV_SEND_SIGNALED)) {
    return false;
  }
  if (!WaitForCompletion()) {
    return false;
  }
  LOG_INFOF("Init request sent to server, awaiting response");

  transfer_io_contexts_[0]->sge.length = config_.slot_size;
  if (!PostRecv((uint64_t)RequestHeader::S2C_MEMORY_REGIONS, &transfer_io_contexts_[0]->sge, 1)) {
    return false;
  }
  if (!WaitForCompletion()) {
    return false;
  }
  LOG_INFOF("Response received from server");

  // parse the response from the server
  auto data_buffer = reinterpret_cast<char*>(transfer_io_contexts_[0]->buf_address);
  size_t offset = 0;
  auto header = (ProtocolHeader*)(data_buffer + offset);
  if (header->header != RequestHeader::S2C_MEMORY_REGIONS) {
    LOG_ERRORF("expected S2C_MEMORY_REGIONS, but got {}", uint16_t(header->header));
    return false;
  }
  offset += sizeof(RequestHeader);
  auto region_reply = (ReplyMemoryRegions*)(data_buffer + offset);
  auto num_regions = region_reply->num_regions;
  // TODO: validate the number of regions
  offset += sizeof(ReplyMemoryRegions);
  auto region_details = (RegionDetails*)(data_buffer + offset);
  for (uint32_t i = 0; i < num_regions; ++i) {
    mem_regions_.push_back({region_details->base_address, region_details->region_rkey});
    offset += sizeof(RegionDetails);
  }
  LOG_INFOF("Memory regions received from server: {}", num_regions);
  ReplyReceiveBuffers* reply_receive_buffers = (ReplyReceiveBuffers*)(data_buffer + offset);
  auto total_expected_slots = config_.num_slots + config_.num_prefetch_slots;
  if (reply_receive_buffers->num_slots != total_expected_slots) {
    LOG_ERRORF("expected {} receive buffer slots, but got {}", total_expected_slots, reply_receive_buffers->num_slots);
    return false;
  }
  auto remote_receive_buffer_rkey = reply_receive_buffers->buffer_rkey;
  offset += sizeof(ReplyReceiveBuffers);
  // each address is a uint64_t and is laid out sequentially after the ReplyReceiveBuffers struct
  for (uint32_t i = 0; i < config_.num_slots; ++i) {
    auto remote_address = *(uint64_t*)(data_buffer + offset);
    transfer_io_contexts_[i]->recv_buffer->remote_address(remote_address);
    std::cout << "Remote address: " << remote_address << " for transfer slot: " << i << std::endl;
    transfer_io_contexts_[i]->recv_buffer->remote_rkey(remote_receive_buffer_rkey);
    offset += sizeof(uint64_t);
  }
  for (uint32_t i = 0; i < config_.num_prefetch_slots; ++i) {
    auto remote_address = *(uint64_t*)(data_buffer + offset);
    prefetch_io_contexts_[i]->recv_buffer->remote_address(remote_address);
    std::cout << "Remote address: " << remote_address << " for prefetch slot: " << i << std::endl;
    prefetch_io_contexts_[i]->recv_buffer->remote_rkey(remote_receive_buffer_rkey);
    offset += sizeof(uint64_t);
  }
  // send the client receive buffer details to the server
  header->header = RequestHeader::C2S_MEMORY_REGIONS;
  offset = sizeof(ProtocolHeader);
  reply_receive_buffers = (ReplyReceiveBuffers*)(data_buffer + offset);
  reply_receive_buffers->num_slots = total_expected_slots;
  reply_receive_buffers->buffer_rkey = mr_->rkey;
  LOG_INFOF("MR RKEY: {}", mr_->rkey);
  offset += sizeof(ReplyReceiveBuffers);
  for (uint32_t i = 0; i < config_.num_slots; ++i) {
    auto remote_address_buf = (uint64_t*)(data_buffer + offset);
    *remote_address_buf = (uint64_t)transfer_io_contexts_[i]->recv_buffer->buffer();
    offset += sizeof(uint64_t);
  }
  for (uint32_t i = 0; i < config_.num_prefetch_slots; ++i) {
    auto remote_address_buf = (uint64_t*)(data_buffer + offset);
    *remote_address_buf = (uint64_t)prefetch_io_contexts_[i]->recv_buffer->buffer();
    LOG_INFOF("Prefetch slot {}, remote_address: {}, slot.resp {}", i, *remote_address_buf, (uint64_t)prefetch_slots_[i].resp);
    offset += sizeof(uint64_t);
  }
  transfer_io_contexts_[0]->sge.length = offset;
  if (!PostSend((uint64_t)RequestHeader::C2S_MEMORY_REGIONS, &transfer_io_contexts_[0]->sge, 1, IBV_SEND_SIGNALED)) {
    return false;
  }
  if (!WaitForCompletion()) {
    return false;
  }
  
  // since we used the first slot to exchange memory regions, let's rewrite its size, etc.
  transfer_io_contexts_[0]->sge.length = config_.slot_size;
  // add requests for the in-flight messages (TODO: review this)
  // for (uint32_t i = 0; i < total_expected_slots; ++i) {
  //   RdmaIORequest* request = new RdmaIORequest();
  //   request->is_read = true;
  //   request->slot_id = i;
  //   transfer_io_contexts_[i]->request_batch->requests[transfer_io_contexts_[i]->request_batch->size++] = request;
  // }

  return true;
}

bool TransferEngine::PostSend(uint64_t request_id, 
  struct ibv_sge* sg_list, 
  size_t num_sge, 
  uint32_t flags) 
{
  int ret = rdma_post_send(conn_id_, 
            NULL,
            (void*)sg_list->addr, 
            size_t(sg_list->length), 
            mr_,
            int(flags));
  if (ret) {
    LOG_ERROR("failed to post send");
    return false;
  }
  return true;
}

bool TransferEngine::PostRecv(uint64_t request_id, 
  struct ibv_sge* sg_list, 
  size_t num_sge) 
{
  struct ibv_recv_wr wr = {};
  wr.wr_id = request_id;
  wr.sg_list = sg_list;
  wr.num_sge = num_sge;

  struct ibv_recv_wr* bad_recv_wr = nullptr;
  if (ibv_post_recv(conn_id_->qp, &wr, &bad_recv_wr)) {
    LOG_ERROR("failed to post recv");
    return false;
  }
  return true;
}

bool TransferEngine::WaitForCompletion()
{
  struct ibv_wc wc;
  while (true) {
    auto num_events = ibv_poll_cq(cq_, 1, &wc);
    if (num_events < 0) {
      LOG_ERROR("failed to poll CQ");
      return false;
    }
    if (num_events == 0) {
      std::this_thread::yield();
      continue;
    }
    if (wc.status != IBV_WC_SUCCESS) {
      LOG_ERRORF("completion status: {}", ibv_wc_status_str(wc.status));
      return false;
    }
    return true;
  }
}

bool TransferEngine::PostWrite(uint64_t request_id,
  struct ibv_sge* sg_list,
  size_t num_sge,
  uint64_t remote_addr,
  uint32_t remote_rkey,
  uint32_t flags)
{
  struct ibv_send_wr wr = {};
  wr.wr_id = request_id;
  wr.sg_list = sg_list;
  wr.num_sge = num_sge;
  wr.opcode = IBV_WR_RDMA_WRITE;
  wr.send_flags = flags;
  wr.wr.rdma.remote_addr = remote_addr;
  wr.wr.rdma.rkey = remote_rkey;

  struct ibv_send_wr* bad_send_wr = nullptr;
  int error_no = ibv_post_send(conn_id_->qp, &wr, &bad_send_wr);
  if (error_no) {
    LOG_ERRORF("failed to post write: {} (errno: {})", strerror(error_no), error_no);
    LOG_INFOF("PostWrite Parameters causing failure:");
    LOG_INFOF("  wr.wr_id: {}", wr.wr_id);
    LOG_INFOF("  wr.num_sge: {}", wr.num_sge);
    LOG_INFOF("  wr.opcode: IBV_WR_RDMA_WRITE");
    LOG_INFOF("  wr.send_flags: {:#x}", wr.send_flags);
    LOG_INFOF("  wr.wr.rdma.remote_addr: {}", wr.wr.rdma.remote_addr);
    LOG_INFOF("  wr.wr.rdma.rkey: {}", wr.wr.rdma.rkey);
  if (sg_list) {
    LOG_INFOF("  sge[0].addr: {:#x}", sg_list[0].addr);
    LOG_INFOF("  sge[0].length: {}", sg_list[0].length);
    LOG_INFOF("  sge[0].lkey: {:#x}", sg_list[0].lkey);
  }
    LOG_INFOF("  Local MR details:");
    LOG_INFOF("    conn->mr->addr: {:#x}", (uint64_t)mr_->addr);
    LOG_INFOF("    conn->mr->length: {}", mr_->length);
    LOG_INFOF("    conn->mr->lkey: {:#x}", mr_->lkey);
    return false;
  }
  send_slots_--;
  return true;
}


} // namespace offload
} // namespace dpf