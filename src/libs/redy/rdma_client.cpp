#include "rdma_client.hpp"

#include "PD3/system/logger.hpp"
#include "types/rdma_protocol.hpp"

#include <infiniband/verbs.h>
#include <rdma/rdma_verbs.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <rdma/rdma_verbs.h>
#include <sys/socket.h>

namespace dpf {

RdmaClient::LocalBuffer::~LocalBuffer() {
    if (buffer_mr) {
      ibv_dereg_mr(buffer_mr);
    }
    if (buffer) {
      free(buffer);
    }
  }

RdmaClient::~RdmaClient() {
  for (auto conn : connections_) {
    for (auto io_ctx : conn->io_contexts) {
      delete[] io_ctx->request_batch->requests;
      delete io_ctx->request_batch;
      delete io_ctx->recv_buffer;
      delete io_ctx;
    }
    for (auto request : conn->inflight_requests) {
      delete request;
    }
    free(conn->mr->addr);
    ibv_dereg_mr(conn->mr);
    ibv_destroy_cq(conn->cq);
    ibv_destroy_qp(conn->conn_id->qp);
    ibv_dealloc_pd(conn->pd);
    rdma_destroy_id(conn->conn_id);
    rdma_destroy_event_channel(conn->event_channel);
    delete conn;
  }
}

void RdmaClient::Configure(const JSON& config) {
  try {
    queue_depth_ = 16;  // default queue depth of 16
    if (config.contains("queue_depth")) {
      queue_depth_ = config["queue_depth"].get<uint64_t>();
    }
    batch_size_ = 256; // default batch size of 128
    if (config.contains("batch_size")) {
      batch_size_ = config["batch_size"].get<uint64_t>();
    }
    server_addr_ = config["server_addr"].get<std::string>();
    server_port_ = config["server_port"].get<std::string>();
    max_send_wr_ = queue_depth_ * 2;
    max_sge_ = queue_depth_ * 2;

  } catch (std::exception& e) {
    LOG_ERROR("failed to configure rdma client: {}", e.what());
    throw;
  }
}

bool RdmaClient::Init(struct ibv_pd** pd, bool set_pd) {
  RdmaConnection* conn = new RdmaConnection();
  conn->id = connections_.size();
  conn->two_sided_verbs = false;

  // TODO: allow for changing the behavior of the connection

  // make the remote connection
  struct rdma_addrinfo hints = {};
  hints.ai_port_space = RDMA_PS_TCP;

  LOG_INFOF("Connecting to server at: {} port: {}", server_addr_, server_port_);

  // prepare resources
  struct rdma_cm_event* event = nullptr;
  int ret = -1;

  conn->event_channel = rdma_create_event_channel();
  if (!conn->event_channel) {
    LOG_ERROR("failed to create RDMA event channel");
    return false;
  }
  LOG_INFO("RDMA event channel created");

  ret = rdma_create_id(conn->event_channel, &conn->conn_id, nullptr, RDMA_PS_TCP);
  if (ret) {
    LOG_ERROR("failed to create RDMA connection id");
    return false;
  }
  LOG_INFO("RDMA connection id created");

  struct sockaddr_in server_addr;
  std::memset(&server_addr, 0, sizeof(server_addr));
  server_addr.sin_family = AF_INET;
  server_addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  server_addr.sin_port = htons(std::stoi(server_port_));

  struct addrinfo* res;
  ret = getaddrinfo(server_addr_.c_str(), nullptr, nullptr, &res);
  if (ret) {
    LOG_ERROR("getaddrinfo failed - invalid hostname or IP address");
    return false;
  }
  memcpy(&server_addr, res->ai_addr, sizeof(struct sockaddr_in));
  freeaddrinfo(res);
  server_addr.sin_port = htons(std::stoi(server_port_));

  ret = rdma_resolve_addr(conn->conn_id, nullptr, (struct sockaddr*)&server_addr, 2000);
  if (ret) {
    LOG_ERRORF("failed to resolve server address: {}", strerror(errno));
    return false;
  }

  ret = rdma_get_cm_event(conn->event_channel, &event);
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

  ret = rdma_resolve_route(conn->conn_id, 2000);
  if (ret) {
    LOG_ERROR("failed to resolve RDMA route");
    return false;
  }

  ret = rdma_get_cm_event(conn->event_channel, &event);
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

  // allocate protection domain
  // TODO: use a single pd for all clients
  if (!set_pd || !*pd) {
    conn->pd = ibv_alloc_pd(conn->conn_id->verbs);
    if (!conn->pd) {
      LOG_ERROR("failed to allocate PD");
      return false;
    }
    if (set_pd) {
      *pd = conn->pd;
    }
  } else {
    conn->pd = *pd;
  }
  LOG_INFO("PD allocated");
  // allocate completion queue
  conn->cq = ibv_create_cq(conn->conn_id->verbs, 1024, nullptr, nullptr, 0);
  if (!conn->cq) {
    LOG_ERROR("failed to create CQ");
    return false;
  }
  LOG_INFO("CQ created");

  ret = ibv_req_notify_cq(conn->cq, 0);
  if (ret) {
    LOG_ERROR("failed to request notification for completion queue");
    return false;
  }
  LOG_INFO("Completion queue notified");

  struct ibv_qp_init_attr qp_attr = {};
  std::memset(&qp_attr, 0, sizeof(qp_attr));
  qp_attr.cap.max_recv_sge = max_sge_;
  qp_attr.cap.max_send_sge = max_sge_;
  qp_attr.cap.max_send_wr = max_send_wr_;
  qp_attr.cap.max_recv_wr = max_send_wr_;
  qp_attr.qp_type = IBV_QPT_RC;
  qp_attr.send_cq = conn->cq;
  qp_attr.recv_cq = conn->cq;

  ret = rdma_create_qp(conn->conn_id, conn->pd, &qp_attr);
  if (ret) {
    LOG_ERROR("failed to create QP");
    return false;
  }
  LOG_INFO("QP created");

  // finally, connect to the server
  struct rdma_conn_param conn_param = {};
  std::memset(&conn_param, 0, sizeof(conn_param));
  // TODO: what should these be?
  conn_param.rnr_retry_count = 7;
  conn_param.responder_resources = 1;
  conn_param.initiator_depth = 1;

  ret = rdma_connect(conn->conn_id, &conn_param);
  if (ret) {
    LOG_ERROR("failed to connect to server");
    return false;
  }

  ret = rdma_get_cm_event(conn->event_channel, &event);
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

  // create the queue slots
  size_t buf_size = RDMA_BUFFER_SIZE * 2 * queue_depth_;
  void* memory_buffer = malloc(buf_size);
  if (!memory_buffer) {
    LOG_ERROR("failed to allocate memory for queue slots");
    return false;
  }
  memset(memory_buffer, 0, buf_size);
  // create the memory region
  int flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE;
  conn->mr = ibv_reg_mr(conn->pd, memory_buffer, buf_size, flags);
  if (!conn->mr) {
    LOG_ERROR("failed to register memory region");
    return false;
  }
  LOG_INFO("Memory region registered");

  // create an IO context for each queue slot
  for (uint32_t i = 0; i < queue_depth_; ++i) {
    RdmaIOContext* io_ctx = new RdmaIOContext();

    void* sge_buf = (void*)((char*)memory_buffer + (uint64_t)(i * RDMA_BUFFER_SIZE * 2));
    void* reply_sge_buf = (void*)((char*)memory_buffer + (uint64_t)(i * RDMA_BUFFER_SIZE * 2) + RDMA_BUFFER_SIZE);

    io_ctx->buf_address = sge_buf;
    io_ctx->sge.addr = (uint64_t)sge_buf;
    io_ctx->sge.length = RDMA_BUFFER_SIZE;
    io_ctx->sge.lkey = conn->mr->lkey;

    io_ctx->recv_buf_address = reply_sge_buf;
    io_ctx->recv_sge.addr = (uint64_t)reply_sge_buf;
    io_ctx->recv_sge.length = RDMA_BUFFER_SIZE;
    io_ctx->recv_sge.lkey = conn->mr->lkey;

    // create and set up the receive buffer
    ReceiveBuffer* recv_buffer = new ReceiveBuffer();
    recv_buffer->max_size(RDMA_BUFFER_SIZE);
    recv_buffer->buffer(reply_sge_buf);
    memset(recv_buffer->buffer(), 0, RDMA_BUFFER_SIZE);
    io_ctx->recv_buffer = recv_buffer;

    // allocate the send request batch
    RdmaRequestBatch* request_batch = new RdmaRequestBatch();
    request_batch->requests = new RdmaRequest[batch_size_];
    request_batch->capacity = batch_size_;
    request_batch->bytes = 0;
    request_batch->size = 0;

    io_ctx->request_batch = request_batch;
    conn->io_contexts.push_back(io_ctx);
  }

  // exchange items with the server: memory regions, batch buffers, etc.
  LOG_INFOF("IO Contexts created, sending init request to server");

  // sleep for 1 second
  std::this_thread::sleep_for(std::chrono::seconds(1));

  // send a request to the server to get memory regions, using the buffer of the first slot
  conn->io_contexts[0]->sge.length = sizeof(ProtocolHeader);
  auto protocol_header = reinterpret_cast<ProtocolHeader*>(conn->io_contexts[0]->sge.addr);
  protocol_header->header = RequestHeader::C2S_INIT;
  // post the send request and wait for the completion
  if (!PostSend(conn, (uint64_t)RequestHeader::C2S_INIT, &conn->io_contexts[0]->sge, 1, IBV_SEND_SIGNALED)) {
    return false;
  }
  if (!WaitForCompletion(conn)) {
    return false;
  }
  LOG_INFOF("Init request sent to server, awaiting response");

  // receive the response from the server (TODO: make the length a constant)
  conn->io_contexts[0]->sge.length = RDMA_BUFFER_SIZE;
  if (!PostRecv(conn, (uint64_t)RequestHeader::S2C_MEMORY_REGIONS, &conn->io_contexts[0]->sge, 1)) {
    return false;
  }
  if (!WaitForCompletion(conn)) {
    return false;
  }
  LOG_INFOF("Response received from server");

  // parse the response from the server
  auto data_buffer = reinterpret_cast<char*>(conn->io_contexts[0]->buf_address);
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

  // store the region details of each physical memory segment 
  uint32_t segment_id = 0;
  RegionDetails* region_details = nullptr;
  for (uint32_t i = 0; i < num_regions; ++i) {
    region_details = (RegionDetails*)(data_buffer + offset);
    conn->address_to_token[region_details->base_address] = region_details->region_rkey;
    conn->segment_to_address[segment_id++] = region_details->base_address;
    offset += sizeof(RegionDetails);
  }
  LOG_INFOF("Received {} memory regions", num_regions);
  // get the receive buffer addresses for q_depth slots
  ReplyReceiveBuffers* reply_receive_buffers = (ReplyReceiveBuffers*)(data_buffer + offset);
  if (reply_receive_buffers->num_slots != queue_depth_) {
    LOG_ERROR("expected {} receive buffer slots, but got {}", queue_depth_, reply_receive_buffers->num_slots);
    return false;
  }
  auto remote_receive_buffer_rkey = reply_receive_buffers->buffer_rkey;
  offset += sizeof(ReplyReceiveBuffers);
  // each address is a uint64_t and is laid out sequentially after the ReplyReceiveBuffers struct
  for (uint32_t i = 0; i < queue_depth_; ++i) {
    auto remote_address = *(uint64_t*)(data_buffer + offset);
    conn->io_contexts[i]->recv_buffer->remote_address(remote_address);
    std::cout << "Remote address: " << remote_address << " for slot: " << i << std::endl;
    conn->io_contexts[i]->recv_buffer->remote_rkey(remote_receive_buffer_rkey);
    offset += sizeof(uint64_t);
  }

  // send the client's receive buffer details to the server
  header->header = RequestHeader::C2S_MEMORY_REGIONS;
  offset = sizeof(ProtocolHeader);
  reply_receive_buffers = (ReplyReceiveBuffers*)(data_buffer + offset);
  reply_receive_buffers->num_slots = queue_depth_;
  reply_receive_buffers->buffer_rkey = conn->mr->rkey;
  offset += sizeof(ReplyReceiveBuffers);
  for (uint32_t i = 0; i < queue_depth_; ++i) {
    auto remote_address_buf = (uint64_t*)(data_buffer + offset);
    *remote_address_buf = (uint64_t)conn->io_contexts[i]->recv_buffer->buffer();
    offset += sizeof(uint64_t);
  }
  conn->io_contexts[0]->sge.length = offset;
  if (!PostSend(conn, (uint64_t)RequestHeader::C2S_MEMORY_REGIONS, &conn->io_contexts[0]->sge, 1, IBV_SEND_SIGNALED)) {
    return false;
  }
  if (!WaitForCompletion(conn)) {
    return false;
  }

  // since we used the first slot to exchange memory regions, let's rewrite its size, etc.
  conn->io_contexts[0]->sge.length = RDMA_BUFFER_SIZE;
  // add requests for the in-flight messages (TODO: review this)
  for (uint32_t i = 0; i < queue_depth_; ++i) {
    RdmaIORequest* request = new RdmaIORequest();
    request->is_read = true;
    request->slot_id = i;
    conn->inflight_requests.push_back(request);
  }

  if (conn->two_sided_verbs) {
    // we will need to pre-post receives for these, but we probably won't use the two_sided_verbs
    // TODO
    return true;
  }

  conn->send_slots = queue_depth_;
  connections_.push_back(conn);

  LOG_INFO("RdmaClient creating local buffer");
  const uint64_t inner_segment_size = 1 << 25;
  one_sided_buffer_.buffer = malloc(inner_segment_size);
  if (!one_sided_buffer_.buffer) {
    LOG_ERROR("failed to allocate local buffer");
    return false;
  }
  one_sided_buffer_.buffer_mr = ibv_reg_mr(conn->pd, one_sided_buffer_.buffer, inner_segment_size, IBV_ACCESS_LOCAL_WRITE);
  if (!one_sided_buffer_.buffer_mr) {
    LOG_ERROR("failed to register local buffer MR");
    return false;
  }
  LOG_INFO("Local buffer created. Initialization complete");

  return true;
}

bool RdmaClient::BatchRequest(uint32_t slot_id,
                              bool is_read,
                              uint32_t segment_id,
                              uint64_t memory_address,
                              uint64_t app_address,
                              uint64_t bytes)
{
  // initially, we only have one backend memory connection, so the default conn idx is 0
  auto conn = connections_[0];
  auto io_ctx = conn->io_contexts[slot_id];
  if (io_ctx->request_batch->capacity == io_ctx->request_batch->size) {
    LOG_ERROR("request batch is full");
    return false;
  }
  auto after_request_batch_bytes = io_ctx->request_batch->bytes + bytes;
  if (after_request_batch_bytes > RDMA_BUFFER_SIZE) {
    LOG_ERRORF("request batch data is full, current size: {}, new size: {}", io_ctx->request_batch->bytes, after_request_batch_bytes);
    return false;
  }
  auto request = &io_ctx->request_batch->requests[io_ctx->request_batch->size++];
  request->is_read = is_read;
  request->bytes = bytes;
  // we need to get the actual physical address for the memory region
  uint64_t physical_base_address = conn->segment_to_address[segment_id];
  uint64_t physical_memory_address = physical_base_address + memory_address;
  request->remote_addr = physical_memory_address;
  request->app_addr = app_address;
  io_ctx->request_batch->bytes = after_request_batch_bytes;
  return true;
}

bool RdmaClient::BatchCanAccomodateRequest(uint32_t slot_id, uint64_t bytes) const
{
  auto conn = connections_[0];
  auto io_ctx = conn->io_contexts[slot_id];
  auto after_request_batch_bytes = io_ctx->request_batch->bytes + bytes;
  if (after_request_batch_bytes > RDMA_BUFFER_SIZE) {
    return false;
  }
  return true;
}

bool RdmaClient::BatchFull(uint32_t slot_id) const {
  auto conn = connections_[0];
  auto io_ctx = conn->io_contexts[slot_id];
  // TODO: add some configurable buffer size here
  return (io_ctx->request_batch->capacity == io_ctx->request_batch->size) || (io_ctx->request_batch->bytes >= RDMA_BUFFER_SIZE);
}

void RdmaClient::OneSidedRequest(bool is_read, uint32_t segment_id, uint64_t memory_address, uint64_t bytes, char* data) {
  // this is a one sided request (synchronous)
  if (is_read) {
    LOG_INFOF("OneSidedRequest: read request, not implemented");
    return;
  }
  // this is a write request
  auto conn = connections_[0];
  auto base_addr = conn->segment_to_address[segment_id];
  auto remote_addr = base_addr + memory_address;
  auto rkey = conn->address_to_token[base_addr];

  std::memcpy(one_sided_buffer_.buffer, data, bytes);

  struct ibv_sge sge = {};
  sge.addr = (uint64_t)one_sided_buffer_.buffer;
  sge.length = bytes;
  sge.lkey = one_sided_buffer_.buffer_mr->lkey;

  auto req_id = (uint64_t)RequestHeader::C2S_ONE_SIDED_REQUEST;

  struct ibv_send_wr wr = {};
  wr.wr_id = req_id;
  wr.sg_list = &sge;
  wr.num_sge = 1;
  wr.opcode = IBV_WR_RDMA_WRITE;
  wr.send_flags = IBV_SEND_SIGNALED;
  wr.wr.rdma.remote_addr = remote_addr;
  wr.wr.rdma.rkey = rkey;

  bool completed = false;
  struct ibv_send_wr* bad_wr = nullptr;
  if (ibv_post_send(conn->conn_id->qp, &wr, &bad_wr)) {
    LOG_ERROR("failed to post write");
    return;
  }

  struct ibv_wc wc;
  while (!completed) {
    auto num_events = ibv_poll_cq(conn->cq, 1, &wc);
    if (num_events < 0) {
      LOG_ERROR("failed to poll CQ");
      return;
    }
    if (num_events == 0) {
      std::this_thread::yield();
      continue;
    }
    if (wc.status != IBV_WC_SUCCESS) {
      LOG_ERRORF("completion status: {}", ibv_wc_status_str(wc.status));
      return;
    }
    if (wc.opcode == IBV_WC_RDMA_WRITE && wc.wr_id == req_id) {
      completed = true;
      break;
    }
  }

  LOG_INFOF("OneSidedRequest: write request completed");

}

void RdmaClient::ExecuteBatch(uint32_t slot_id) {
  auto conn = connections_[0];
  auto io_ctx = conn->io_contexts[slot_id];
  auto inflight_request = conn->inflight_requests[slot_id];

  RdmaRequestBatch* request_batch = io_ctx->request_batch;
  // request-response protocol over one sided verbs
  if (!conn->one_sided && !conn->two_sided_verbs) [[likely]] {

    // construct the request message
    // format of the message is msg_size | msg_bytes | 1
    auto data_buffer = reinterpret_cast<char*>(io_ctx->buf_address);
    uint64_t offset = sizeof(uint32_t);
    auto request_header = (ProtocolHeader*)(data_buffer + offset);
    request_header->header = RequestHeader::C2S_BATCH_REQUEST;
    offset += sizeof(ProtocolHeader);

    BatchIORequest* payload = reinterpret_cast<BatchIORequest*>(data_buffer + offset);
    payload->num_requests = request_batch->size;
    payload->local_request_ptr = (uint64_t)request_batch;
    offset += sizeof(BatchIORequest);
    // add the requests to the message (TODO: review)
    for (uint32_t i = 0; i < request_batch->size; ++i) {
      auto curr_request = reinterpret_cast<SingleIORequest*>(data_buffer + offset);
      curr_request->address = request_batch->requests[i].remote_addr;
      curr_request->bytes = request_batch->requests[i].bytes;
      curr_request->is_read = request_batch->requests[i].is_read;
      offset += sizeof(SingleIORequest);

      // if not a read, we need to add the data
      if (!curr_request->is_read) {
        auto app_addr = request_batch->requests[i].app_addr;
        auto size = request_batch->requests[i].bytes;
        std::memcpy(data_buffer + offset, (char*)app_addr, size);
        offset += size;
      }
    }

    // set the message size
    *reinterpret_cast<uint32_t*>(data_buffer) = offset - sizeof(uint32_t);
    // set the 1
    *reinterpret_cast<uint32_t*>(data_buffer + offset) = 1;

    while (conn->send_slots == 0) {
      struct ibv_wc wc;
      int ret = ibv_poll_cq(conn->cq, 1, &wc);
      if (ret < 0) {
        LOG_ERRORF("Failed to poll completion queue: {}", ret);
        return;
      }
      if (ret == 0) {
        continue;
      }
      if (wc.status != IBV_WC_SUCCESS) {
        LOG_ERRORF("Completion queue entry status: {}", ibv_wc_status_str(wc.status));
        return;
      }
      if (wc.opcode == IBV_WC_RDMA_WRITE) {
        conn->send_slots++;
      }
    }

    io_ctx->sge.length = offset + sizeof(uint32_t);
    // TODO: add flags with support for inline data
    uint32_t flags = IBV_SEND_SIGNALED;
    if (!PostWrite(conn,
                   (uint64_t)RequestHeader::C2S_BATCH_REQUEST,
                   &io_ctx->sge,
                   1,
                   io_ctx->recv_buffer->remote_address(),
                   io_ctx->recv_buffer->remote_rkey(),
                   flags)) {
      return;
    }
    io_ctx->request_batch->size = 0;
    io_ctx->request_batch->bytes = 0;

  } else {
    // TODO: implement this, but we won't use it for V1
    throw std::runtime_error("not implemented");
  }
}

void RdmaClient::ProcessCompletion(uint32_t slot_id) {
  // this is only for processing completions for one-sided verbs
}

bool RdmaClient::PollMessageAt(uint32_t slot_id) {
  auto conn = connections_[0];
  auto io_ctx = conn->io_contexts[slot_id];
  return io_ctx->recv_buffer->PollMessage();
}

char* RdmaClient::GetResponseAt(uint32_t slot_id, uint32_t* bytes_read) {
  auto conn = connections_[0];
  auto io_ctx = conn->io_contexts[slot_id];
  auto start_data = reinterpret_cast<char*>(io_ctx->recv_buffer->buffer()) + sizeof(uint32_t) + sizeof(BatchIOResponse);
  *bytes_read = reinterpret_cast<uint32_t*>(io_ctx->recv_buffer->buffer())[0];
  return start_data;
}

void RdmaClient::RemoveResponseAt(uint32_t slot_id) {
  auto conn = connections_[0];
  auto io_ctx = conn->io_contexts[slot_id];
  io_ctx->recv_buffer->RemoveMessage();
}

bool RdmaClient::PostSend(RdmaConnection* conn,
                          uint64_t request_id,
                          struct ibv_sge* sg_list,
                          size_t num_sge,
                          uint32_t flags)
{
  // struct ibv_send_wr wr = {};
  // wr.wr_id = request_id;
  // wr.sg_list = sg_list;
  // wr.num_sge = num_sge;
  // wr.opcode = IBV_WR_SEND;
  // wr.send_flags = flags;

  // struct ibv_send_wr* bad_send_wr = nullptr;
  // if (ibv_post_send(conn->conn_id->qp, &wr, &bad_send_wr)) {
  //   LOG_ERROR("failed to post send");
  //   return false;
  // }
  // return true;
  auto protocol_header = reinterpret_cast<ProtocolHeader*>(sg_list->addr);
  int ret = rdma_post_send(conn->conn_id,
                           NULL,
                           (void*)sg_list->addr,
                           size_t(sg_list->length),
                           conn->mr,
                           int(flags));
  if (ret) {
    LOG_ERROR("failed to post send");
    return false;
  }
  return true;
}

bool RdmaClient::PostRecv(RdmaConnection* conn,
                          uint64_t request_id,
                          struct ibv_sge* sg_list,
                          size_t num_sge)
{
  struct ibv_recv_wr wr = {};
  wr.wr_id = request_id;
  wr.sg_list = sg_list;
  wr.num_sge = num_sge;

  struct ibv_recv_wr* bad_recv_wr = nullptr;
  if (ibv_post_recv(conn->conn_id->qp, &wr, &bad_recv_wr)) {
    LOG_ERROR("failed to post recv");
    return false;
  }
  return true;
}

bool RdmaClient::PostWrite(RdmaConnection* conn,
                           uint64_t request_id,
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
  int error_no = ibv_post_send(conn->conn_id->qp, &wr, &bad_send_wr);
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
    LOG_INFOF("    conn->mr->addr: {:#x}", (uint64_t)conn->mr->addr);
    LOG_INFOF("    conn->mr->length: {}", conn->mr->length);
    LOG_INFOF("    conn->mr->lkey: {:#x}", conn->mr->lkey);
    return false;
  }
  conn->send_slots--;
  return true;
}

bool RdmaClient::WaitForCompletion(RdmaConnection* conn)
{
  struct ibv_wc wc;
  while (true) {
    auto num_events = ibv_poll_cq(conn->cq, 1, &wc);
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

} // namespace dpf
