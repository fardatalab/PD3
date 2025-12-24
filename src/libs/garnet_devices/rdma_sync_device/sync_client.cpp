#include "sync_client.hpp"

#include "PD3/system/logger.hpp"

#include <thread>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <netdb.h>

namespace dpf {

SyncClient::LocalBuffer::~LocalBuffer() {
  if (buffer_mr) {
    ibv_dereg_mr(buffer_mr);
  }
  if (buffer) {
    free(buffer);
  }
}

SyncClient::~SyncClient()
{
  rdma_destroy_qp(conn_id_);
  rdma_destroy_event_channel(event_channel_);
  ibv_destroy_cq(cq_);
  rdma_destroy_id(conn_id_);
}

void SyncClient::Configure(const std::string& server_addr, const std::string& server_port)
{
  server_port_ = server_port;
  server_addr_ = server_addr;
}

int SyncClient::Init(struct ibv_pd** pd)
{
  int ret;
  struct ibv_wc wc;
  struct ibv_qp_init_attr qp_init_attr;

  struct rdma_addrinfo hints = {};
  hints.ai_port_space = RDMA_PS_TCP;

  LOG_INFOF("Connecting to server at: {} port: {}", server_addr_, server_port_);

  // prepare resources
  struct rdma_cm_event* event = nullptr;

  event_channel_ = rdma_create_event_channel();
  if (!event_channel_) {
    LOG_ERROR("Failed to create RDMA event channel");
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
  server_addr.sin_port = htons(std::stoi(server_port_));
  std::cout << "server_addr.sin_port: " << ntohs(server_addr.sin_port) << '\n';

  struct addrinfo *res;
	ret = getaddrinfo(server_addr_.c_str(), nullptr, nullptr, &res);
	if (ret) {
		LOG_ERROR("getaddrinfo failed - invalid hostname or IP address");
		return false;
	}
	memcpy(&server_addr, res->ai_addr, sizeof(struct sockaddr_in));
	freeaddrinfo(res);
  server_addr.sin_port = htons(std::stoi(server_port_));

  std::cout << "server_addr.sin_port: " << ntohs(server_addr.sin_port) << '\n';


  ret = rdma_resolve_addr(conn_id_, nullptr, (struct sockaddr*)&server_addr, 2000);
  if (ret) {
    LOG_ERROR("failed to resolve address");
    return false;
  }

  ret = rdma_get_cm_event(event_channel_, &event);
  if (ret) {
    LOG_ERROR("failed to get CM event");
    return false;
  }
  if (event->event != RDMA_CM_EVENT_ADDR_RESOLVED) {
    rdma_ack_cm_event(event);
    LOG_ERRORF("expected RDMA_CM_EVENT_ADDR_RESOLVED but got {}", rdma_event_str(event->event));
    return false;
  }
  ret = rdma_ack_cm_event(event);
  if (ret) {
    LOG_ERROR("failed to ack CM event");
    return false;
  }

  ret = rdma_resolve_route(conn_id_, 2000);
  if (ret) {
    LOG_ERROR("failed to resolve route");
    return false;
  }

  ret = rdma_get_cm_event(event_channel_, &event);
  if (ret) {
    LOG_ERROR("failed to get CM event");
    return false;
  }
  if (event->event != RDMA_CM_EVENT_ROUTE_RESOLVED) {
    LOG_ERRORF("expected RDMA_CM_EVENT_ROUTE_RESOLVED but got {}", rdma_event_str(event->event));
    return false;
  }
  ret = rdma_ack_cm_event(event);
  if (ret) {
    LOG_ERROR("failed to ack CM event");
    return false;
  }

  LOG_INFOF("Trying to connect to server at: {} port: {}", inet_ntoa(server_addr.sin_addr), ntohs(server_addr.sin_port));

  if (!*pd) {
    *pd = ibv_alloc_pd(conn_id_->verbs);  
    if (!*pd) {
      LOG_ERROR("failed to allocate PD");
      return false;
    }
    LOG_INFO("PD allocated");
  }
  pd_ = *pd;

  // create the local buffers
  const uint64_t inner_segment_size = 1 << 25;
  read_buffer_.buffer = malloc(inner_segment_size);
  if (!read_buffer_.buffer) {
    LOG_ERROR("failed to allocate read buffer");
    return false;
  }
  read_buffer_.buffer_mr = ibv_reg_mr(*pd, read_buffer_.buffer, inner_segment_size, IBV_ACCESS_LOCAL_WRITE);
  if (!read_buffer_.buffer_mr) {
    LOG_ERROR("failed to register read buffer MR");
    return false;
  }

  write_buffer_.buffer = malloc(inner_segment_size);
  if (!write_buffer_.buffer) {
    LOG_ERROR("failed to allocate write buffer");
    return false;
  }
  write_buffer_.buffer_mr = ibv_reg_mr(*pd, write_buffer_.buffer, inner_segment_size, IBV_ACCESS_LOCAL_WRITE);
  if (!write_buffer_.buffer_mr) {
    LOG_ERROR("failed to register write buffer MR");
    return false;
  }

  // create the completion queue
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

  // create the queue pair
  struct ibv_qp_init_attr qp_attr = {};
  std::memset(&qp_attr, 0, sizeof(qp_attr));
  qp_attr.cap.max_recv_sge = 1;
  qp_attr.cap.max_send_sge = 1;
  qp_attr.cap.max_send_wr = 1024; // TODO: fix
  qp_attr.cap.max_recv_wr = 1024;
  qp_attr.qp_type = IBV_QPT_RC;
  qp_attr.send_cq = cq_;
  qp_attr.recv_cq = cq_;

  ret = rdma_create_qp(conn_id_, *pd, &qp_attr);
  if (ret) {
    LOG_ERROR("failed to create QP");
    return false;
  }
  LOG_INFO("QP created");
  qp_ = conn_id_->qp;

  // connect to the remote memory backend
  struct rdma_conn_param conn_param = {};
  std::memset(&conn_param, 0, sizeof(conn_param));
  conn_param.initiator_depth = 1;
  conn_param.responder_resources = 1;
  conn_param.rnr_retry_count = 7;
  conn_param.retry_count = 8;

  ret = rdma_connect(conn_id_, &conn_param);
  if (ret) {
    LOG_ERROR("failed to connect to remote memory");
    return false;
  }

  ret = rdma_get_cm_event(event_channel_, &event);
  if (ret) {
    LOG_ERROR("failed to get CM event");
    return false;
  }
  if (event->event != RDMA_CM_EVENT_ESTABLISHED) {
    LOG_ERRORF("expected RDMA_CM_EVENT_ESTABLISHED but got {}", rdma_event_str(event->event));
    return false;
  }
  ret = rdma_ack_cm_event(event);
  if (ret) {
    LOG_ERROR("failed to ack CM event");
    return false;
  }
  LOG_INFO("connected to remote memory, connection established successfully");

  return 0;
}

int SyncClient::InitializeMemoryRegions(std::vector<RemoteMemoryRegion>& regions)
{
  using namespace remote_memory_details;
  auto request = new RemoteMemoryDetailsRequest();
  request->req = 0;
  auto request_mr = ibv_reg_mr(pd_, request, sizeof(RemoteMemoryDetailsRequest), IBV_ACCESS_LOCAL_WRITE);
  if (!request_mr ) {
    LOG_ERROR("failed to register request MR");
    return -1;
  }

  auto response = new RemoteMemoryDetails();
  auto response_mr = ibv_reg_mr(pd_, response, sizeof(RemoteMemoryDetails), IBV_ACCESS_LOCAL_WRITE);
  if (!response_mr) {
    LOG_ERROR("failed to register response MR");
    return -1;
  }

  // post recv
  struct ibv_sge recv_sge = {};
  recv_sge.addr = (uint64_t)response;
  recv_sge.length = sizeof(RemoteMemoryDetails);
  recv_sge.lkey = response_mr->lkey;

  struct ibv_recv_wr recv_wr = {};
  recv_wr.wr_id = 0;
  recv_wr.sg_list = &recv_sge;
  recv_wr.num_sge = 1;

  struct ibv_recv_wr* bad_recv_wr = nullptr;
  if (ibv_post_recv(qp_, &recv_wr, &bad_recv_wr)) {
    LOG_ERROR("failed to post recv");
    return -1;
  }
  LOG_INFO("Recv posted");

  // post send
  struct ibv_sge sge = {};
  sge.addr = (uint64_t)request;
  sge.length = sizeof(RemoteMemoryDetailsRequest);
  sge.lkey = request_mr->lkey;

  struct ibv_send_wr send_wr = {};
  send_wr.wr_id = 1;
  send_wr.sg_list = &sge;
  send_wr.num_sge = 1;
  send_wr.opcode = IBV_WR_SEND;
  send_wr.send_flags = IBV_SEND_SIGNALED;

  struct ibv_send_wr* bad_send_wr = nullptr;
  if (ibv_post_send(qp_, &send_wr, &bad_send_wr)) {
    LOG_ERROR("failed to post send");
    return -1;
  }
  LOG_INFO("Send posted");

  bool completed = AwaitCompletions({
    {IBV_WC_SEND, 1},
    {IBV_WC_RECV, 0}
  });
  LOG_INFO("Send and recv completed");
  if (!completed) {
    LOG_ERROR("failed to complete send");
    return -1;
  }

  auto num_memory_regions = response->num_entries;
  LOG_INFOF("Received {} memory regions", num_memory_regions);
  for (uint64_t i = 0; i < num_memory_regions; i++) {
    regions.push_back({
      .addr = response->regions[i].address,
      .rkey = response->regions[i].rkey
    });
    LOG_INFOF("Memory region {}: address = {}, rkey = {}", i, regions[i].addr, regions[i].rkey);
  }

  return 0;
}

void SyncClient::SetMemoryRegions(const std::vector<RemoteMemoryRegion>& regions)
{
  remote_memory_regions_ = regions;
}

int SyncClient::ReadSync(uint64_t source, void* dest, uint32_t length)
{
  uint64_t segment = source / segment_size_;
  auto new_source = source % segment_size_;
  // LOG_INFOF("Source: {}, Segment: {} Address: {} Length: {}", source, segment, new_source, length);

  // kick off an RDMA read on this thread
  struct ibv_sge sge = {};
  sge.addr = (uint64_t)read_buffer_.buffer;
  sge.length = length;
  sge.lkey = read_buffer_.buffer_mr->lkey;

  struct ibv_send_wr wr = {};
  wr.wr_id = 0;
  wr.sg_list = &sge;
  wr.num_sge = 1;
  wr.opcode = IBV_WR_RDMA_READ;
  wr.send_flags = IBV_SEND_SIGNALED;
  wr.wr.rdma.remote_addr = (uint64_t)remote_memory_regions_[segment].addr + new_source;
  wr.wr.rdma.rkey = remote_memory_regions_[segment].rkey;

  bool completed = false;
  struct ibv_send_wr* bad_wr = nullptr;
  if (ibv_post_send(qp_, &wr, &bad_wr)) {
    LOG_ERROR("failed to post read");
    return -1;
  }

  completed = AwaitCompletions({
    {IBV_WC_RDMA_READ, 0}
  });
  if (!completed) {
    LOG_ERROR("failed to complete read");
    return -1;
  }

  std::memcpy(dest, read_buffer_.buffer, length);

  return 0;
}

int SyncClient::WriteSync(uint64_t dest, const void* source, uint32_t length)
{
  uint64_t segment = dest / segment_size_;
  auto new_dest = dest % segment_size_;
  LOG_INFOF("WriteSync: segment: {}, new_dest: {}", segment, new_dest);

  // kick off an RDMA write on this thread
  std::memcpy(write_buffer_.buffer, source, length);

  struct ibv_sge sge = {};
  sge.addr = (uint64_t)write_buffer_.buffer;
  sge.length = length;
  sge.lkey = write_buffer_.buffer_mr->lkey;

  struct ibv_send_wr wr = {};
  wr.wr_id = 1;
  wr.sg_list = &sge;
  wr.num_sge = 1;
  wr.opcode = IBV_WR_RDMA_WRITE;
  wr.send_flags = IBV_SEND_SIGNALED;
  wr.wr.rdma.remote_addr = (uint64_t)remote_memory_regions_[segment].addr + new_dest;
  wr.wr.rdma.rkey = remote_memory_regions_[segment].rkey;

  bool completed = false;
  struct ibv_send_wr* bad_wr = nullptr;
  if (ibv_post_send(qp_, &wr, &bad_wr)) {
    LOG_ERROR("failed to post write");
    return -1;
  }

  completed = AwaitCompletions({
    {IBV_WC_RDMA_WRITE, 1}
  });
  if (!completed) {
    LOG_ERROR("failed to complete write");
    return -1;
  }
  return 0;
}

bool SyncClient::AwaitCompletions(std::set<std::pair<ibv_wc_opcode, uint64_t>> expected_wcs) 
{
  struct ibv_wc wc;

  while (!expected_wcs.empty()) {
    auto num_events = ibv_poll_cq(cq_, 1, &wc);
    if (num_events < 0) {
      LOG_ERROR("failed to poll cq for create segment");
      return false;
    }
    if (num_events == 0) {
      std::this_thread::yield();
      continue;
    }
    if (wc.status != IBV_WC_SUCCESS) {
      LOG_ERRORF("failed to poll cq: status = {}", ibv_wc_status_str(wc.status));
      // LOG_ERROR("failed to poll cq for create segment");
      return false;
    }

    std::pair<ibv_wc_opcode, uint64_t> w = std::make_pair(wc.opcode, wc.wr_id);
    if (expected_wcs.count(w) == 0) {
      LOG_ERROR("obtained unexpected wc");
      return false;
    } else {
      // LOG_INFOF("wc: opcode = {}, wr_id = {}", (uint64_t)wc.opcode, wc.wr_id);
    }

    expected_wcs.erase(w);
  }

  return true;
}

} // namespace dpf