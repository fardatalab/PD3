#include "remote_server.hpp"

#include "config.h"
#include "PD3/system/logger.hpp"

#include <system_error>
#include <iostream>
#include <cstring>

#include <netdb.h>
#include <netinet/in.h>	
#include <arpa/inet.h>
#include <sys/socket.h>

#include <fcntl.h>
#include <unistd.h>
#include <arpa/inet.h>

namespace dpf {

RemoteServer::RemoteServer() 
{
  std::memset(&hints_, 0, sizeof(hints_));
}

RemoteServer::~RemoteServer() {
  // clean up resources
  for (auto& details : segment_mr_list_) {
    free(details.mr->addr);
    ibv_dereg_mr(details.mr);
  }
  for (auto& mr : temp_mrs_) {
    ibv_dereg_mr(mr);
  }
  segment_mr_map_.clear();
  // clean up more resources
  rdma_destroy_id(listen_id_);
  rdma_freeaddrinfo(res_);
}

void RemoteServer::Configure(const std::string& server_addr, 
                             const std::string& server_port,
                             const std::string& server_name,
                             uint32_t max_wr,
                             size_t segment_size) 
{ 
  server_addr_ = server_addr;
  server_port_ = server_port;
  server_name_ = server_name;
  max_wr_ = max_wr;
  segment_size_ = segment_size;
  LOG_INFOF("Configured server with addr: {} port: {} name: {} max_wr: {} segment_size: {}", 
            server_addr_, server_port_, server_name_, max_wr_, segment_size_);
}

void RemoteServer::Run() {
  running_ = true;
  std::thread t([this]() {
    this->WorkerThread();
  });
  worker_thread_ = std::move(t);
}

void RemoteServer::Stop()
{
  running_.store(false, std::memory_order_relaxed);
  if (worker_thread_.joinable()) {
    worker_thread_.join();
  }
}

void RemoteServer::WorkerThread() {
  // this function runs the server
  hints_.ai_flags = RAI_PASSIVE;
  hints_.ai_port_space = RDMA_PS_TCP;

  // init resources
  auto ret = rdma_getaddrinfo(server_addr_.c_str(), server_port_.c_str(), &hints_, &res_);
  if (ret != 0) {
    LOG_ERRORF("Failed to get address info: {}", ret);
    return;
  }

  ret = SetupConnectionChannel();
  if (ret != 0) {
    LOG_ERRORF("Failed to setup connection channel: {}", ret);
    return;
  }

  ret = RdmaListen();
  if (ret != 0) {
    LOG_ERRORF("Failed to listen: {}", ret);
    return;
  }

  while (running_.load(std::memory_order_relaxed)) {
    LOG_INFO("Waiting for client connection");
    // outer loop
    ret = SetupClientResources();
    if (ret != 0) {
      LOG_ERRORF("Failed to setup client resources: {}", ret);
      continue;
    }

    if (!running_) {
      break;
    }

    struct rdma_conn_param conn_param = {};
    std::memset(&conn_param, 0, sizeof(conn_param));
    conn_param.initiator_depth = 1;
    conn_param.responder_resources = 1;
    conn_param.rnr_retry_count = 7;

    ret = rdma_accept(client_id_, &conn_param);
    if (ret != 0) {
      LOG_ERRORF("Failed to accept connection: {}", ret);
      return;
    }

    struct rdma_cm_event* cm_event = nullptr;
    while (running_.load(std::memory_order_relaxed)) { // Inner check for shutdown
      ret = rdma_get_cm_event(cm_event_channel_, &cm_event);
      if (ret == 0) {
          break; // Got an event
      } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
          // No event yet, sleep briefly to avoid busy-waiting
          usleep(10000);
          continue;
      } else {
          // Real error
          LOG_ERRORF("Failed to get CM event while waiting for connection: {}", strerror(errno));
          return;
      }
    }
    if (!running_.load(std::memory_order_relaxed)) {
      return;
    }

    if (cm_event->event != RDMA_CM_EVENT_ESTABLISHED) {
      LOG_ERRORF("unexpected event: {}", rdma_event_str(cm_event->event));
      rdma_ack_cm_event(cm_event);
      return;
    }
    // if (0 != cm_event->status) {
    //   rdma_ack_cm_event(cm_event);
    //   throw std::runtime_error("Failed to get CM event");
    // }
    // if (cm_event->event != RDMA_CM_EVENT_ESTABLISHED) {
    //   LOG_ERRORF("unexpected event: {}", rdma_event_str(cm_event->event));
    //   rdma_ack_cm_event(cm_event);
    //   return;
    // }
    LOG_INFO("RDMA connection established");
    ret = rdma_ack_cm_event(cm_event);
    if (ret) {
      return;
    }

    bool client_connected = true;
    const int POLL_BATCH_SIZE = 1024;
    struct ibv_wc wc[POLL_BATCH_SIZE];
    while (running_.load(std::memory_order_relaxed) && client_connected) {

      // check that the client is still connected
      ret = rdma_get_cm_event(cm_event_channel_, &cm_event);
      if (ret == 0) {
        if (cm_event->id == client_id_ && cm_event->event == RDMA_CM_EVENT_DISCONNECTED) {
          client_connected = false;
          LOG_INFOF("Client disconnected, recieved event: {}", rdma_event_str(cm_event->event));
          rdma_ack_cm_event(cm_event);
        } else {
          LOG_WARNF("unexpected event: {}", rdma_event_str(cm_event->event));
          rdma_ack_cm_event(cm_event);
        }
      } else if (errno != EAGAIN) {
        LOG_ERRORF("Failed to get CM event: {}", ret);
        client_connected = false;
      }
      if (!client_connected) {
        break;
      }

      // poll the completion queue
      int num_events = ibv_poll_cq(cq_, POLL_BATCH_SIZE, wc);
      if (num_events < 0) {
        LOG_ERRORF("Failed to poll completion queue: {}", ret);
        client_connected = false;
        break;
      }
      for (int i = 0; i < num_events; i++) {
        LOG_INFOF("Handling event: {}", i);
        auto out = HandleCompletion(&wc[i]);
        if (out == -1) {
          client_connected = false;
          break;
        }
      }
      if (num_events == 0) {
        // sleep for a small bit
        usleep(10000);
      }
    } // client handling loop
    CleanupClientResources();

  } // end of outer loop

  CleanupClientResources();
  LOG_INFO("Shutting down server");
} 

int RemoteServer::SetupConnectionChannel() {
  // create the event channel explicitly
  cm_event_channel_ = rdma_create_event_channel();
  if (!cm_event_channel_) {
    LOG_ERROR("Failed to create event channel");
    return -1;
  }

  int flags = fcntl(cm_event_channel_->fd, F_GETFL);
  if (flags == -1) {
      LOG_ERRORF("Failed to get RDMA event channel flags: {}", strerror(errno));
      rdma_destroy_event_channel(cm_event_channel_); // Clean up
      cm_event_channel_ = nullptr;
      rdma_freeaddrinfo(res_);
      res_ = nullptr;
      return -1;
  }
  if (fcntl(cm_event_channel_->fd, F_SETFL, flags | O_NONBLOCK) == -1) {
      LOG_ERRORF("Failed to set RDMA event channel to non-blocking: {}", strerror(errno));
      rdma_destroy_event_channel(cm_event_channel_); // Clean up
      cm_event_channel_ = nullptr;
      rdma_freeaddrinfo(res_);
      res_ = nullptr;
      return -1;
  }
  LOG_INFO("RDMA event channel created and set to non-blocking.");
  return 0;
}

int RemoteServer::RdmaListen() {
  // create the listen id
  struct sockaddr_in server_addr;
  server_addr.sin_family = AF_INET;
  struct addrinfo *res;
	int ret = -1;
	ret = getaddrinfo(server_addr_.c_str(), NULL, NULL, &res);
	if (ret) {
		LOG_ERRORF("getaddrinfo failed - invalid hostname or IP address: {}", ret);
		return ret;
	}
	memcpy(&server_addr, res->ai_addr, sizeof(struct sockaddr_in));
	freeaddrinfo(res);
  // server_addr.sin_addr.s_addr = inet_addr(server_addr_.c_str());
  server_addr.sin_port = htons(std::stoi(server_port_));

  ret = rdma_create_id(cm_event_channel_, &listen_id_, nullptr, RDMA_PS_TCP);
  if (ret != 0) {
    LOG_ERRORF("Failed to create listen id: {}", ret);
    return -1;
  }
  LOG_INFOF("RDMA listen id created at fd: {}", listen_id_->channel->fd);

  ret = rdma_bind_addr(listen_id_, (struct sockaddr*)&server_addr);
  if (ret) {
    LOG_ERRORF("Failed to bind address: {}", ret);
    rdma_destroy_id(listen_id_);
    listen_id_ = nullptr;
    rdma_destroy_event_channel(cm_event_channel_);
    cm_event_channel_ = nullptr;
    rdma_freeaddrinfo(res_);
    res_ = nullptr;
    return -1;
  }
  LOG_INFOF("RDMA address bound at: {} {}", inet_ntoa(server_addr.sin_addr), ntohs(server_addr.sin_port));

  ret = rdma_listen(listen_id_, 8);
  if (ret != 0) {
    LOG_ERRORF("Failed to listen: {}", ret);
    rdma_destroy_id(listen_id_);
    listen_id_ = nullptr;
    rdma_destroy_event_channel(cm_event_channel_);
    cm_event_channel_ = nullptr;
    rdma_freeaddrinfo(res_);
    res_ = nullptr;
    return -1;
  }
  LOG_INFOF("RDMA listen started at {} {}", server_addr_, server_port_);
  return 0;
}

int RemoteServer::SetupClientResources() {
  struct rdma_cm_event* cm_event = nullptr;
  int ret = -1;
  // Since the channel is non-blocking now, we need a loop or poll/select
  while (running_.load(std::memory_order_relaxed)) { // Inner check for shutdown
    ret = rdma_get_cm_event(cm_event_channel_, &cm_event);
    if (ret == 0) {
        break; // Got an event
    } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
        // No event yet, sleep briefly to avoid busy-waiting
        usleep(10000);
        continue;
    } else {
        // Real error
        LOG_ERRORF("Failed to get CM event while waiting for connection: {}", strerror(errno));
        return -1; // Or handle error more gracefully
    }
  }
  if (!running_.load(std::memory_order_relaxed)) {
    LOG_INFO("Returning from setup client resources");
    return -1;
  }

  if (0 != cm_event->status) {
    rdma_ack_cm_event(cm_event);
    LOG_ERRORF("Failed to get CM event: {}", cm_event->status);
    return -1;
  }
  if (cm_event->event != RDMA_CM_EVENT_CONNECT_REQUEST) {
    LOG_ERRORF("unexpected event: {}", rdma_event_str(cm_event->event));
    rdma_ack_cm_event(cm_event);
    return -1;
  }
  LOG_INFO("RDMA connection request received");

  client_id_ = cm_event->id;
  ret = rdma_ack_cm_event(cm_event);
  if (ret) {
    LOG_ERRORF("Failed to ack CM event: {}", ret);
    return -1;
  }

  if (!client_id_) {
    LOG_ERROR("Failed to get client id");
    return -1;
  }

  pd_ = ibv_alloc_pd(client_id_->verbs);
  if (!pd_) {
    // TODO: handle this
    LOG_ERROR("Failed to allocate protection domain");
    return -1;
  }
  LOG_INFO("Protection domain allocated");

  // create completion queue
  cq_ = ibv_create_cq(client_id_->verbs, 1024, nullptr, nullptr, 0);
  if (!cq_) {
    // TODO: handle this
    LOG_ERROR("Failed to create completion queue");
    return -1;
  }
  LOG_INFOF("Completion queue created with {} elements", cq_->cqe);

  ret = ibv_req_notify_cq(cq_, 0);
  if (ret) {
    LOG_ERROR("Failed to request notification for completion queue");
    return -1;
  }
  LOG_INFO("Completion queue notified");

  struct ibv_qp_init_attr qp_init_attr;
  memset(&qp_init_attr, 0, sizeof(qp_init_attr));
  qp_init_attr.send_cq = cq_;
  qp_init_attr.recv_cq = cq_;
  qp_init_attr.qp_type = IBV_QPT_RC;
  qp_init_attr.cap.max_send_wr = max_wr_;
  qp_init_attr.cap.max_recv_wr = max_wr_;
  qp_init_attr.cap.max_send_sge = 1;
  qp_init_attr.cap.max_recv_sge = 1;

  if (rdma_create_qp(client_id_, pd_, &qp_init_attr)) {
    LOG_ERRORF("Failed to create queue pair: {}", ret);
    return -1;
  }
  LOG_INFO("Client queue pair created");

  // we need to post the receive buffers
  for (int i = 0; i < PD3_REMOTE_SERVER_QUEUE_DEPTH; i++) {
    PostRecvBuffer();
  }
  LOG_INFO("Client resources setup complete");
  return 0;
}

void RemoteServer::PostRecvBuffer() {
  auto* request = new offload::RemoteSegmentRequest();
  struct ibv_mr* mr = ibv_reg_mr(pd_, request, sizeof(offload::RemoteSegmentRequest), IBV_ACCESS_LOCAL_WRITE);
  if (!mr) {
    delete request;
    LOG_ERROR("Failed to register memory region");
    return;
  }
  temp_mrs_.insert(mr);

  struct ibv_sge sge = {};
  sge.addr = (uint64_t)request;
  sge.length = sizeof(offload::RemoteSegmentRequest);
  sge.lkey = mr->lkey;

  RequestContext* request_context = new RequestContext{request, mr};

  struct ibv_recv_wr* bad_recv_wr = nullptr;
  struct ibv_recv_wr wr = {};
  wr.wr_id = (uint64_t)request_context;
  wr.sg_list = &sge;
  wr.num_sge = 1;
  wr.next = nullptr;

  if (ibv_post_recv(client_id_->qp, &wr, &bad_recv_wr)) {
    ibv_dereg_mr(mr);
    delete request;
    LOG_ERROR("Failed to post receive");
    return;
  }
}

int RemoteServer::HandleCompletion(struct ibv_wc* wc) {
  LOG_INFOF("Handling completion: {}", uint64_t(wc->opcode));
  if (wc->status != IBV_WC_SUCCESS) {
    LOG_ERRORF("Work completion error: {}", ibv_wc_status_str(wc->status));
    if (wc->status == IBV_WC_WR_FLUSH_ERR) {
      LOG_ERROR("Work completion error: WR_FLUSH_ERR");
      return -1;
    }
    if (wc->opcode == IBV_WC_SEND) {
      LOG_INFO("Handling send completion error");
      ResponseContext* response_context = reinterpret_cast<ResponseContext*>(wc->wr_id);
      temp_mrs_.erase(response_context->mr);
      ibv_dereg_mr(response_context->mr);
      delete response_context->response;
      delete response_context;
      LOG_INFO("Completed handling send completion error");
    }
    if (wc->opcode == IBV_WC_RECV) {
      LOG_INFO("Handling recv completion error");
      RequestContext* request_context = reinterpret_cast<RequestContext*>(wc->wr_id);
      temp_mrs_.erase(request_context->mr);
      ibv_dereg_mr(request_context->mr);
      delete request_context->request;
      delete request_context;
      LOG_INFO("Completed handling recv completion error");
    }
    return -1;
  }

  if (wc->opcode == IBV_WC_RECV) {
    LOG_INFO("Handling recv completion success");
    RequestContext* request_context = reinterpret_cast<RequestContext*>(wc->wr_id);
    HandleRequest(request_context->request);

    temp_mrs_.erase(request_context->mr);
    ibv_dereg_mr(request_context->mr);
    delete request_context->request;
    delete request_context;

    PostRecvBuffer();
    LOG_INFO("Completed handling recv completion success");
    return 0;
  } else if (wc->opcode == IBV_WC_SEND) {
    // we need to handle this correctly so as not to leak memory
    ResponseContext* response_context = reinterpret_cast<ResponseContext*>(wc->wr_id);
    temp_mrs_.erase(response_context->mr);
    ibv_dereg_mr(response_context->mr);
    delete response_context->response;
    delete response_context;
    return 0;
  } else {
    LOG_WARNF("unexpected successful wc opcode: {}", uint32_t(wc->opcode));
    return 0;
  }

  return 0;
}

void RemoteServer::HandleRequest(offload::RemoteSegmentRequest* request) {
  switch (request->op) {
    case offload::OffloadRequestOp::CreateSegment:
      HandleCreateSegment(request);
      break;
    case offload::OffloadRequestOp::RemoveSegment:
      HandleRemoveSegment(request);
      break;
    case offload::OffloadRequestOp::RemoveAll: 
      HandleRemoveAll(request);
      break;
  }
}

void print_response(offload::RemoteSegmentResponse* response) {
  LOG_INFOF("Response: op:{}, status:{}, segment:{}, offset:{}, remote_key:{}, address:{}",
           uint16_t(response->op), uint16_t(response->status), uint64_t(response->segment), uint64_t(response->offset), uint64_t(response->remote_key), uint64_t(response->address));
}

void RemoteServer::HandleCreateSegment(offload::RemoteSegmentRequest* request) {
  ResponseContext* response_context = new ResponseContext;
  if (!response_context) {
    LOG_ERROR("Failed to allocate response context");
    return;
  }
  response_context->response = new offload::RemoteSegmentResponse;
  if (!response_context->response) {
    LOG_ERROR("Failed to allocate response");
    delete response_context;
    return;
  }
  response_context->mr = nullptr;
  response_context->response->op = offload::OffloadRequestOp::CreateSegment;
  response_context->response->status = offload::OffloadRequestStatus::Error;

  // get the memory segment
  size_t details_idx = 0;
  for (auto& details : segment_mr_list_) {
    if (!details.free_segments_.empty()) {
      auto offset = details.free_segments_.front();
      details.free_segments_.pop();
      response_context->response->status = offload::OffloadRequestStatus::Completed;
      response_context->response->offset = offset;
      response_context->response->segment = request->segment;
      response_context->response->remote_key = details.mr->lkey;
      response_context->response->address = (uint64_t)details.mr->addr;
      segment_mr_map_[request->segment] = {details_idx, offset};
      print_response(response_context->response);
      SendResponse(response_context);
      return;
    }
    details_idx++;
  }

  // if we get here, we need to create a new memory segment
  if (response_context->response->status == offload::OffloadRequestStatus::Error) {
    // create a new memory segment
    LOG_INFO("Creating new memory segment");
    auto ptr = (char*)malloc(PD3_REMOTE_SERVER_CHUNK_SIZE);
    if (!ptr) {
      LOG_ERROR("Failed to allocate memory for server");
      delete response_context->response;
      delete response_context;
      return;
    }
    auto mr = ibv_reg_mr(pd_, ptr, PD3_REMOTE_SERVER_CHUNK_SIZE,
     IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
    if (!mr) {
      free(ptr);
      LOG_ERROR("Failed to register memory region");
      delete response_context->response;
      delete response_context;
      return;
    }
    MemoryRegionDetails details;
    details.mr = mr;
    LOG_INFOF("MR key: {}, address: {}", mr->lkey, mr->addr);
    auto num_segments = PD3_REMOTE_SERVER_CHUNK_SIZE / segment_size_;
    size_t offset = 0;
    for (uint64_t i = 0; i < num_segments; i++) {
      details.free_segments_.push(offset);
      offset += segment_size_;
    }
    auto details_idx = segment_mr_list_.size();
    segment_mr_list_.push_back(std::move(details));
    response_context->response->offset = 0;
    response_context->response->segment = request->segment;
    response_context->response->status = offload::OffloadRequestStatus::Completed;
    response_context->response->remote_key = mr->lkey;
    response_context->response->address = (uint64_t)mr->addr;
    segment_mr_list_.back().free_segments_.pop();
    segment_mr_map_[request->segment] = {details_idx, 0};
    print_response(response_context->response);
    SendResponse(response_context);
  }
  return;
}

void RemoteServer::HandleRemoveSegment(offload::RemoteSegmentRequest* request) {
  ResponseContext* response_context = new ResponseContext;
  response_context->response = new offload::RemoteSegmentResponse;
  response_context->mr = nullptr;
  response_context->response->op = offload::OffloadRequestOp::RemoveSegment;
  response_context->response->status = offload::OffloadRequestStatus::Error;

  auto it = segment_mr_map_.find(request->segment);
  if (it == segment_mr_map_.end()) {
    SendResponse(response_context);
    return;
  }

  auto& details = segment_mr_list_[it->second.details_idx];
  details.free_segments_.push(it->second.offset);
  segment_mr_map_.erase(it);
  response_context->response->status = offload::OffloadRequestStatus::Completed;
  SendResponse(response_context);
}

void RemoteServer::HandleRemoveAll(offload::RemoteSegmentRequest* request) {
  ResponseContext* response_context = new ResponseContext;
  response_context->response = new offload::RemoteSegmentResponse;
  response_context->mr = nullptr;
  response_context->response->op = offload::OffloadRequestOp::RemoveAll;
  response_context->response->status = offload::OffloadRequestStatus::Error;

  for (auto& details : segment_mr_list_) {
    while (!details.free_segments_.empty()) {
      details.free_segments_.pop();
    }
  }
  for (auto& element: segment_mr_list_) {
    free(element.mr->addr);
    ibv_dereg_mr(element.mr);
  }
  segment_mr_list_.clear();
  segment_mr_map_.clear();
  response_context->response->status = offload::OffloadRequestStatus::Completed;
  SendResponse(response_context);
}

void RemoteServer::SendResponse(ResponseContext* response_context) {  
  struct ibv_mr* mr = ibv_reg_mr(pd_, response_context->response, sizeof(offload::RemoteSegmentResponse), 
                                IBV_ACCESS_LOCAL_WRITE);
  if (!mr) {
    delete response_context->response;
    delete response_context;
    LOG_ERROR("Failed to register memory region for response");
    return;
  }
  temp_mrs_.insert(mr);
  response_context->mr = mr;

  // Prepare the scatter-gather element
  struct ibv_sge sge = {};
  sge.addr = (uint64_t)response_context->response;
  sge.length = sizeof(offload::RemoteSegmentResponse);
  sge.lkey = mr->lkey;

  // Prepare the send work request
  struct ibv_send_wr wr = {};
  struct ibv_send_wr* bad_wr = nullptr;
  wr.wr_id = (uint64_t)response_context;  // Store pointer for cleanup after completion
  wr.sg_list = &sge;
  wr.num_sge = 1;
  wr.opcode = IBV_WR_SEND;
  wr.send_flags = IBV_SEND_SIGNALED;  // Request completion notification

  // Post the send request
  if (ibv_post_send(client_id_->qp, &wr, &bad_wr)) {
    ibv_dereg_mr(mr);
    temp_mrs_.erase(mr);
    delete response_context->response;
    delete response_context;
    LOG_ERROR("Failed to post send");
    return;
  }
  LOG_INFO("Sent response");
}

void RemoteServer::CleanupClientResources() {
  for (auto& details : segment_mr_list_) {
    free(details.mr->addr);
    ibv_dereg_mr(details.mr);
  }
  for (auto& mr : temp_mrs_) {
    ibv_dereg_mr(mr);
  }
  segment_mr_list_.clear();
  segment_mr_map_.clear();
  temp_mrs_.clear();
  if (cq_) {
    ibv_destroy_cq(cq_);
  }
  if (pd_) {
    ibv_dealloc_pd(pd_);
  }
  if (client_id_) {
    rdma_destroy_id(client_id_);
  }
}
} // namespace dpf