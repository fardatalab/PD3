#include "mem_server.hpp"

#include "PD3/transfer_engine/types.hpp"
#include "PD3/system/logger.hpp"

#include <cstring>
#include <infiniband/verbs.h>

#include <fcntl.h>
#include <rdma/rdma_cma.h>
#include <unistd.h>
#include <string.h>
#include <netdb.h>
#include <netinet/in.h>	
#include <arpa/inet.h>
#include <sys/socket.h>

namespace dpf {

int MemoryPreallocatedServer::RemoteBuffer::Init(struct ibv_pd* pd, size_t size) {
  ptr = (char*)malloc(size);
  if (ptr == nullptr) {
    LOG_ERROR("Failed to allocate remote buffer");
    return -1;
  }
  mr = ibv_reg_mr(pd, ptr, size, IBV_ACCESS_LOCAL_WRITE);
  if (mr == nullptr) {
    LOG_ERROR("Failed to register remote buffer");
    free(ptr);
    ptr = nullptr;
    return -1;
  }
  return 0;
}

MemoryPreallocatedServer::RemoteBuffer::~RemoteBuffer() {
  if (mr) {
    ibv_dereg_mr(mr);
  }
  if (ptr) {
    free(ptr);
    ptr = nullptr;
  }
} 

MemoryPreallocatedServer::MemoryPreallocatedServer() {
  std::memset(&hints_, 0, sizeof(hints_));
}

MemoryPreallocatedServer::~MemoryPreallocatedServer() {
  // Stop();
}

void MemoryPreallocatedServer::Configure(const std::string& server_addr,
                                         const std::string& server_port,
                                         const std::string& server_name,
                                         uint32_t max_wr,
                                         size_t segment_size,
                                         size_t num_segments) 
{
  server_addr_ = server_addr;
  server_port_ = server_port;
  server_name_ = server_name;
  max_wr_ = max_wr;
  segment_size_ = segment_size;
  num_segments_ = num_segments;
  LOG_INFOF("Configured server with addr: {} port: {} name: {} max_wr: {} segment_size: {} num_segments: {}", 
            server_addr_, server_port_, server_name_, max_wr_, segment_size_, num_segments_);
}

void MemoryPreallocatedServer::Run() {
  running_ = true;
  std::thread t([this]() {
    this->ServerThread();
  });
  server_thread_ = std::move(t);
}

void MemoryPreallocatedServer::Stop() {
  running_ = false;
  if (server_thread_.joinable()) {
    server_thread_.join();
  }
  for (auto& workers: worker_threads_) {
    if (workers.joinable()) {
      workers.join();
    }
  }
  worker_threads_.clear();
  CleanupGeneralResources();
}

void MemoryPreallocatedServer::ServerThread() {
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
    struct rdma_cm_id* connecting_id = nullptr;
    ret = ListenAndDispatchConnectionEvents(connecting_id, RDMA_CM_EVENT_CONNECT_REQUEST);
    if (!running_) break; // check if we exited out of listen and dispatch due to stop
    if (ret != 0) {
      LOG_ERRORF("Failed to listen and dispatch connection events: {}", ret);
      break;
    }
    if (connecting_id == nullptr) {
      LOG_ERROR("no connection id received");
      break;
    }

    ret = SetupClientResources(connecting_id);
    if (ret != 0) {
      LOG_ERRORF("Failed to setup client resources: {}", ret);
      continue;
    }

    if (!running_) break;

    struct rdma_conn_param conn_param = {};
    std::memset(&conn_param, 0, sizeof(conn_param));
    conn_param.initiator_depth = 1;
    conn_param.responder_resources = 1;
    conn_param.rnr_retry_count = 7;

    ret = rdma_accept(connecting_id, &conn_param);
    if (ret != 0) {
      LOG_ERRORF("Failed to accept connection: {}", ret);
      break;
    }

    ret = ListenAndDispatchConnectionEvents(connecting_id, RDMA_CM_EVENT_ESTABLISHED);
    if (ret != 0) {
      LOG_ERRORF("Error while listening for connection event {}", ret);
    }
    LOG_INFO("RDMA connection established");

    // spin up the client handling thread
    auto client_state = connected_clients_.back();
    std::thread t([this, client_state]() {
      this->ClientHandlerThread(client_state);
    });
    worker_threads_.push_back(std::move(t));
  }
}

void MemoryPreallocatedServer::ClientHandlerThread(ClientState* client_state) {
  while (client_state->running.load(std::memory_order_relaxed)) {
    struct ibv_wc wc;
    int ret = ibv_poll_cq(client_state->cq, 1, &wc);
    if (ret < 0) {
      LOG_ERRORF("Failed to poll completion queue: {}", ret);
      break;
    }
    if (ret == 0) {
      // no events, sleep for a bit
      usleep(10000);
      continue;
    }
    if (wc.status != IBV_WC_SUCCESS) {
      LOG_ERRORF("Completion queue event had non-success status: {}", ibv_wc_status_str(wc.status));
      break;
    }
    if (HandleCompletion(&wc, client_state)) {
      break;
    }
  }
  // LOG_INFO("Client handler thread exiting");
  // CleanupClientResources(client_state);
  // LOG_INFO("Client handler thread exiting");
}

int MemoryPreallocatedServer::SetupConnectionChannel() {
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

int MemoryPreallocatedServer::RdmaListen() {
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

int MemoryPreallocatedServer::SetupClientResources(struct rdma_cm_id* client_id) {
  LOG_INFO("Setting up client resources for new client");
  ClientState* client_state = new ClientState();
  connected_clients_.push_back(client_state);
  client_state->client_id = client_id;
  client_state->running = true;
  
  using namespace remote_memory_details;
  size_t remote_buffer_size = sizeof(RemoteMemoryDetailsRequest) + sizeof(RemoteMemoryDetails);

  if (!pd_) {
    pd_ = ibv_alloc_pd(client_id->verbs);
    if (!pd_) {
      // TODO: handle this
      LOG_ERROR("Failed to allocate protection domain");
      return -1;
    }
    LOG_INFO("Protection domain allocated");
    InitializeMemoryRegions(); // create the memory regions on the first client connect
  }

  if (client_state->remote_buffer.Init(pd_, remote_buffer_size) != 0) {
    LOG_ERROR("Failed to initialize remote buffer");
    return -1;
  }
  using namespace remote_memory_details;
  client_state->remote_memory_details = 
    reinterpret_cast<RemoteMemoryDetails*>(client_state->remote_buffer.ptr + sizeof(RemoteMemoryDetailsRequest));
  client_state->remote_memory_details->num_entries = num_segments_;
  for (size_t i = 0; i < num_segments_; i++) {
    client_state->remote_memory_details->regions[i].address = (uint64_t)segment_mr_list_[i].ptr;
    client_state->remote_memory_details->regions[i].rkey = segment_mr_list_[i].mr->rkey;
  }

  // create completion queue
  auto cq = ibv_create_cq(client_id->verbs, 1024, nullptr, nullptr, 0);
  if (!cq) {
    // TODO: handle this
    LOG_ERROR("Failed to create completion queue");
    return -1;
  }
  LOG_INFOF("Completion queue created with {} elements", cq->cqe);

  int ret = ibv_req_notify_cq(cq, 0);
  if (ret) {
    LOG_ERROR("Failed to request notification for completion queue");
    return -1;
  }
  LOG_INFO("Completion queue notified");

  struct ibv_qp_init_attr qp_init_attr;
  memset(&qp_init_attr, 0, sizeof(qp_init_attr));
  qp_init_attr.send_cq = cq;
  qp_init_attr.recv_cq = cq;
  qp_init_attr.qp_type = IBV_QPT_RC;
  qp_init_attr.cap.max_send_wr = max_wr_;
  qp_init_attr.cap.max_recv_wr = max_wr_;
  qp_init_attr.cap.max_send_sge = 1;
  qp_init_attr.cap.max_recv_sge = 1;

  if (rdma_create_qp(client_state->client_id, pd_, &qp_init_attr)) {
    LOG_ERRORF("Failed to create queue pair: {}", ret);
    return -1;
  }
  LOG_INFO("Client queue pair created");
  client_state->cq = cq;

  // possibly post some receive buffers
  ret = PostRecv(client_state);
  if (ret != 0) {
    LOG_ERRORF("Failed to post receive: {}", ret);
    return -1;
  }
  LOG_INFO("Receive buffer posted");

  LOG_INFO("Client resources setup complete");
  return 0;
}

int MemoryPreallocatedServer::ListenAndDispatchConnectionEvents(struct rdma_cm_id*& id, rdma_cm_event_type expected_event) {
  struct rdma_cm_event* cm_event = nullptr;
  int ret = -1;
  while (running_.load(std::memory_order_relaxed)) {
    ret = rdma_get_cm_event(cm_event_channel_, &cm_event);
    if (ret != 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        usleep(10000);
        continue;
      } else {
        LOG_ERRORF("Failed to get CM event while waiting for connection: {}", strerror(errno));
        return -1;
      }
    }
    if (0 != cm_event->status) {
      rdma_ack_cm_event(cm_event);
      LOG_ERRORF("Failed to get CM event, status was non-zero: {}", cm_event->status);
      return -1;
    }
    id = cm_event->id;
    switch (cm_event->event) {
      case RDMA_CM_EVENT_CONNECT_REQUEST:
        LOG_INFO("RDMA connection request received");
        if (expected_event == RDMA_CM_EVENT_CONNECT_REQUEST) {
          rdma_ack_cm_event(cm_event);
          return 0;
        }
        break;
      case RDMA_CM_EVENT_ESTABLISHED:
        LOG_INFO("RDMA connection established");
        if (expected_event == RDMA_CM_EVENT_ESTABLISHED) {
          rdma_ack_cm_event(cm_event);
          return 0;
        }
        break;
      case RDMA_CM_EVENT_DISCONNECTED:
        LOG_INFO("RDMA connection disconnected");
        // dispatch this event to the client session
        if (expected_event == RDMA_CM_EVENT_DISCONNECTED) {
          rdma_ack_cm_event(cm_event);
          return 0;
        }
        for (auto&& client_state : connected_clients_) {
          if (client_state->client_id == id) {
            LOG_INFO("Client disconnected, setting client connected to false");
            client_state->running = false;
          }
        }
        break;
      default:
        LOG_ERRORF("Unexpected event: {}", rdma_event_str(cm_event->event));
        break;
    }
    rdma_ack_cm_event(cm_event);
  }
  return 0;
}

int MemoryPreallocatedServer::InitializeMemoryRegions() {
  // create initial resources
  LOG_INFOF("Creating {} memory segments of size {}", num_segments_, segment_size_);

  for (size_t i = 0; i < num_segments_; i++) {
    void* ptr = malloc(segment_size_);
    if (ptr == nullptr) {
      LOG_ERRORF("Failed to allocate memory for segment {}", i);
      return -1;
    }
    struct ibv_mr* mr = ibv_reg_mr(pd_, ptr, segment_size_, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
    if (mr == nullptr) {
      LOG_ERRORF("Failed to register memory region for segment {}", i);
      return -1;
    }
    segment_mr_list_.push_back({mr, ptr});
  }

  return 0;
}

int MemoryPreallocatedServer::HandleCompletion(struct ibv_wc* wc, ClientState* client_state)
{
  // TODO: fill in
  switch (wc->opcode) {
  case IBV_WC_SEND:
    // no-op, we don't care about send completions
    return 0;
  case IBV_WC_RECV: {
    // send over the remote memory regions
    int ret = SendMemoryRegions(client_state);
    if (ret != 0) {
      LOG_ERRORF("Failed to send memory regions: {}", ret);
      return -1;
    }
    return 0;
  }
  default:
    LOG_INFOF("Default case for completion: {}", uint64_t(wc->opcode));
    return -1;
  }
  return 0;
}

// client communication
int MemoryPreallocatedServer::PostRecv(ClientState* client_state) {
  using namespace remote_memory_details;
  struct ibv_sge sge = {};
  sge.addr = (uint64_t)client_state->remote_buffer.ptr;
  sge.length = sizeof(RemoteMemoryDetailsRequest);
  sge.lkey = client_state->remote_buffer.mr->lkey;

  struct ibv_recv_wr* bad_recv_wr = nullptr;
  struct ibv_recv_wr wr = {};
  wr.wr_id = 0;
  wr.sg_list = &sge;
  wr.num_sge = 1;
  wr.next = nullptr;

  if (ibv_post_recv(client_state->client_id->qp, &wr, &bad_recv_wr)) {
    LOG_ERROR("Failed to post receive");
    return -1;
  }
  LOG_INFO("Receive buffer posted");
  return 0;
}

int MemoryPreallocatedServer::SendMemoryRegions(ClientState* client_state) 
{
  using namespace remote_memory_details;
  struct ibv_sge sge = {};
  sge.addr = (uint64_t)client_state->remote_memory_details;
  sge.length = sizeof(RemoteMemoryDetails);
  sge.lkey = client_state->remote_buffer.mr->lkey;

  struct ibv_send_wr* bad_wr = nullptr;
  struct ibv_send_wr wr = {};
  wr.wr_id = 0;
  wr.sg_list = &sge;
  wr.num_sge = 1;
  wr.next = nullptr;
  wr.opcode = IBV_WR_SEND;
  wr.send_flags = IBV_SEND_SIGNALED;

  if (ibv_post_send(client_state->client_id->qp, &wr, &bad_wr)) {
    LOG_ERROR("Failed to post send");
    return -1;
  }
  return 0;
}

void MemoryPreallocatedServer::CleanupGeneralResources() 
{
  for (auto client : connected_clients_) {
    if (client->client_id != nullptr) {
      CleanupClientResources(client);
    }
  }
  for (int i =  0; i < connected_clients_.size(); i++) {
    delete connected_clients_[i];
  }
  connected_clients_.clear();
  for (auto mr : segment_mr_list_) {
    ibv_dereg_mr(mr.mr);
    free(mr.ptr);
  }
  if (pd_) {
    ibv_dealloc_pd(pd_);
    pd_ = nullptr;
  }
  if (cm_event_channel_) {
    rdma_destroy_event_channel(cm_event_channel_);
    cm_event_channel_ = nullptr;
  }
  if (res_) {
    rdma_freeaddrinfo(res_);
    res_ = nullptr;
  }
  if (listen_id_) {
    rdma_destroy_id(listen_id_);
    listen_id_ = nullptr;
  }
}

void MemoryPreallocatedServer::CleanupClientResources(ClientState* state) {
  rdma_disconnect(state->client_id);
  ibv_destroy_qp(state->client_id->qp);
  state->client_id->qp = nullptr;
  state->remote_memory_details = nullptr;
  ibv_destroy_cq(state->cq);
  rdma_destroy_id(state->client_id);
  state->client_id = nullptr;
}

}