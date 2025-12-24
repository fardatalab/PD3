#include "batched_server.hpp"

#include "types/rdma_protocol.hpp"
#include "PD3/system/logger.hpp"

#include <cstdint>
#include <fcntl.h>
#include <infiniband/verbs.h>
#include <unistd.h>
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <cstring>

namespace dpf {

BatchedMemoryBackend::BatchedMemoryBackend() {
  // TODO: implement this
}

BatchedMemoryBackend::~BatchedMemoryBackend() {
  for (auto session : client_sessions_) {
    for (auto memory_region : session->memory_regions) {
      free(memory_region->base_address);
      ibv_dereg_mr(memory_region->mr);
    }
    for (auto&& buffer : session->recv_buffers) {
      delete buffer;
    }
    for (auto sge : session->sges) {
      delete sge;
    }
    for (auto sge : session->reply_sges) {
      delete sge;
    }
    free(session->mr->addr);
    ibv_dereg_mr(session->mr);

    ibv_destroy_cq(session->cq);
    ibv_destroy_qp(session->conn_id->qp);
    ibv_destroy_comp_channel(session->comp_channel);
    rdma_destroy_id(session->conn_id);
    delete session;
  }
  rdma_destroy_event_channel(cm_event_channel_);
  rdma_destroy_id(listen_id_);
  free(res_);
}

void BatchedMemoryBackend::Configure(const JSON& config) {
  try {
    server_addr_ = config["server_addr"].get<std::string>();
    server_port_ = config["server_port"].get<std::string>();
    server_name_ = config["server_name"].get<std::string>();
  } catch (const std::exception& e) {
    LOG_ERRORF("Failed to configure batched memory backend: {}", e.what());
    throw;
  }
}

void BatchedMemoryBackend::Run() {
  // We can extend this server to span multiple cores if higher throughput is needed

  // 1. setup RDMA context
  // 2. setup listener
  // 3. accept client connection
  // 4. create client session object + network resources
  // 5. send over memory regions (initial # should be 16G), we thus won't need to resize
  // 6. start the worker loop to handle batched requests
  hints_.ai_flags = RAI_PASSIVE;
  hints_.ai_port_space = RDMA_PS_TCP;

  // initialize resources
  auto ret = rdma_getaddrinfo(server_addr_.c_str(), server_port_.c_str(), &hints_, &res_);
  if (ret != 0) {
    LOG_ERRORF("Failed to get address info: {}", ret);
    return;
  }

  // setup the connection channel: this relays connection events from the RDMA layer to the server
  ret = SetupConnectionChannel();
  if (ret != 0) {
    return;
  }

  // setup the RDMA listener
  ret = RdmaListen();
  if (ret != 0) {
    return;
  }

  // start the accept loop
  while (running_.load(std::memory_order_relaxed)) {
    LOG_INFO("Waiting for client connection");
    // listen for a connection event
    struct rdma_cm_id* conn_id = nullptr;
    ret = ListenAndDispatchConnectionEvents(conn_id, RDMA_CM_EVENT_CONNECT_REQUEST);
    if (ret != 0) {
      // error during connection event dispatching
      return;
    }
    // setup client resources, this is needed for a successful connection
    ret = SetupClientResources(conn_id);
    if (ret != 0) {
      return;
    }
    // check if we should stop
    if (!running_) {
      break;
    }

    // setup the connection parameters
    struct rdma_conn_param conn_param = {};
    std::memset(&conn_param, 0, sizeof(conn_param));
    conn_param.initiator_depth = 1;
    conn_param.responder_resources = 1;
    conn_param.rnr_retry_count = 7;

    ret = rdma_accept(client_sessions_.back()->conn_id, &conn_param);
    if (ret != 0) {
      LOG_ERRORF("Failed to accept connection: {}", ret);
      return;
    }
    // check that the correct event was received
    ret = ListenAndDispatchConnectionEvents(conn_id, RDMA_CM_EVENT_ESTABLISHED);
    if (ret != 0) {
      LOG_ERROR("Failed to check connection event");
      return;
    }
    LOG_INFO("RDMA connection established");

    bool client_connected = true;

    auto curr_session = client_sessions_.back();

    // create the per-client memory regions and reply buffers
    size_t total_buffer_size = RDMA_BUFFER_SIZE * 2 * queue_depth_;
    void* buffer_memory = malloc(total_buffer_size);
    if (!buffer_memory) {
      LOG_ERROR("Failed to allocate buffer memory");
      return;
    }
    std::memset(buffer_memory, 0, total_buffer_size);

    // register the memory region
    uint32_t flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ;
    curr_session->mr = ibv_reg_mr(curr_session->pd, buffer_memory, total_buffer_size, flags);
    if (!curr_session->mr) {
      LOG_ERROR("Failed to register memory region");
      return;
    }
    LOG_INFOF("Memory region registered at: {}", curr_session->mr->lkey);

    // create the reply buffers
    auto char_buffer_memory = static_cast<char*>(buffer_memory);
    for (int i = 0; i < queue_depth_; ++i) {
      // buffer setup to receive batched requests from the client
      struct ibv_sge* req_sge = new struct ibv_sge();
      void* recv_buffer = char_buffer_memory + (uint64_t)i * 2 * RDMA_BUFFER_SIZE;
      std::cout << "Recv buffer block address: " << (uint64_t)recv_buffer << std::endl;
      req_sge->addr = (uint64_t)recv_buffer;
      req_sge->length = RDMA_BUFFER_SIZE;
      req_sge->lkey = curr_session->mr->lkey;
      curr_session->sges.push_back(req_sge);
      curr_session->buffers.push_back(recv_buffer);
      ReceiveBuffer* recv_buffer_obj = new ReceiveBuffer();
      recv_buffer_obj->max_size(RDMA_BUFFER_SIZE);
      recv_buffer_obj->buffer(recv_buffer);
      memset(recv_buffer, 0, RDMA_BUFFER_SIZE);
      curr_session->recv_buffers.push_back(recv_buffer_obj);
      // buffer setup to send responses to the client
      struct ibv_sge* reply_sge = new struct ibv_sge();
      void* reply_buffer = char_buffer_memory + ((uint64_t)i * 2 + 1) * RDMA_BUFFER_SIZE;
      reply_sge->addr = (uint64_t)reply_buffer;
      reply_sge->length = RDMA_BUFFER_SIZE;
      reply_sge->lkey = curr_session->mr->lkey;
      curr_session->reply_sges.push_back(reply_sge);
      curr_session->reply_buffers.push_back(reply_buffer);
      memset(reply_buffer, 0, RDMA_BUFFER_SIZE);
    }

    // create the memory regions (shared amongst clients)
    if (memory_regions_.empty()) {
      LOG_INFO("Creating memory regions shared amongst clients");
      for (int i = 0; i < init_regions_; ++i) {
        auto mem_region_descriptor = new ServerMemoryRegionContext();
        auto memory_region = malloc(MEMORY_REGION_SIZE);
        if (!memory_region) {
          LOG_ERRORF("Failed to allocate memory region #{} out of {}", i+1, init_regions_);
          return;
        }
        int flags = IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ;
        mem_region_descriptor->mr = ibv_reg_mr(pd_, memory_region, MEMORY_REGION_SIZE, flags);
        if (!mem_region_descriptor->mr) {
          LOG_ERRORF("Failed to register memory region #{} out of {}", i+1, init_regions_);
          return;
        }
        mem_region_descriptor->base_address = memory_region;
        memory_regions_.push_back(mem_region_descriptor);
        // curr_session->memory_regions.push_back(mem_region_descriptor);
        LOG_INFOF("Memory region #{} registered at: {}", i+1, mem_region_descriptor->mr->lkey);
      }
    }

    // we now exchange the memory regions + reply buffers
    // wait for the client to send the init request, post a receive to get the request
    LOG_INFO("Posting receive for init request");
    PostReceive(curr_session, 0, curr_session->sges[0], 1);
    LOG_INFO("Waiting for completion");
    WaitForCompletion(curr_session);

    LOG_INFO("Received init request");

    auto protocol_header = reinterpret_cast<ProtocolHeader*>(curr_session->buffers[0]);
    if (protocol_header->header != RequestHeader::C2S_INIT) {
      LOG_ERRORF("Received unexpected request, expected INIT, got: {}", request_header_str(protocol_header->header));
      LOG_ERRORF("Int request: {}", uint64_t(protocol_header->header));
      return;
    }

    // construct the response
    auto reply_buf = reinterpret_cast<char*>(curr_session->reply_buffers[0]);
    uint64_t offset = 0;
    auto reply_header = reinterpret_cast<ProtocolHeader*>(reply_buf);
    reply_header->header = RequestHeader::S2C_MEMORY_REGIONS;
    offset += sizeof(RequestHeader);
    auto reply_memory_regions = reinterpret_cast<ReplyMemoryRegions*>(reply_buf + offset);
    reply_memory_regions->num_regions = init_regions_;
    offset += sizeof(ReplyMemoryRegions);

    // add the memory regions
    for (int i = 0; i < init_regions_; ++i) {
      auto region_details = reinterpret_cast<RegionDetails*>(reply_buf + offset);
      region_details->base_address = (uint64_t)memory_regions_[i]->base_address;
      region_details->size = MEMORY_REGION_SIZE;
      region_details->region_rkey = memory_regions_[i]->mr->rkey;
      offset += sizeof(RegionDetails);
    }
    // add the response buffer details
    auto reply_receive_buffers = reinterpret_cast<ReplyReceiveBuffers*>(reply_buf + offset);
    reply_receive_buffers->num_slots = queue_depth_;
    reply_receive_buffers->buffer_rkey = curr_session->mr->rkey;
    offset += sizeof(ReplyReceiveBuffers);
    for (int i = 0; i < queue_depth_; ++i) {
      uint64_t* buff_address = reinterpret_cast<uint64_t*>(reply_buf + offset);
      *buff_address = (uint64_t)curr_session->buffers[i];
      std::cout << "Sending buffer address: " << *buff_address << std::endl;
      offset += sizeof(uint64_t);
    }

    // before sending the response, post a receive to expect the client's receive buffers
    PostReceive(curr_session, 1, curr_session->sges[0], 1);

    // send the response
    LOG_INFOF("Sending response, length: {}, header: {}", offset, uint64_t(reply_header->header));
    curr_session->reply_sges[0]->length = offset;
    PostSend(curr_session, 0, curr_session->reply_sges[0], 1, 0);
    WaitForCompletion(curr_session);
    LOG_INFO("Sent response");

    // wait for client response
    WaitForCompletion(curr_session);

    reply_header = reinterpret_cast<ProtocolHeader*>(curr_session->buffers[0]);
    reply_buf = reinterpret_cast<char*>(curr_session->buffers[0]);
    offset = sizeof(ProtocolHeader);
    if (reply_header->header != RequestHeader::C2S_MEMORY_REGIONS) {
      LOG_ERRORF("Received unexpected request, expected MEMORY_REGIONS, got: {}", request_header_str(reply_header->header));
      return;
    }

    // get the receive buffers
    reply_receive_buffers = reinterpret_cast<ReplyReceiveBuffers*>(reply_buf + offset);
    offset += sizeof(ReplyReceiveBuffers);
    if (reply_receive_buffers->num_slots != queue_depth_) {
      LOG_ERRORF("Received unexpected number of receive buffers, expected {}, got: {}", queue_depth_, reply_receive_buffers->num_slots);
      return;
    }
    auto remote_rkey = reply_receive_buffers->buffer_rkey;
    for (int i = 0; i < reply_receive_buffers->num_slots; ++i) {
      uint64_t* buff_address = reinterpret_cast<uint64_t*>(reply_buf + offset);
      curr_session->recv_buffers[i]->remote_address(*buff_address);
      curr_session->recv_buffers[i]->remote_rkey(remote_rkey);
      offset += sizeof(uint64_t);
    }

    // clear the buffers that are just used
    std::memset(curr_session->buffers[0], 0, RDMA_BUFFER_SIZE);
    std::memset(curr_session->reply_buffers[0], 0, RDMA_BUFFER_SIZE);
    // reset the sge state
    curr_session->sges[0]->length = RDMA_BUFFER_SIZE;
    curr_session->reply_sges[0]->length = RDMA_BUFFER_SIZE;
    // set the send slots
    curr_session->send_slots = queue_depth_;

    // spin up a thread to handle the data plane for this client
    auto session = client_sessions_.back();
    session->client_connected = true;
    data_threads_.push_back(std::thread([this, session]() {
      HandleClientDataPlane(session);
    }));
  }

}

void BatchedMemoryBackend::RunAync() {
  running_ = true;
  worker_thread_ = std::thread([this]() {
    this->Run();
  });
}

void BatchedMemoryBackend::Stop() {
  running_ = false;
  if (worker_thread_.joinable()) {
    worker_thread_.join();
  }
}

void BatchedMemoryBackend::HandleClientDataPlane(RdmaDataSession* session) {
  // this thread will handle the data plane for a single client, 2-sided RPC over 1-sided verbs
  uint64_t batches_processed = 0;

  static uint64_t core_id = 4;

  // TODO: check if we need to add any CPU affinity here
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(core_id, &cpuset);
  core_id++;
  pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
  std::cout << "Data plane thread pinned to core " << core_id << std::endl;

  while (running_.load(std::memory_order_relaxed)) {
    // check if client is still connected
    if (!session->client_connected) {
      LOG_INFO("Client disconnected, exiting data plane thread");
      return;
    }
    // wait for a request
    int slot_id_to_process = -1;
    for (int i = 0; i < queue_depth_; ++i) {
      if (session->recv_buffers[i]->PollMessage()) {
        // we have a request, let's process it
        slot_id_to_process = i;
        break;
      }
    }

    if (slot_id_to_process == -1) {
      std::this_thread::yield();
      continue;
    }

    // process the request
    auto request_buffer = reinterpret_cast<char*>(session->recv_buffers[slot_id_to_process]->buffer());
    uint64_t offset = sizeof(uint32_t); // skip the message size
    ProtocolHeader* header = reinterpret_cast<ProtocolHeader*>(request_buffer + offset);
    offset += sizeof(ProtocolHeader);
    if (header->header == RequestHeader::C2S_BATCH_REQUEST) {
      // prepare the batch request
      auto batch_request = reinterpret_cast<BatchIORequest*>(request_buffer + offset);
      offset += sizeof(BatchIORequest);

      // prepare the reply buffer
      auto reply_buffer = reinterpret_cast<char*>(session->reply_buffers[slot_id_to_process]);
      uint64_t reply_offset = sizeof(uint32_t);

      BatchIOResponse* batch_response = reinterpret_cast<BatchIOResponse*>(reply_buffer + reply_offset);
      reply_offset += sizeof(BatchIOResponse);
      batch_response->num_requests = batch_request->num_requests;
      batch_response->local_request_ptr = batch_request->local_request_ptr;
      
      for (int i = 0; i < batch_request->num_requests; ++i) {
        auto curr_request = reinterpret_cast<SingleIORequest*>(request_buffer + offset);
        offset += sizeof(SingleIORequest);
        if (curr_request->is_read) {
          // read from local into the reply buffer
          std::memcpy(reply_buffer + reply_offset, (const void*)curr_request->address, curr_request->bytes);
          reply_offset += curr_request->bytes;
        } else {
          // write to local from the request buffer
          std::memcpy((void*)curr_request->address, request_buffer + offset, curr_request->bytes);
          offset += curr_request->bytes;
        }
      }

      // send the reply
      auto total_bytes_to_send = reply_offset - sizeof(uint32_t);

      // remove the request message
      session->recv_buffers[slot_id_to_process]->RemoveMessage();

      // set the reply message size
      *(reinterpret_cast<uint32_t*>(reply_buffer)) = total_bytes_to_send;
      // set a 1 to mark the completion of the message
      *(reinterpret_cast<uint32_t*>(reply_buffer + reply_offset)) = 1;
      reply_offset += sizeof(uint32_t);

      while (session->send_slots == 0) {
        struct ibv_wc wc;
        int ret = ibv_poll_cq(session->cq, 1, &wc);
        if (ret < 0) {
          LOG_ERRORF("Failed to poll completion queue: {}", ret);
          return;
        }
        if (wc.status != IBV_WC_SUCCESS) {
          LOG_ERRORF("Completion queue entry status: {}", ibv_wc_status_str(wc.status));
          return;
        }
        if (wc.opcode == IBV_WC_RDMA_WRITE) {
          session->send_slots++;
        }
      }

      session->reply_sges[slot_id_to_process]->length = reply_offset;
      // send using the write verb
      int flags = IBV_SEND_SIGNALED;
      ReceiveBuffer* recv_buffer = session->recv_buffers[slot_id_to_process];
      PostWrite(session,
                slot_id_to_process,
                session->reply_sges[slot_id_to_process],
                1,
                recv_buffer->remote_address(),
                recv_buffer->remote_rkey(),
                flags);

      // these are updated lazily
      session->send_slots--;
    } else {
      LOG_ERRORF("Received unexpected request, expected BATCH_REQUEST, got: {}", request_header_str(header->header));
      return;
    }
  }
  LOG_INFO("Client data plane thread exiting");
}

int BatchedMemoryBackend::SetupConnectionChannel() {
  cm_event_channel_ = rdma_create_event_channel();
  if (!cm_event_channel_) {
    LOG_ERROR("Failed to create event channel");
    return -1;
  }

  int flags = fcntl(cm_event_channel_->fd, F_GETFL);
  if (flags == -1) {
      LOG_ERRORF("Failed to get RDMA event channel flags: {}", strerror(errno));
      return -1;
  }
  if (fcntl(cm_event_channel_->fd, F_SETFL, flags | O_NONBLOCK) == -1) {
      LOG_ERRORF("Failed to set RDMA event channel to non-blocking: {}", strerror(errno));
      return -1;
  }
  LOG_INFO("RDMA event channel created and set to non-blocking.");
  return 0;
}

int BatchedMemoryBackend::RdmaListen() {
  // create the RDMA listen id
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

  // bind the listen id to the server address+port
  ret = rdma_bind_addr(listen_id_, (struct sockaddr*)&server_addr);
  if (ret) {
    LOG_ERRORF("Failed to bind address: {}", ret);
    return -1;
  }
  LOG_INFOF("RDMA address bound at: {} {}", inet_ntoa(server_addr.sin_addr), ntohs(server_addr.sin_port));

  ret = rdma_listen(listen_id_, 8);
  if (ret != 0) {
    LOG_ERRORF("Failed to listen: {}", ret);
    return -1;
  }
  LOG_INFOF("RDMA listen started at {} {}", server_addr_, server_port_);
  return 0;
}

int BatchedMemoryBackend::SetupClientResources(struct rdma_cm_id* id) {
  RdmaDataSession* session = new RdmaDataSession();
  int ret = -1;
  // Since the channel is non-blocking now, we need a loop or poll/select
  // ret = CheckConnectionEvent(RDMA_CM_EVENT_CONNECT_REQUEST);
  // if (ret != 0) {
  //   LOG_ERROR("Failed to check connection event");
  //   return -1;
  // }
  // LOG_INFO("RDMA connection request received");
  session->conn_id = id;

  if (!session->conn_id) {
    LOG_ERROR("Failed to get client id");
    return -1;
  }

  if (!pd_) {
    pd_ = ibv_alloc_pd(session->conn_id->verbs);
    if (!pd_) {
      LOG_ERROR("Failed to allocate protection domain");
      return -1;
    }
    LOG_INFO("Protection domain allocated for server");
  }

  session->pd = pd_;
  LOG_INFO("Protection domain allocated");

  // create completion queue
  session->cq = ibv_create_cq(session->conn_id->verbs, 1024, nullptr, nullptr, 0);
  if (!session->cq) {
    LOG_ERROR("Failed to create completion queue");
    return -1;
  }
  LOG_INFOF("Completion queue created with {} elements", session->cq->cqe);

  ret = ibv_req_notify_cq(session->cq, 0);
  if (ret) {
    LOG_ERROR("Failed to request notification for completion queue");
    return -1;
  }
  LOG_INFO("Completion queue notified");

  struct ibv_qp_init_attr qp_init_attr;
  memset(&qp_init_attr, 0, sizeof(qp_init_attr));
  qp_init_attr.send_cq = session->cq;
  qp_init_attr.recv_cq = session->cq;
  qp_init_attr.qp_type = IBV_QPT_RC;
  qp_init_attr.cap.max_send_wr = max_wr_;
  qp_init_attr.cap.max_recv_wr = max_wr_;
  qp_init_attr.cap.max_send_sge = 1;
  qp_init_attr.cap.max_recv_sge = 1;

  if (rdma_create_qp(session->conn_id, session->pd, &qp_init_attr)) {
    LOG_ERRORF("Failed to create queue pair: {}", ret);
    return -1;
  }
  LOG_INFO("Client queue pair created");
  client_sessions_.push_back(session);

  LOG_INFO("Client resources setup complete");
  return 0;
}

int BatchedMemoryBackend::CheckConnectionEvent(rdma_cm_event_type expected_event) {
  // TODO: check here if we get a disconnect event, etc. so we can set the relevant flags
  struct rdma_cm_event* cm_event = nullptr;
  int ret = -1;
  while (running_.load(std::memory_order_relaxed)) {
    ret = rdma_get_cm_event(cm_event_channel_, &cm_event);
    if (ret == 0) {
      break;
    } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
      usleep(10000);
      continue;
    } else {
      LOG_ERRORF("Failed to get CM event while waiting for connection: {}", strerror(errno));
      return -1;
    }
  }
  if (!running_) {
    return -1;
  }
  if (0 != cm_event->status) {
    rdma_ack_cm_event(cm_event);
    LOG_ERRORF("Failed to get CM event, status was non-zero: {}", cm_event->status);
    return -1;
  }
  if (cm_event->event != expected_event) {
    LOG_ERRORF("unexpected event, expected {}, got: {}", rdma_event_str(expected_event), rdma_event_str(cm_event->event));
    rdma_ack_cm_event(cm_event);
    return -1;
  }
  rdma_ack_cm_event(cm_event);
  return 0;
}

int BatchedMemoryBackend::ListenAndDispatchConnectionEvents(struct rdma_cm_id*& id, rdma_cm_event_type expected_event)
{
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
          return 0;
        }
        break;
      case RDMA_CM_EVENT_ESTABLISHED:
        LOG_INFO("RDMA connection established");
        if (expected_event == RDMA_CM_EVENT_ESTABLISHED) {
          return 0;
        }
        break;
      case RDMA_CM_EVENT_DISCONNECTED:
        LOG_INFO("RDMA connection disconnected");
        // dispatch this event to the client session
        if (expected_event == RDMA_CM_EVENT_DISCONNECTED) {
          return 0;
        }
        for (auto session : client_sessions_) {
          if (session->conn_id == id) {
            LOG_INFO("Client disconnected, setting client connected to false");
            session->client_connected = false;
          }
        }
        break;
      default:
        LOG_ERRORF("Unexpected event: {}", rdma_event_str(cm_event->event));
        break;
    }
  }
  return 0;
}

bool BatchedMemoryBackend::PostReceive(RdmaDataSession* session,
                                        uint64_t request_id,
                                        struct ibv_sge* sg_list,
                                        size_t num_sge)
{
  int ret = rdma_post_recv(session->conn_id,
    NULL,
    (void*)sg_list->addr,
    sg_list->length,
    session->mr);
  if (ret != 0) {
    LOG_ERRORF("Failed to post recv: {}", ret);
    return false;
  }
  return true;
}

bool BatchedMemoryBackend::PostSend(RdmaDataSession* session,
                                    uint64_t request_id,
                                    struct ibv_sge* sg_list,
                                    size_t num_sge,
                                    int flags)
{
  int ret = rdma_post_send(session->conn_id,
   NULL,
   (void*)sg_list->addr,
   sg_list->length,
   session->mr,
   IBV_SEND_SIGNALED);
  if (ret != 0) {
    LOG_ERRORF("Failed to post send: {}", ret);
    return false;
  }
  return true;
}

bool BatchedMemoryBackend::PostWrite(RdmaDataSession* session,
                                    uint64_t request_id,
                                    struct ibv_sge* sg_list,
                                    size_t num_sge,
                                    uint64_t remote_addr,
                                    uint32_t remote_rkey,
                                    int flags)
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
  if (ibv_post_send(session->conn_id->qp, &wr, &bad_send_wr)) {
    LOG_ERROR("failed to post write");
    return false;
  }
  return true;
}


bool BatchedMemoryBackend::WaitForCompletion(RdmaDataSession* session) {
  struct ibv_wc wc;
  while (true) {
    int ret = ibv_poll_cq(session->cq, 1, &wc);
    if (ret < 0) {
      LOG_ERRORF("Failed to poll completion queue: {}", ret);
      return false;
    }
    if (ret == 0) {
      std::this_thread::yield();
      continue;
    }
    if (wc.status != IBV_WC_SUCCESS) {
      LOG_ERRORF("Completion queue entry status: {} {}", ibv_wc_status_str(wc.status), uint64_t(wc.status));
      return false;
    } else {
      break;
    }
  }
  return true;
}

} // namespace dpf
