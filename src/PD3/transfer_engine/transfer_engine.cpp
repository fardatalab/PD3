#include "transfer_engine.hpp"

#include "config.h"
#include "PD3/system/logger.hpp"
#include "PD3/buffer/request_buffer.hpp"
#include "PD3/buffer/agent_buffer.hpp"
#include "PD3/buffer/response_buffer.hpp"
#include "types/rdma_protocol.hpp"
#include "user_defined/garnet.hpp"

#include <infiniband/verbs.h>
#include <iostream>
#include <unistd.h>

#include <doca_buf.h>
#include <doca_buf_inventory.h>
#include <doca_ctx.h>
#include <doca_dma.h>
#include <doca_error.h>
#include <doca_log.h>
#include <doca_mmap.h>
#include <doca_pe.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <rdma/rdma_verbs.h>

DOCA_LOG_REGISTER(TRANSFER_ENGINE);

namespace dpf {
namespace offload {

[[nodiscard]] constexpr std::size_t round_up_64(std::size_t n) noexcept
{
    return (n + 63u) & ~std::size_t{63u};   // add 63, then clear lower 6 bits
}

[[nodiscard]] constexpr uint32_t align_to_eight_bytes(uint32_t pos) noexcept {
  return (pos + 7) & ~7;
}

static inline void handle_error(doca_error_t result, 
                                const char* expr, 
                                const char* file, 
                                int line)
{
  LOG_INFOF("call {} failed with error: {} on file: {}, line: {}", expr, doca_error_get_name(result), file, line);
  return;
}

#define CHECK_DOCA(call_)         \ 
  do {                       \  
    doca_error_t _rc = (call_);       \ 
    if (_rc != DOCA_SUCCESS) { \
      handle_error(_rc, #call_, __FILE__, __LINE__); \
      return false; \
    } \
  } while (0)

static bool FileExists(const std::string& path) {
  return access(path.c_str(), F_OK) == 0;
}

/*
 * DMA Memcpy task completed callback
 *
 * @dma_task [in]: Completed task
 * @task_user_data [in]: doca_data from the task
 * @ctx_user_data [in]: doca_data from the context
 */
static void dma_memcpy_completed_callback(
	doca_dma_task_memcpy* dma_task, doca_data task_user_data, doca_data ctx_user_data
) {
	(void)ctx_user_data;

	auto* dma_job = static_cast<TransferEngine::DmaJob*>(task_user_data.ptr);
  auto* transfer_engine = static_cast<TransferEngine*>(ctx_user_data.ptr);

  transfer_engine->HandleDmaCompletion(dma_job);

  // free resources and place the job back
	doca_task_free(doca_dma_task_memcpy_as_task(dma_task));
  if (doca_buf_dec_refcount(dma_job->src_buf, nullptr) != DOCA_SUCCESS) {
    std:: cerr << "failed to remove DOCA src buffer reference count\n";
  }
  if (doca_buf_dec_refcount(dma_job->dest_buf, nullptr) != DOCA_SUCCESS) {
    std::cerr << "failed to remove DOCA dest buffer reference count\n";
  }
  transfer_engine->EnqueueDmaJob(dma_job);
}

/*
 * DMA Memcpy task error callback
 *
 * @dma_task [in]: failed task
 * @task_user_data [in]: doca_data from the task
 * @ctx_user_data [in]: doca_data from the context
 */
static void dma_memcpy_error_callback(
	doca_dma_task_memcpy* dma_task, doca_data task_user_data, doca_data ctx_user_data
) {
	(void)ctx_user_data;

  auto* dma_job = static_cast<TransferEngine::DmaJob*>(task_user_data.ptr);
  auto* transfer_engine = static_cast<TransferEngine*>(ctx_user_data.ptr);

	/* Get the result of the task */
	doca_task* task = doca_dma_task_memcpy_as_task(dma_task);
	const doca_error_t result = doca_task_get_status(task);
	DOCA_LOG_ERR("DMA task failed: %s", doca_error_get_descr(result));

  // free resources and place the job back
  doca_task_free(doca_dma_task_memcpy_as_task(dma_task));
  if (doca_buf_dec_refcount(dma_job->src_buf, nullptr) != DOCA_SUCCESS) {
    std:: cerr << "failed to remove DOCA src buffer reference count\n";
  }
  if (doca_buf_dec_refcount(dma_job->dest_buf, nullptr) != DOCA_SUCCESS) {
    std::cerr << "failed to remove DOCA dest buffer reference count\n";
  }
  transfer_engine->EnqueueDmaJob(dma_job);
}

/**
 * Callback triggered whenever DMA context state changes
 *
 * @user_data [in]: User data associated with the DMA context which holds `dma_resources*`
 * @ctx [in]: The DMA context that had a state change
 * @prev_state [in]: Previous context state
 * @next_state [in]: Next context state (context is already in this state when the callback is called)
 */
static void dma_state_changed_callback(
	const doca_data user_data, doca_ctx* ctx, doca_ctx_states prev_state, doca_ctx_states next_state
) {
  (void)user_data;
	(void)ctx;
	(void)prev_state;

	switch (next_state) {
	case DOCA_CTX_STATE_IDLE:
		DOCA_LOG_INFO("DMA context has been stopped");
		break;
	case DOCA_CTX_STATE_STARTING:
		/* The context is in starting state, this is unexpected for DMA */
		DOCA_LOG_ERR("DMA context entered into starting state. Unexpected transition");
		break;
	case DOCA_CTX_STATE_RUNNING:
		DOCA_LOG_INFO("DMA context is running");
		break;
	case DOCA_CTX_STATE_STOPPING:
		/**
		 * doca_ctx_stop() has been called.
		 * This happens either due to a failure encountered, in which case doca_pe_progress() will cause any inflight
		 * task to be flushed, or due to the successful compilation of the flow.
		 * In both cases, doca_pe_progress() will eventually transition the context to idle state.
		 */
		DOCA_LOG_INFO("DMA context entered into stopping state. Any inflight tasks will be flushed");
		break;
	default:
		break;
	}
}

TransferEngine::~TransferEngine()
{
  for (auto job : all_jobs_) {
    if (job->dest_buf != nullptr) {
      // TODO: decrement the ref count
    }
    if (job->src_buf != nullptr) {
      // TODO: decrement the ref count
    }
    delete job;
  }
  all_jobs_.clear();
  // TODO: destroy RDMA resources
  // TODO: destroy DMA resources
  // TODO: clean up memory
}

bool TransferEngine::Initialize(const TransferEngineConfig& config)
{
  config_ = config;
  std::chrono::steady_clock::time_point start_time = std::chrono::steady_clock::now();
  bool all_files_exist = true;
  do {
    if (config.enable_transfer && !FileExists(config.export_desc_client_file_path)) {
      all_files_exist = false;
    }
    if (config.enable_agent && !FileExists(config.export_desc_agent_file_path)) {
      all_files_exist = false;
    }
    if (config.enable_transfer && !FileExists(config.buf_client_file_path)) {
      all_files_exist = false;
    }
    if (config.enable_agent && !FileExists(config.buf_agent_file_path)) {
      all_files_exist = false;
    }
    if (std::chrono::steady_clock::now() - start_time > std::chrono::seconds(60)) {
      throw std::runtime_error("Failed to receive files for one minute (60 seconds)");
    }
    sleep(1);
  } while (!all_files_exist);

  if (!InitializeDma()) {
    return false;
  }

  if (config.use_rdma && !InitializeRdma()) {
    return false;
  }

  send_slots_ = config_.num_prefetch_slots + config_.num_slots;

  LOG_INFO("All initialization done");
  return true;
}

bool TransferEngine::InitializeNoDma(const TransferEngineConfig& config)
{
  config_ = config;
  // create the slots
  if (!InitializeSlots()) {
    return false;
  }

  if (config.use_rdma && !InitializeRdma()) {
    return false;
  }
  // prepare the prefetch deque
  prefetch_deque_.length = config_.num_prefetch_slots;
  prefetch_deque_.num_slots = config_.num_prefetch_slots;
  prefetch_deque_.base_slot_idx = 0;

  // prepare the send slots
  send_slots_ = config_.num_prefetch_slots + config_.num_slots;

  return true;
}

void TransferEngine::AddPrefetchRequestQueue(PrefetchRequestBatchQueue* prefetch_req_batch_q)
{
  prefetch_req_batch_qs_.push_back(prefetch_req_batch_q);
}

void TransferEngine::Run()
{
  running_.store(true);
  std::thread t([this]() {
    this->WorkerThread();
  });
  worker_ = std::move(t);
}

void TransferEngine::RunNoDma()
{
  running_.store(true);
  std::thread t([this]() {
    this->WorkerThreadNoDma();
  });
  worker_ = std::move(t);
}

void TransferEngine::Stop()
{
  running_.store(false);
  if (worker_.joinable()) {
    worker_.join();
  }
}

// handle the prefetch batch
bool TransferEngine::HandlePrefetchBatch(char* batch_ptr, size_t batch_size)
{
  if (prefetch_deque_.full()) {
    LOG_ERRORF("Prefetch deque is full, cannot handle batch. THIS SHOULD NOT HAPPEN");
    return false;
  }
  // get the slot
  auto slot = prefetch_deque_.tail();
  prefetch_deque_.advance_tail();
  auto& prefetch_slot = prefetch_slots_[slot];
  // post the write to the PD3 server
  auto data_buffer = reinterpret_cast<char*>(prefetch_slot.req);
  uint64_t offset = sizeof(uint32_t);
  auto request_header = (ProtocolHeader*)(data_buffer + offset);
  request_header->header = RequestHeader::C2S_PREFETCH_REQUEST;
  offset += sizeof(ProtocolHeader);

  // copy the batch
  std::memcpy(data_buffer + offset, batch_ptr, batch_size);
  offset += batch_size;
  // set the message size
  *reinterpret_cast<uint32_t*>(data_buffer) = offset - sizeof(uint32_t);
  // set the 1
  *reinterpret_cast<uint32_t*>(data_buffer + offset) = 1;


  while (send_slots_ == 0) { // maybe separate this between prefetch and transfer
    struct ibv_wc wc;
    int ret = ibv_poll_cq(cq_, 1, &wc);
    if (ret < 0) {
      LOG_ERRORF("Failed to poll completion queue: {}", ret);
      return false;
    }
    if (ret == 0) {
      continue;
    }
    if (wc.status != IBV_WC_SUCCESS) {
      LOG_ERRORF("Completion queue entry status: {}", ibv_wc_status_str(wc.status));
      return false;
    }
    if (wc.opcode == IBV_WC_RDMA_WRITE) {
      send_slots_++;
    }
  }

  auto io_ctx = prefetch_io_contexts_[slot];
  io_ctx->sge.length = offset + sizeof(uint32_t);
  uint32_t flags = IBV_SEND_SIGNALED;
  if (!PostWrite((uint64_t)RequestHeader::C2S_PREFETCH_REQUEST, 
                 &io_ctx->sge, 
                 1, 
                 io_ctx->recv_buffer->remote_address(), 
                 io_ctx->recv_buffer->remote_rkey(), flags)) {
    return false;
  }
  io_ctx->request_batch->size = 0;
  io_ctx->request_batch->bytes = 0;
  // set status
  prefetch_slot.status = SlotStatus::PENDING_RDMA_OP;
  return true;
}

bool TransferEngine::HandlePrefetchBatchWithSlot(char* batch_ptr, size_t batch_size, uint32_t slot_idx)
{
  // get the slot
  auto& prefetch_slot = prefetch_slots_[slot_idx];
  // post the write to the PD3 server
  auto data_buffer = reinterpret_cast<char*>(prefetch_slot.req);
  uint64_t offset = sizeof(uint32_t);
  auto request_header = (ProtocolHeader*)(data_buffer + offset);
  request_header->header = RequestHeader::C2S_PREFETCH_REQUEST;
  offset += sizeof(ProtocolHeader);

  // copy the batch
  std::memcpy(data_buffer + offset, batch_ptr, batch_size);
  offset += batch_size;
  // set the message size
  *reinterpret_cast<uint32_t*>(data_buffer) = offset - sizeof(uint32_t);
  // set the 1
  *reinterpret_cast<uint32_t*>(data_buffer + offset) = 1;


  while (send_slots_ == 0) { // maybe separate this between prefetch and transfer
    struct ibv_wc wc;
    int ret = ibv_poll_cq(cq_, 1, &wc);
    if (ret < 0) {
      LOG_ERRORF("Failed to poll completion queue: {}", ret);
      return false;
    }
    if (ret == 0) {
      continue;
    }
    if (wc.status != IBV_WC_SUCCESS) {
      LOG_ERRORF("Completion queue entry status: {}", ibv_wc_status_str(wc.status));
      return false;
    }
    if (wc.opcode == IBV_WC_RDMA_WRITE) {
      send_slots_++;
    }
  }

  auto io_ctx = prefetch_io_contexts_[slot_idx];
  io_ctx->sge.length = offset + sizeof(uint32_t);
  uint32_t flags = IBV_SEND_SIGNALED;
  if (!PostWrite((uint64_t)RequestHeader::C2S_PREFETCH_REQUEST, 
                 &io_ctx->sge, 
                 1, 
                 io_ctx->recv_buffer->remote_address(), 
                 io_ctx->recv_buffer->remote_rkey(), flags)) {
    return false;
  }
  io_ctx->request_batch->size = 0;
  io_ctx->request_batch->bytes = 0;
  // set status
  prefetch_slot.status = SlotStatus::PENDING_RDMA_OP;
  return true;
}

bool TransferEngine::HasPrefetchSlots()
{
  return !prefetch_deque_.empty();
}

bool TransferEngine::PollPrefetchSlot(uint32_t slot_idx) {
  auto& prefetch_slot = prefetch_slots_[slot_idx];
  auto& io_ctx = prefetch_io_contexts_[slot_idx];
  if (io_ctx->recv_buffer->PollMessage()) {
    io_ctx->recv_buffer->RemoveMessage();
    return true;
  }
  prefetch_slot.status = SlotStatus::COMPLETED;
  return false;
}

bool TransferEngine::InitializeDma() 
{
  // open the doca device
  CHECK_DOCA(open_doca_device_with_pci(config_.dpu_pcie_addr.c_str(), nullptr, &doca_state_.dev));

  // create the dma context
  CHECK_DOCA(doca_dma_create(doca_state_.dev, &dma_ctx_));
  doca_state_.ctx = doca_dma_as_ctx(dma_ctx_);

  CHECK_DOCA(doca_ctx_set_state_changed_cb(doca_state_.ctx, dma_state_changed_callback));
  CHECK_DOCA(doca_dma_task_memcpy_set_conf(dma_ctx_, dma_memcpy_completed_callback, dma_memcpy_error_callback, config_.workq_depth));
  CHECK_DOCA(doca_ctx_set_user_data(doca_state_.ctx, { .ptr = this}));

  // init core objects
  auto num_bufs_in_inventory = config_.workq_depth * 4;
  LOG_INFOF("initializing doca buf inventory with {} buffers", num_bufs_in_inventory);
  CHECK_DOCA(init_core_objects(&doca_state_, config_.workq_depth, num_bufs_in_inventory));

  // save configs into state
  size_t client_mmap_desc_len;
  char client_mmap_desc[1024] = {0};
  char* host_req_buffer_addr = nullptr;
  size_t host_req_buffer_len = 0;
  char* host_resp_buffer_addr = nullptr;
  size_t host_resp_buffer_len = 0;

  if (config_.enable_transfer) {
    auto success = SaveConfigsIntoState(config_.export_desc_client_file_path, 
      config_.buf_client_file_path,
      &client_mmap_desc_len,
      client_mmap_desc,
      &host_req_buffer_addr,
      &host_req_buffer_len,
      nullptr, // &host_resp_buffer_addr,
      nullptr); //&host_resp_buffer_len);
    if (!success) {
    LOG_ERRORF("could not load configs into state");
    return false;
    }
    auto req_buffer_size = sizeof(dpf::buffer::RequestBuffer);
    auto final_req_buf_size = round_up_64(req_buffer_size);
    auto resp_buffer_size = sizeof(dpf::buffer::ResponseBuffer);
    auto final_resp_buf_size = round_up_64(resp_buffer_size);
    host_req_buffer_len = final_req_buf_size;
    host_resp_buffer_len = final_resp_buf_size;
  }

  host_req_addr_ = host_req_buffer_addr;
  host_resp_addr_ = host_resp_buffer_addr;

  // create memory range accessible by DOCA and RDMA
  LOG_INFOF("initializing {} slots with slot size {}", config_.num_slots, config_.slot_size);
  
  // this is for transfer requests
  size_t total_req_buffer_size = config_.slot_size * config_.num_slots;
  auto final_req_size = round_up_64(total_req_buffer_size);
  size_t resp_slot_size = config_.slot_size;
  size_t total_resp_buffer_size = resp_slot_size * config_.num_slots;
  auto final_resp_size = round_up_64(total_resp_buffer_size);
  // this is for prefetch requests
  size_t total_prefetch_buffer_size = config_.slot_size * config_.num_prefetch_slots;
  auto final_prefetch_size = round_up_64(total_prefetch_buffer_size);
  size_t prefetch_resp_slot_size = config_.slot_size;
  size_t total_prefetch_resp_buffer_size = prefetch_resp_slot_size * config_.num_prefetch_slots;
  auto final_prefetch_resp_size = round_up_64(total_prefetch_resp_buffer_size);
  // place for metadata and other copies
  auto metadata_sizes = buffer::RequestBuffer::METADATA_SIZE +
                        buffer::ResponseBuffer::METADATA_SIZE+
                        buffer::AgentBuffer::METADATA_SIZE;
  size_t total_mem_size = final_req_size + final_resp_size + final_prefetch_size + final_prefetch_resp_size + (metadata_sizes * 2);
  
  LOG_INFOF("allocating unified buffer memory: {}", total_mem_size);
  unified_buf_memory_ = (char*)malloc(total_mem_size);
  if (!unified_buf_memory_) {
    LOG_ERRORF("could not allocate unified buffer memory ({}B)", total_mem_size);
    return false;
  }
  unified_buf_mem_size_ = total_mem_size;
  rdma_buf_mem_size_ = final_req_size + final_resp_size + final_prefetch_size + final_prefetch_resp_size;

  // clear memory
  std::memset(unified_buf_memory_, 0, unified_buf_mem_size_);

  // mmap for dpu side
  CHECK_DOCA(doca_mmap_set_memrange(doca_state_.src_mmap, unified_buf_memory_, unified_buf_mem_size_));
  CHECK_DOCA(doca_mmap_start(doca_state_.src_mmap));

  // create remote mmap 
  if (config_.enable_transfer) {
    CHECK_DOCA(doca_mmap_create_from_export(NULL, 
                                            (const void*)client_mmap_desc, 
                                            client_mmap_desc_len,
                                            doca_state_.dev,
                                            &remote_mmap_));
  }
  
  if (config_.enable_agent) {
    size_t agent_mmap_desc_len;
    char agent_mmap_desc[1024] = {0};
    char* agent_buffer_addr;
    size_t agent_buffer_len;
    SaveConfigsIntoState(config_.export_desc_agent_file_path,
                        config_.buf_agent_file_path,
                        &agent_mmap_desc_len,
                        agent_mmap_desc,
                        &agent_buffer_addr,
                        &agent_buffer_len,
                        nullptr, nullptr);
    CHECK_DOCA(doca_mmap_create_from_export(NULL, 
                                            (const void*)agent_mmap_desc, 
                                            agent_mmap_desc_len,
                                            doca_state_.dev,
                                            &agent_mmap_));
    agent_buffer_addr_ = agent_buffer_addr;
    agent_buffer_len_ = agent_buffer_len;

    // create the agent buffer here + create an mmap for it
    agent_buffer_dpu_addr_ = (char*)malloc(agent_buffer_len_);
    if (!agent_buffer_dpu_addr_) {
      LOG_ERRORF("could not allocate agent buffer memory ({}B)", agent_buffer_len_);
      return false;
    }
    std::memset(agent_buffer_dpu_addr_, 0, agent_buffer_len_);
    CHECK_DOCA(doca_mmap_set_memrange(doca_state_.dst_mmap, agent_buffer_dpu_addr_, agent_buffer_len_));
    CHECK_DOCA(doca_mmap_start(doca_state_.dst_mmap));
    host_agent_meta_ = reinterpret_cast<BufferMetadata*>(agent_buffer_dpu_addr_);
    LOG_INFOF("Agent buffer DPU memory allocated and mmap created: agent_buffer_dpu_addr_ = {}", agent_buffer_dpu_addr_);
  }
  
  LOG_INFO("Creating slots and metadata");

  // create slots and metadata
  for (int i = 0; i < config_.num_slots; ++i) {
    UnifiedBufferSlot slot;
    slot.req = reinterpret_cast<char*>(unified_buf_memory_ + (config_.slot_size * i));
    slot.resp = reinterpret_cast<char*>(unified_buf_memory_ + (config_.slot_size * i) + final_req_size);
    unified_slots_.push_back(slot);
  }
  for (int i = 0; i < config_.num_prefetch_slots; ++i) {
    UnifiedBufferSlot slot;
    slot.req = reinterpret_cast<char*>(unified_buf_memory_ + (config_.slot_size * i) + final_req_size + final_resp_size);
    slot.resp = reinterpret_cast<char*>(unified_buf_memory_ + (config_.slot_size * i) + final_req_size + final_resp_size + final_prefetch_size);
    prefetch_slots_.push_back(slot);
  }
  size_t curr_offset = final_req_size + final_resp_size + final_prefetch_size + final_prefetch_resp_size;
  te_req_meta_ = reinterpret_cast<BufferMetadata*>(unified_buf_memory_ + curr_offset);
  curr_offset += sizeof(BufferMetadata);
  host_req_meta_ = reinterpret_cast<BufferMetadata*>(unified_buf_memory_ + curr_offset);
  curr_offset += sizeof(BufferMetadata);
  te_resp_meta_ = reinterpret_cast<BufferMetadata*>(unified_buf_memory_ + curr_offset);
  curr_offset += sizeof(BufferMetadata);
  host_resp_meta_ = reinterpret_cast<BufferMetadata*>(unified_buf_memory_ + curr_offset);
  curr_offset += sizeof(BufferMetadata);
  agent_meta_ = reinterpret_cast<BufferMetadata*>(unified_buf_memory_ + curr_offset);
  // initialize
  te_req_meta_->consumer_idx.value = 0;

  // create DMA job vector
  for (int i = 0; i < config_.num_slots * 4; ++i) {
    auto job_ptr = new DmaJob();
    all_jobs_.push_back(job_ptr);
    dma_jobs_.push(job_ptr);
  }

  // initialize the transfer deque
  transfer_deque_.length = config_.num_slots;
  transfer_deque_.num_slots = config_.num_slots;
  transfer_deque_.base_slot_idx = 0;
  // initialize the prefetch deque
  prefetch_deque_.length = config_.num_prefetch_slots;
  prefetch_deque_.num_slots = config_.num_prefetch_slots;
  prefetch_deque_.base_slot_idx = 0;
  return true;
}

bool TransferEngine::InitializeSlots()
{
  LOG_INFOF("initializing {} slots with slot size {}", config_.num_slots, config_.slot_size);
  
  // this is for transfer requests
  size_t total_req_buffer_size = config_.slot_size * config_.num_slots;
  auto final_req_size = round_up_64(total_req_buffer_size);
  size_t resp_slot_size = config_.slot_size;
  size_t total_resp_buffer_size = resp_slot_size * config_.num_slots;
  auto final_resp_size = round_up_64(total_resp_buffer_size);
  // this is for prefetch requests
  size_t total_prefetch_buffer_size = config_.slot_size * config_.num_prefetch_slots;
  auto final_prefetch_size = round_up_64(total_prefetch_buffer_size);
  size_t prefetch_resp_slot_size = config_.slot_size;
  size_t total_prefetch_resp_buffer_size = prefetch_resp_slot_size * config_.num_prefetch_slots;
  auto final_prefetch_resp_size = round_up_64(total_prefetch_resp_buffer_size);
  // place for metadata and other copies
  auto metadata_sizes = buffer::RequestBuffer::METADATA_SIZE +
                        buffer::ResponseBuffer::METADATA_SIZE+
                        buffer::AgentBuffer::METADATA_SIZE;
  size_t total_mem_size = final_req_size + final_resp_size + final_prefetch_size + final_prefetch_resp_size + (metadata_sizes * 2);
  
  LOG_INFOF("allocating unified buffer memory: {}", total_mem_size);
  unified_buf_memory_ = (char*)malloc(total_mem_size);
  if (!unified_buf_memory_) {
    LOG_ERRORF("could not allocate unified buffer memory ({}B)", total_mem_size);
    return false;
  }
  unified_buf_mem_size_ = total_mem_size;
  rdma_buf_mem_size_ = final_req_size + final_resp_size + final_prefetch_size + final_prefetch_resp_size;

  // clear memory
  std::memset(unified_buf_memory_, 0, unified_buf_mem_size_);

  // create the slots and metadata
  LOG_INFO("Creating slots and metadata");

  // create slots and metadata
  for (int i = 0; i < config_.num_slots; ++i) {
    UnifiedBufferSlot slot;
    slot.req = reinterpret_cast<char*>(unified_buf_memory_ + (config_.slot_size * i));
    slot.resp = reinterpret_cast<char*>(unified_buf_memory_ + (config_.slot_size * i) + final_req_size);
    unified_slots_.push_back(slot);
  }
  for (int i = 0; i < config_.num_prefetch_slots; ++i) {
    UnifiedBufferSlot slot;
    slot.req = reinterpret_cast<char*>(unified_buf_memory_ + (config_.slot_size * i) + final_req_size + final_resp_size);
    slot.resp = reinterpret_cast<char*>(unified_buf_memory_ + (config_.slot_size * i) + final_req_size + final_resp_size + final_prefetch_size);
    prefetch_slots_.push_back(slot);
  }
  size_t curr_offset = final_req_size + final_resp_size + final_prefetch_size + final_prefetch_resp_size;
  te_req_meta_ = reinterpret_cast<BufferMetadata*>(unified_buf_memory_ + curr_offset);
  curr_offset += sizeof(BufferMetadata);
  host_req_meta_ = reinterpret_cast<BufferMetadata*>(unified_buf_memory_ + curr_offset);
  curr_offset += sizeof(BufferMetadata);
  te_resp_meta_ = reinterpret_cast<BufferMetadata*>(unified_buf_memory_ + curr_offset);
  curr_offset += sizeof(BufferMetadata);
  host_resp_meta_ = reinterpret_cast<BufferMetadata*>(unified_buf_memory_ + curr_offset);
  curr_offset += sizeof(BufferMetadata);
  agent_meta_ = reinterpret_cast<BufferMetadata*>(unified_buf_memory_ + curr_offset);
  // initialize
  te_req_meta_->consumer_idx.value = 0;
  return true;
}



bool TransferEngine::SaveConfigsIntoState(const std::string& export_path,
                                         const std::string& buf_path,
                                         size_t* export_desc_len,
                                         char* export_desc,
                                         char** remote_addr,
                                         size_t* remote_addr_len,
                                         char** remote_addr_2,
                                         size_t* remote_addr_len_2)
{
  static constexpr size_t RECV_BUF_SIZE = 1024;
  static constexpr size_t MAX_DMA_BUF_SIZE = 1024;

  FILE* fp;
  long file_size;
  char buffer[RECV_BUF_SIZE];

  fp = fopen(export_path.c_str(), "r");
  if (fp == nullptr) {
    LOG_ERRORF("failed to open export file: {}", export_path);
    return false;
  }

  if (fseek(fp, 0, SEEK_END) != 0) {
    LOG_ERRORF("failed to calculate file size: {}", export_path);
    fclose(fp);
    return false;
  }

  file_size = ftell(fp);
  if (file_size == -1) {
    LOG_ERRORF("failed to calculate file size: {}", export_path);
    fclose(fp);
    return false;
  }

  if (file_size > MAX_DMA_BUF_SIZE) {
    LOG_ERRORF("file size is too large: {}", export_path);
    file_size = MAX_DMA_BUF_SIZE;
  }

  *export_desc_len = file_size;

  if (fseek(fp, 0L, SEEK_SET) != 0) {
    LOG_ERRORF("failed to seek to start of file: {}", export_path);
    fclose(fp);
    return false;
  }

  if (fread(export_desc, 1, file_size, fp) != file_size) {
    LOG_ERRORF("failed to read export descriptor: {}", export_path);
    fclose(fp);
    return false;
  }

  fclose(fp);

  // read the source buffer information from file
  fp = fopen(buf_path.c_str(), "r");
  if (fp == nullptr) {
    LOG_ERRORF("failed to open buffer file: {}", buf_path);
    return false;
  }

  if (fgets(buffer, RECV_BUF_SIZE, fp) == nullptr) {
    LOG_ERRORF("failed to read source buffer address: {}", buf_path);
    fclose(fp);
    return false;
  }

  *remote_addr = (char*)strtoull(buffer, nullptr, 0);
  memset(buffer, 0, RECV_BUF_SIZE);

  if (fgets(buffer, RECV_BUF_SIZE, fp) == nullptr) {
    LOG_ERRORF("failed to read source buffer length: {}", buf_path);
    fclose(fp);
    return false; 
  }
  *remote_addr_len = strtoull(buffer, nullptr, 0);

  if (remote_addr_2 != nullptr && remote_addr_len_2 != nullptr) {
    memset(buffer, 0, RECV_BUF_SIZE);
    if (fgets(buffer, RECV_BUF_SIZE, fp) == nullptr) {
      LOG_ERRORF("failed to read source buffer address 2: {}", buf_path);
      fclose(fp);
      return false; 
    }
    *remote_addr_2 = (char*)strtoull(buffer, nullptr, 0);
    memset(buffer, 0, RECV_BUF_SIZE);
    if (fgets(buffer, RECV_BUF_SIZE, fp) == nullptr) {
      LOG_ERRORF("failed to read source buffer length 2: {}", buf_path);
      fclose(fp);
      return false; 
    }
    *remote_addr_len_2 = strtoull(buffer, nullptr, 0);
  }
  if (fp != nullptr) {
    fclose(fp);
  }
  return true;
}

void TransferEngine::WorkerThread()
{
  // Pin this thread to a CPU core (core 9 for example)
  int core_id = 9;
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(core_id, &cpuset);
  int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
  if (rc != 0) {
    std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
  }
  // kick off stuff
  if (config_.enable_transfer) {
    meta_read_outstanding_ = true;
    ClientReqMetadataRead();
  }
  // core event loop
  bool pause = false;
  // auto start_time = std::chrono::high_resolution_clock::now();
  while (running_.load(std::memory_order_relaxed)) {
    // if (total_prefetched_batches_ == 2048) {
    //   break;
    // }
    // poll prefetch requests
    while (prefetch_deque_.get_num_slots() > 0) {
      bool has_batch = false;
      for (auto prefetch_req_batch_q : prefetch_req_batch_qs_) {
        PrefetchRequestBatch** batch = prefetch_req_batch_q->front();
        if (batch) {
          auto batch_ptr = *batch;
          prefetch_req_batch_q->pop();
          HandlePrefetchBatch((char*)batch_ptr->keys, batch_ptr->size * sizeof(uint64_t));
          has_batch = true;
        }
      }
      if (!has_batch) {
        break;
      }
    }
    PollRdmaCompletions();  // poll rdma completions
    while (ProgressDmaEvent() != 0) {
      pause = true;
    }
    RecycleSlots();  // recycle slots
    // if a certain number of slots are completed, then fetch agent data
    if (total_num_bytes_written_ - total_num_bytes_read_ >= 4_MiB) {
      // kick off a new read
      AgentDataRead();
    }
    if (pause) {
      std::this_thread::yield();
    }
    pause = false;
  }
  // auto end_time = std::chrono::high_resolution_clock::now();
  // auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
  // auto requests = total_prefetched_batches_ * 500;
  // double tput = (double)requests / duration.count();
  // LOG_INFOF("Throughput: {} million requests/s", tput);
}

void TransferEngine::WorkerThreadNoDma()
{
  auto start_time = std::chrono::high_resolution_clock::now();
  int count = 0;
  while (running_.load(std::memory_order_relaxed)) {
    // poll prefetch requests
    while (prefetch_deque_.get_num_slots() > 0) {
      bool has_batch = false;
      for (auto prefetch_req_batch_q : prefetch_req_batch_qs_) {
        PrefetchRequestBatch** batch = prefetch_req_batch_q->front();
        if (batch) {
          auto batch_ptr = *batch;
          prefetch_req_batch_q->pop();
          HandlePrefetchBatch((char*)batch_ptr->keys, batch_ptr->size * sizeof(uint64_t));
          has_batch = true;
        }
      }
      if (!has_batch) {
        break;
      }
    }
    PollRdmaCompletionsNoDma();  // poll rdma completions
    RecycleSlots();  // recycle slots
    if (count <= 0) {
      std::this_thread::yield();
    }
    if (total_prefetched_batches_ == 2048) {
      break;
    }
  }
  auto end_time = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end_time - start_time);
  auto requests = total_prefetched_batches_ * 500;
  double tput = (double)requests / duration.count();
  LOG_INFOF("Throughput: {} million requests/s", tput);
}

int TransferEngine::ProgressDmaEvent()
{
  // Progress the PE, invoking callbacks for any completed tasks
  // Returns 1 if any progress was made, and 0 otherwise
  return doca_pe_progress(doca_state_.pe);
}

void TransferEngine::HandleDmaCompletion(DmaJob* dma_job) {
  // now we have successfully completed an event
  switch (dma_job->user_event_type)
  {
  case REQ_BUFFER_METADATA_READ: {
    auto consumer_idx = req_consumer_idx_;
    auto producer_idx = host_req_meta_->producer_idx.value;
    auto progress_idx = host_req_meta_->progress_idx.value;
    if (consumer_idx == producer_idx || progress_idx != producer_idx) {
      // no new requests yet
      meta_read_outstanding_ = true;
      ClientReqMetadataRead();
      break;
    }
    if (transfer_deque_.full()) {
      // we will try to submit requests when we poll RDMA responses
      meta_read_outstanding_ = false;
      break;
    }
    size_t available_bytes = 0;
    size_t read_size = 0;
    if (progress_idx > consumer_idx) {
      available_bytes = progress_idx - consumer_idx;
      read_size = available_bytes;
    } else {
      available_bytes = PD3_RING_BUFFER_SIZE - consumer_idx;
      read_size = available_bytes + progress_idx;
    }
    size_t bytes_completed = 0;
    while (bytes_completed < read_size) {
      auto curr_tail = transfer_deque_.tail();
      auto& unified_slot = unified_slots_[curr_tail];
      unified_slot.status = SlotStatus::PENDING_DMA_READ;
      const size_t MAX_READ_SIZE = 12000;
      size_t slot_read_size = std::min(read_size - bytes_completed, MAX_READ_SIZE);
      auto available_bytes = PD3_RING_BUFFER_SIZE - consumer_idx;
      if (slot_read_size > available_bytes) {
        // set the size of the request and the protocol header
        *reinterpret_cast<uint32_t*>(unified_slot.req) = slot_read_size;
        *reinterpret_cast<ProtocolHeader*>(unified_slot.req + sizeof(uint32_t)) = ProtocolHeader{RequestHeader::C2S_TRANSFER_REQUEST};
        auto read_offset = sizeof(uint32_t) + sizeof(ProtocolHeader);
        ClientReqDataRead(curr_tail, read_offset, consumer_idx, available_bytes);
        ClientReqDataRead(curr_tail, available_bytes, 0, slot_read_size - available_bytes);
        unified_slot.num_reads_remaining = 2; // we will read two segments
        // mark the ending 1 as well
        *reinterpret_cast<uint32_t*>(unified_slot.req + sizeof(uint32_t) + sizeof(ProtocolHeader) + slot_read_size) = 1;
        transfer_io_contexts_[curr_tail]->sge.length = sizeof(uint32_t) + sizeof(ProtocolHeader) + slot_read_size + sizeof(uint32_t);
      } else {
        *reinterpret_cast<uint32_t*>(unified_slot.req) = slot_read_size;
        *reinterpret_cast<ProtocolHeader*>(unified_slot.req + sizeof(uint32_t)) = ProtocolHeader{RequestHeader::C2S_TRANSFER_REQUEST};
        auto read_offset = sizeof(uint32_t) + sizeof(ProtocolHeader);
        ClientReqDataRead(curr_tail, read_offset, consumer_idx, slot_read_size);
        unified_slot.num_reads_remaining = 1; // we will read one segment
        // mark the ending 1 as well
        *reinterpret_cast<uint32_t*>(unified_slot.req + sizeof(uint32_t) + sizeof(ProtocolHeader) + slot_read_size) = 1;
        transfer_io_contexts_[curr_tail]->sge.length = sizeof(uint32_t) + sizeof(ProtocolHeader) + slot_read_size + sizeof(uint32_t);
      }
      consumer_idx = (consumer_idx + slot_read_size) % PD3_RING_BUFFER_SIZE;
      bytes_completed += slot_read_size;
      transfer_deque_.advance_tail();
      if (transfer_deque_.full()) {
        meta_read_outstanding_ = false; // mark new reads from slot recycling
        break;
      }
    }
    req_consumer_idx_ = consumer_idx;
    if (!transfer_deque_.full()) {
      meta_read_outstanding_ = true;
      ClientReqMetadataRead(); // we can fill more slots
    }
    if (!meta_write_outstanding_ && te_req_meta_->consumer_idx.value != req_consumer_idx_) {
      meta_write_outstanding_ = true;
      te_req_meta_->consumer_idx.value = req_consumer_idx_;
      ClientReqMetadataWrite(); // we can write consumer idx down
    }
    break;
  }
  case REQ_BUFFER_METADATA_WRITE: {
    if (te_req_meta_->consumer_idx.value != req_consumer_idx_) {
      te_req_meta_->consumer_idx.value = req_consumer_idx_;
      meta_write_outstanding_ = true;
      ClientReqMetadataWrite();
    } else {
      meta_write_outstanding_ = false;
    }
    break;
  }
  case REQ_BUFFER_DATA_READ: {
    auto unified_slot_id = dma_job->slot_id;
    auto& unified_slot = unified_slots_[unified_slot_id];
    unified_slot.num_reads_remaining--;
    if (unified_slot.num_reads_remaining != 0) {
      // the slot is not complete
      break;
    }
    // send the request to the RDMA server
    while (send_slots_ == 0) {
      struct ibv_wc wc;
      int ret = ibv_poll_cq(cq_, 1, &wc);
      if (ret < 0) {
        LOG_ERRORF("Failed to poll completion queue: {}", ret);
        break;
      }
      if (wc.status != IBV_WC_SUCCESS) {
        LOG_ERRORF("Completion queue entry status: {}", ibv_wc_status_str(wc.status));
        break;
      }
      if (wc.opcode == IBV_WC_RDMA_WRITE) {
        send_slots_++;
      }
    }
    HandleWriteRefresh(unified_slot_id); // this data is being written to remote memory
    auto io_ctx = transfer_io_contexts_[unified_slot_id];
    uint32_t flags = IBV_SEND_SIGNALED;
    if (!PostWrite((uint64_t)RequestHeader::C2S_TRANSFER_REQUEST,
                   &io_ctx->sge,
                   1,
                   io_ctx->recv_buffer->remote_address(),
                   io_ctx->recv_buffer->remote_rkey(),
                   flags)) {
      LOG_ERRORF("Failed to post write for transfer request");
      break;
    }
    unified_slot.status = SlotStatus::PENDING_RDMA_OP;
    break;
  }
  case RESP_BUFFER_METADATA_READ: {
    // TODO:
    // check whether we can produce to the response buffer
    // auto consumer_idx = host_resp_meta_->consumer_idx.value;
    // auto progress_idx = host_resp_meta_->progress_idx.value;
    // auto producer_idx = te_resp_meta_->producer_idx.value;
    // if (consumer_idx != progress_idx) {
    //   ClientRespMetadataRead();
    //   return; // we still cannot write, kick off another read
    // }
    // size_t distance = 0;
    // if (consumer_idx >= producer_idx) {
    //   distance = consumer_idx - producer_idx;
    // } else {
    //   distance = PD3_RING_BUFFER_SIZE - producer_idx + consumer_idx;
    // }
    // if (distance < config_.batch_size) {
    //   ClientRespMetadataRead();
    //   return; // we cannot write, kick off another read
    // }
    // // let the polling function do the writes
    // break;
    break;
  }
  case RESP_BUFFER_DATA_WRITE: {
    auto unified_slot_id = dma_job->slot_id;
    auto& unified_slot = unified_slots_[unified_slot_id];
    unified_slot.num_writes_remaining--;
    if (unified_slot.num_writes_remaining != 0) {
      // the slot is not complete
      break;
    }
    HandleReadRefresh(unified_slot_id); // this data is being read from remote memory
    transfer_io_contexts_[unified_slot_id]->recv_buffer->RemoveMessage();
    unified_slot.status = SlotStatus::COMPLETED;
    unified_slot.num_writes_remaining = 0;
    unified_slot.num_reads_remaining = 0;
    break;
  }
  case RESP_BUFFER_METADATA_WRITE: {
    resp_meta_write_outstanding_ = false;
    ClientRespMetadataWrite();
   break;
  }
  case AGENT_BUFFER_DATA_WRITE: {
    auto prefetch_slot_id = dma_job->slot_id;
    auto& prefetch_slot = prefetch_slots_[prefetch_slot_id];
    prefetch_slot.num_writes_remaining--;
    if (prefetch_slot.num_writes_remaining != 0) {
      // the slot is not complete
      // LOG_INFOF("Agent buffer write not complete for slot: {}", prefetch_slot_id);
      break;
    }
    // LOG_INFOF("Agent buffer write completed for slot: {}", prefetch_slot_id);
    prefetch_io_contexts_[prefetch_slot_id]->recv_buffer->RemoveMessage();
    prefetch_slot.status = SlotStatus::COMPLETED;
    prefetch_slot.num_writes_remaining = 0;
    break;
  }
  case AGENT_BUFFER_DATA_READ: {
    num_agent_buffer_reads_outstanding_--;
    if (num_agent_buffer_reads_outstanding_ != 0) {
      break;
    }
    agent_meta_->consumer_idx.value = total_num_bytes_read_;
    // write slots down to memory until buffer filled
    for (uint64_t i = 0; i < prefetch_deque_.get_num_full_slots(); ++i) {
      auto slot_idx = prefetch_deque_.get_slot_at_idx_from_head(i);
      auto& slot = prefetch_slots_[slot_idx];
      if (slot.status == SlotStatus::BLOCKED_DUE_TO_BACKPRESSURE) {
        if (!AgentDataWrite(slot_idx)) {
          AgentDataRead(); // kicik off a new read
          break;
        } else slot.status = SlotStatus::PENDING_DMA_WRITE;
      } else {
        break;
      }
    }
    break;
  }
  case AGENT_BUFFER_METADATA_WRITE: {
    agent_metadata_write_outstanding_ = false;
    AgentBufferMetadataWrite(); // see if more to write
    break;
  }
  default:
    break;
  }
}

void TransferEngine::PollRdmaCompletions()
{
  for (uint64_t i = 0; i < prefetch_deque_.get_num_full_slots(); ++i) {
    auto slot_idx = prefetch_deque_.get_slot_at_idx_from_head(i);
    auto& slot = prefetch_slots_[slot_idx];
    if (slot.status == SlotStatus::PENDING_RDMA_OP) {
      auto io_ctx = prefetch_io_contexts_[slot_idx];
      if (io_ctx->recv_buffer->PollMessage()) {
        // we have a completion
        // write this to the loading zone
        if (!AgentDataWrite(slot_idx)) {
          slot.status = SlotStatus::BLOCKED_DUE_TO_BACKPRESSURE;
          AgentDataRead(); // kick off a new read
          break;
        } else slot.status = SlotStatus::PENDING_DMA_WRITE;
      }
    } 
    if (slot.status == SlotStatus::BLOCKED_DUE_TO_BACKPRESSURE) {
      break;
    }
  }
  for (uint64_t i = 0; i < transfer_deque_.get_num_full_slots(); ++i) {
    auto slot_idx = transfer_deque_.get_slot_at_idx_from_head(i);
    auto& slot = unified_slots_[slot_idx];
    if (slot.status == SlotStatus::PENDING_RDMA_OP) {
      auto io_ctx = transfer_io_contexts_[slot_idx];
      if (io_ctx->recv_buffer->PollMessage()) {
        // we have a completion
        // write this to the transfer response buffer
        if (!WriteToTransferResponseBuffer(slot_idx)) {
          slot.status = SlotStatus::BLOCKED_DUE_TO_BACKPRESSURE;
          // TODO: kick off a new read
          break;
        } else slot.status = SlotStatus::PENDING_DMA_WRITE;
      }
    } 
  }
}

void TransferEngine::PollRdmaCompletionsNoDma()
{
  // TODO: change this slots impl, num_slots is the num of EMPTY slots
  for (uint64_t i = 0; i < prefetch_deque_.get_num_full_slots(); ++i) {
    auto slot_idx = prefetch_deque_.get_slot_at_idx_from_head(i);
    auto& slot = prefetch_slots_[slot_idx];
    if (slot.status == SlotStatus::PENDING_RDMA_OP) {
      auto io_ctx = prefetch_io_contexts_[slot_idx];
      if (io_ctx->recv_buffer->PollMessage()) {
        // we have a completion, mark this as completed
        if (!config_.testing_mode) {
          io_ctx->recv_buffer->RemoveMessage();
        }
        slot.status = SlotStatus::COMPLETED;
        total_prefetched_batches_++;
      }
    }
  }
}

UnifiedBufferSlot* TransferEngine::GetSlot(uint32_t slot_idx, bool prefetch)
{
  if (prefetch) {
    return &prefetch_slots_[slot_idx];
  } else {
    return &unified_slots_[slot_idx];
  }
}

void TransferEngine::RecycleSlots()
{
  while (!transfer_deque_.empty()) {
    auto head = transfer_deque_.head();
    auto& slot = unified_slots_[head];
    if (slot.status == SlotStatus::COMPLETED) {
      transfer_deque_.advance_head();
    } else {
      break;
    }
  }
  if (!meta_read_outstanding_ && !transfer_deque_.full()) {
    meta_read_outstanding_ = true;
    ClientReqMetadataRead();
  }
  while (!prefetch_deque_.empty()) {
    auto head = prefetch_deque_.head();
    auto& slot = prefetch_slots_[head];
    if (slot.status == SlotStatus::COMPLETED) {
      total_prefetched_batches_++;
      // LOG_INFOF("Recycling prefetch slot: {}", head);
      prefetch_deque_.advance_head();
    } else {
      break;
    }
  }
}

bool TransferEngine::ClientReqMetadataRead()
{
  // read the metadata from the client request buffer into the BufferMetadata struct at host_req_meta_
  auto dma_job = dma_jobs_.front();
  dma_jobs_.pop();
  struct doca_buf* src_doca_buf;
  struct doca_buf* dst_doca_buf;
  doca_error_t result;

  const size_t TRANSFER_SIZE = dpf::buffer::RequestBuffer::PRODUCER_METADATA_SIZE;

  dma_job->user_event_type = REQ_BUFFER_METADATA_READ;

	// the destination buffer is the host_req_meta_ structure
  auto dst_address = (void*)host_req_meta_;
	result = doca_buf_inventory_buf_get_by_addr(doca_state_.buf_inv,
	                                        doca_state_.src_mmap /*put mmap here*/,
																				  dst_address,
																				  TRANSFER_SIZE,
																				  &dst_doca_buf);
  if (result != DOCA_SUCCESS) {
    LOG_ERRORF("unable to allocate doca buffer: {}", doca_error_get_name(result));
		doca_buf_dec_refcount(dst_doca_buf, nullptr);
		return false;
	}
	// the source buffer is the buffer in the remote mmap
  auto src_address = (void*)host_req_addr_;
	result = doca_buf_inventory_buf_get_by_addr(doca_state_.buf_inv, 
																				  remote_mmap_ /*put mmap here*/,
																				  src_address,
																				  TRANSFER_SIZE,
																				  &src_doca_buf);
  if (result != DOCA_SUCCESS) {
		LOG_ERRORF("unable to allocate doca buffer: {}", doca_error_get_name(result));
		doca_buf_dec_refcount(src_doca_buf, nullptr);
		doca_buf_dec_refcount(dst_doca_buf, nullptr);
		return false;
	}
	
  result = doca_dma_task_memcpy_alloc_init(
    dma_ctx_,
    src_doca_buf,
    dst_doca_buf,
    { .ptr = static_cast<void*>(dma_job)},
    &dma_job->dma_task
  );
  if (result != DOCA_SUCCESS) {
    LOG_ERRORF("unable to allocate DOCA task: {}", doca_error_get_name(result));
    doca_buf_dec_refcount(src_doca_buf, nullptr);
    doca_buf_dec_refcount(dst_doca_buf, nullptr);
    return false;
  }

	dma_job->src_buf = src_doca_buf;
	dma_job->dest_buf = dst_doca_buf;
  dma_job->slot_id = -1; // no slot, this is a metadata read

	result = doca_buf_set_data(src_doca_buf, src_address, TRANSFER_SIZE);
	if (result != DOCA_SUCCESS) {
		std::cerr << "failed to set data for DOCA buffer: " << doca_error_get_name(result) << '\n';
		return false;
	}

  // submit the job
  result = doca_task_submit(doca_dma_task_memcpy_as_task(dma_job->dma_task));
  if (result != DOCA_SUCCESS) {
    // free all the stuff
		std::cerr << "error while submitting dma job: " << doca_error_get_name(result) << '\n';
		doca_buf_dec_refcount(src_doca_buf, nullptr);
		doca_buf_dec_refcount(dst_doca_buf, nullptr);
    return false;
  }

  return true;
}

bool TransferEngine::ClientReqMetadataWrite()
{
  LOG_INFO("Writing client metadata");
  // write consumer_idx from te_req_meta into host memory
  auto dma_job = dma_jobs_.front();
  dma_jobs_.pop();
  struct doca_buf* src_doca_buf;
  struct doca_buf* dst_doca_buf;
	doca_error_t result;

  const size_t TRANSFER_SIZE = sizeof(uint64_t);

  dma_job->user_event_type = REQ_BUFFER_METADATA_WRITE;

	// the src buffer is the memory containing the consumer idx
  auto src_address = (void*)(&te_req_meta_->consumer_idx);
	result = doca_buf_inventory_buf_get_by_addr(doca_state_.buf_inv,
	                                        doca_state_.src_mmap /*put mmap here*/,
																				  src_address,
																				  TRANSFER_SIZE,
																				  &src_doca_buf);
  if (result != DOCA_SUCCESS) {
		std::cerr << "unable to allocate doca source buffer: " << doca_error_get_name(result) << '\n';
		doca_buf_dec_refcount(src_doca_buf, nullptr);
		return false;
	}
	// the dst buffer is the buffer in the remote mmap
  auto dst_address = (void*)(host_req_addr_ + dpf::buffer::RequestBuffer::CONSUMER_INDEX_OFFSET);
	result = doca_buf_inventory_buf_get_by_addr(doca_state_.buf_inv, 
																				  remote_mmap_ /*put mmap here*/,
																				  dst_address,
																				  TRANSFER_SIZE,
																				  &dst_doca_buf);
  if (result != DOCA_SUCCESS) {
		std::cerr << "unable to allocate doca dest buffer: " << doca_error_get_name(result) << '\n';
		doca_buf_dec_refcount(src_doca_buf, nullptr);
		doca_buf_dec_refcount(dst_doca_buf, nullptr);
		return false;
	}

  result = doca_dma_task_memcpy_alloc_init(
    dma_ctx_,
    src_doca_buf,
    dst_doca_buf,
    { .ptr = static_cast<void*>(dma_job)},
    &dma_job->dma_task
  );
  if (result != DOCA_SUCCESS) {
    LOG_ERRORF("unable to allocate DOCA task: {}", doca_error_get_name(result));
    doca_buf_dec_refcount(src_doca_buf, nullptr);
    doca_buf_dec_refcount(dst_doca_buf, nullptr);
    return false;
  }

	dma_job->src_buf = src_doca_buf;
	dma_job->dest_buf = dst_doca_buf;
  dma_job->slot_id = -1; // no slot, this is a metadata write

	result = doca_buf_set_data(src_doca_buf, src_address, TRANSFER_SIZE);
	if (result != DOCA_SUCCESS) {
		std::cerr << "failed to set data for DOCA buffer: " << doca_error_get_name(result) << '\n';
		return false;
	}

  // submit the job
  result = doca_task_submit(doca_dma_task_memcpy_as_task(dma_job->dma_task));
  if (result != DOCA_SUCCESS) {
    // free all the stuff
		std::cerr << "error while submitting dma job: " << doca_error_get_name(result) << '\n';
		doca_buf_dec_refcount(src_doca_buf, nullptr);
		doca_buf_dec_refcount(dst_doca_buf, nullptr);
    return false;
  }
  return true;
}

bool TransferEngine::ClientReqDataRead(uint32_t slot_idx,
                                       uint64_t slot_offset, 
                                       uint64_t data_offset,
                                       size_t size)
{
  // LOG_INFOF("ClientReqDataRead: {}, {}, {}, {}", slot_idx, slot_offset, data_offset, size);
  // read the data into the slot memory given by slot_idx
  auto& slot = unified_slots_[slot_idx];

  auto& dma_job = dma_jobs_.front();
  dma_jobs_.pop();
  struct doca_buf* src_doca_buf;
  struct doca_buf* dst_doca_buf;
	doca_error_t result;

  dma_job->user_event_type = REQ_BUFFER_DATA_READ;
  // LOG_INFOF("Submitting {} event_type", dma_job->user_event_type);

	// the dst buffer is the slot memory
  // need to leave space for the size of the request and the protocol header
  char* dst_address = slot.req + slot_offset;
	result = doca_buf_inventory_buf_get_by_addr(doca_state_.buf_inv,
	                                        doca_state_.src_mmap /*put mmap here*/,
																				  dst_address,
																				  size,
																				  &dst_doca_buf);
  if (result != DOCA_SUCCESS) {
		std::cerr << "unable to allocate doca source buffer: " << doca_error_get_name(result) << '\n';
		doca_buf_dec_refcount(dst_doca_buf, nullptr);
		return false;
	}
	// the dst buffer is the buffer in the remote mmap
  auto src_address = (void*)(host_req_addr_ + dpf::buffer::RequestBuffer::DATA_OFFSET + data_offset);
	result = doca_buf_inventory_buf_get_by_addr(doca_state_.buf_inv, 
																				  remote_mmap_ /*put mmap here*/,
																				  src_address,
																				  size,
																				  &src_doca_buf);
  if (result != DOCA_SUCCESS) {
		std::cerr << "unable to allocate doca dest buffer: " << doca_error_get_name(result) << '\n';
		doca_buf_dec_refcount(src_doca_buf, nullptr);
		doca_buf_dec_refcount(dst_doca_buf, nullptr);
		return false;
  }

  result = doca_dma_task_memcpy_alloc_init(
    dma_ctx_,
    src_doca_buf,
    dst_doca_buf,
    { .ptr = static_cast<void*>(dma_job)},
    &dma_job->dma_task
  );
  if (result != DOCA_SUCCESS) {
    LOG_ERRORF("unable to allocate DOCA task: {}", doca_error_get_name(result));
    doca_buf_dec_refcount(src_doca_buf, nullptr);
    doca_buf_dec_refcount(dst_doca_buf, nullptr);
    return false;
  }

	dma_job->src_buf = src_doca_buf;
	dma_job->dest_buf = dst_doca_buf;
  dma_job->slot_id = slot_idx;

	result = doca_buf_set_data(src_doca_buf, src_address, size);
	if (result != DOCA_SUCCESS) {
		std::cerr << "failed to set data for DOCA buffer: " << doca_error_get_name(result) << '\n';
		return false;
	}

  // submit the job
  result = doca_task_submit(doca_dma_task_memcpy_as_task(dma_job->dma_task));
  if (result != DOCA_SUCCESS) {
    // free all the stuff
		std::cerr << "error while submitting dma job: " << doca_error_get_name(result) << '\n';
		doca_buf_dec_refcount(src_doca_buf, nullptr);
		doca_buf_dec_refcount(dst_doca_buf, nullptr);
    return false;
  }
  LOG_INFO("Submitted data read job");

  return true;
}

bool TransferEngine::ClientRespMetadataRead()
{
  // called to refresh the consumer idx for the response buffer
  return false;
}

bool TransferEngine::ClientRespMetadataWrite()
{
  if (resp_meta_write_outstanding_) {
    // we will kick off the new metadata write once the current one finishes
    return true;
  }
  if (resp_producer_idx_ == te_resp_meta_->producer_idx.value) {
    // there is nothing new to write, so return
    return true;
  }
  // write the producer idx to the client buffer
  auto dma_job = dma_jobs_.front();
  dma_jobs_.pop();
  struct doca_buf* src_doca_buf;
  struct doca_buf* dst_doca_buf;
	doca_error_t result;

  const size_t TRANSFER_SIZE = sizeof(uint64_t);

  dma_job->user_event_type = RESP_BUFFER_METADATA_WRITE;

  te_resp_meta_->producer_idx.value = resp_producer_idx_;

	// the src buffer is the memory containing the consumer idx
  auto src_address = (void*)(&te_resp_meta_->producer_idx);
	result = doca_buf_inventory_buf_get_by_addr(doca_state_.buf_inv,
	                                        doca_state_.src_mmap /*put mmap here*/,
																				  src_address,
																				  TRANSFER_SIZE,
																				  &src_doca_buf);
  if (result != DOCA_SUCCESS) {
		std::cerr << "unable to allocate doca source buffer: " << doca_error_get_name(result) << '\n';
		doca_buf_dec_refcount(src_doca_buf, nullptr);
		return false;
	}
	// the dst buffer is the buffer in the remote mmap
  auto dst_address = (void*)(host_resp_addr_);
	result = doca_buf_inventory_buf_get_by_addr(doca_state_.buf_inv, 
																				  remote_mmap_ /*put mmap here*/,
																				  dst_address,
																				  TRANSFER_SIZE,
																				  &dst_doca_buf);
  if (result != DOCA_SUCCESS) {
		std::cerr << "unable to allocate doca dest buffer: " << doca_error_get_name(result) << '\n';
		doca_buf_dec_refcount(src_doca_buf, nullptr);
		doca_buf_dec_refcount(dst_doca_buf, nullptr);
		return false;
	}

  result = doca_dma_task_memcpy_alloc_init(
    dma_ctx_,
    src_doca_buf,
    dst_doca_buf,
    { .ptr = static_cast<void*>(dma_job)},
    &dma_job->dma_task
  );
  if (result != DOCA_SUCCESS) {
    LOG_ERRORF("unable to allocate DOCA task: {}", doca_error_get_name(result));
    doca_buf_dec_refcount(src_doca_buf, nullptr);
    doca_buf_dec_refcount(dst_doca_buf, nullptr);
    return false;
  }

	dma_job->src_buf = src_doca_buf;
	dma_job->dest_buf = dst_doca_buf;
  dma_job->slot_id = -1; // no slot, this is a metadata write

	result = doca_buf_set_data(src_doca_buf, src_address, TRANSFER_SIZE);
	if (result != DOCA_SUCCESS) {
		std::cerr << "failed to set data for DOCA buffer: " << doca_error_get_name(result) << '\n';
		return false;
	}

  // submit the job
  result = doca_task_submit(doca_dma_task_memcpy_as_task(dma_job->dma_task));
  if (result != DOCA_SUCCESS) {
    // free all the stuff
		std::cerr << "error while submitting dma job: " << doca_error_get_name(result) << '\n';
		doca_buf_dec_refcount(src_doca_buf, nullptr);
		doca_buf_dec_refcount(dst_doca_buf, nullptr);
    return false;
  }
  resp_meta_write_outstanding_ = true;
  return true;
}

bool TransferEngine::ClientRespDataWrite(uint32_t slot_idx,
                                         uint64_t slot_offset,
                                         uint64_t data_offset,
                                         size_t size,
                                         uint64_t offset_2,
                                         size_t size_2)
{
  // LOG_INFOF("Writing Client Response Data: slot: {}, slot_offset: {}, data_offset: {}, size: {}", slot_idx, slot_offset, data_offset, size);
  auto& slot = unified_slots_[slot_idx];

  // TODO: handle when offset_2 and size_2 are non-zero

  // TODO: we need to handle backpressure here (i.e. when the buffer becomes full)
  // TODO: kick off a DMA read to the response buffer metadata
  if (dma_jobs_.empty()) {
    // THIS SHOULD NEVER HAPPEN
    LOG_ERROR("No DMA jobs available, this should never happen");
    return false;
  }
  auto dma_job = dma_jobs_.front();
  dma_jobs_.pop();
  struct doca_buf* src_doca_buf;
  struct doca_buf* dst_doca_buf;
	doca_error_t result;

  const size_t TRANSFER_SIZE = size;

  dma_job->user_event_type = RESP_BUFFER_DATA_WRITE;

	// the src buffer is the memory containing the consumer idx
  auto src_address = (void*)(slot.resp + slot_offset);
	result = doca_buf_inventory_buf_get_by_addr(doca_state_.buf_inv,
	                                        doca_state_.src_mmap /*put mmap here*/,
																				  src_address,
																				  TRANSFER_SIZE,
																				  &src_doca_buf);
  if (result != DOCA_SUCCESS) {
		std::cerr << "unable to allocate doca source buffer: " << doca_error_get_name(result) << '\n';
		doca_buf_dec_refcount(src_doca_buf, nullptr);
		return false;
	}
	// the dst buffer is the buffer in the remote mmap
  auto dst_address = (void*)(host_resp_addr_ + dpf::buffer::ResponseBuffer::DATA_OFFSET + data_offset);
	result = doca_buf_inventory_buf_get_by_addr(doca_state_.buf_inv, 
																				  remote_mmap_ /*put mmap here*/,
																				  dst_address,
																				  TRANSFER_SIZE,
																				  &dst_doca_buf);
  if (result != DOCA_SUCCESS) {
		std::cerr << "unable to allocate doca dest buffer: " << doca_error_get_name(result) << '\n';
		doca_buf_dec_refcount(src_doca_buf, nullptr);
		doca_buf_dec_refcount(dst_doca_buf, nullptr);
		return false;
	}

  result = doca_dma_task_memcpy_alloc_init(
    dma_ctx_,
    src_doca_buf,
    dst_doca_buf,
    { .ptr = static_cast<void*>(dma_job)},
    &dma_job->dma_task
  );
  if (result != DOCA_SUCCESS) {
    LOG_ERRORF("unable to allocate DOCA task: {}", doca_error_get_name(result));
    doca_buf_dec_refcount(src_doca_buf, nullptr);
    doca_buf_dec_refcount(dst_doca_buf, nullptr);
    return false;
  }
	dma_job->src_buf = src_doca_buf;
	dma_job->dest_buf = dst_doca_buf;
  dma_job->slot_id = slot_idx;

	result = doca_buf_set_data(src_doca_buf, src_address, TRANSFER_SIZE);
	if (result != DOCA_SUCCESS) {
		std::cerr << "failed to set data for DOCA buffer: " << doca_error_get_name(result) << '\n';
		return false;
	}

  // submit the job
  result = doca_task_submit(doca_dma_task_memcpy_as_task(dma_job->dma_task));
  if (result != DOCA_SUCCESS) {
    // free all the stuff
		std::cerr << "error while submitting dma job: " << doca_error_get_name(result) << '\n';
		doca_buf_dec_refcount(src_doca_buf, nullptr);
		doca_buf_dec_refcount(dst_doca_buf, nullptr);
    return false;
  }
  slot.num_writes_remaining = 1;
  return true;
}


bool TransferEngine::AgentDataWrite(uint32_t slot_idx)
{
  auto& slot = prefetch_slots_[slot_idx];
  auto producer_idx = agent_meta_->producer_idx.value;
  auto producer_pos = dpf::buffer::AgentBuffer::GetBufferIdx(producer_idx);
  auto consumer_idx = agent_meta_->consumer_idx.value;
  auto consumer_pos = dpf::buffer::AgentBuffer::GetBufferIdx(consumer_idx);

  uint32_t bytes_to_write = *reinterpret_cast<uint32_t*>(slot.resp);

  size_t remaining_space = PD3_RING_BUFFER_SIZE - producer_idx + consumer_idx;

  if (remaining_space < bytes_to_write) {
    // need to kick off a new DMA read to the agent buffer
    return false;
  }

  size_t remaining_space_before_wrap = PD3_RING_BUFFER_SIZE - producer_pos;
  if (remaining_space_before_wrap < bytes_to_write) {
    AgentDataWriteWithOffsetAndSize(slot_idx, sizeof(uint32_t), producer_pos, remaining_space_before_wrap);
    auto new_size = bytes_to_write - remaining_space_before_wrap;
    AgentDataWriteWithOffsetAndSize(slot_idx, sizeof(uint32_t) + remaining_space_before_wrap, 0, new_size);
    agent_meta_->producer_idx.value += bytes_to_write;
    slot.num_writes_remaining = 2;
    AgentBufferMetadataWrite();
    total_num_bytes_written_ += bytes_to_write;
    return true;
  }

  // continue as normal
  if (dma_jobs_.empty()) {
    // THIS SHOULD NEVER HAPPEN
    LOG_ERROR("No DMA jobs available, this should never happen");
    return false;
  }
  auto dma_job = dma_jobs_.front();
  dma_jobs_.pop();
  struct doca_buf* src_doca_buf;
  struct doca_buf* dst_doca_buf;
	doca_error_t result;

  const size_t TRANSFER_SIZE = bytes_to_write;

  dma_job->user_event_type = AGENT_BUFFER_DATA_WRITE;

	// the src buffer is the memory containing the consumer idx
  auto src_address = (void*)(slot.resp + sizeof(uint32_t));
	result = doca_buf_inventory_buf_get_by_addr(doca_state_.buf_inv,
	                                        doca_state_.src_mmap /*put mmap here*/,
																				  src_address,
																				  TRANSFER_SIZE,
																				  &src_doca_buf);
  if (result != DOCA_SUCCESS) {
    LOG_ERRORF("unable to allocate doca source buffer: {}", doca_error_get_name(result));
		doca_buf_dec_refcount(src_doca_buf, nullptr);
		return false;
	}
	// the dst buffer is the buffer in the remote mmap
  auto dst_address = (void*)(agent_buffer_addr_ + dpf::buffer::AgentBuffer::DATA_OFFSET + producer_pos);
	result = doca_buf_inventory_buf_get_by_addr(doca_state_.buf_inv, 
																				  agent_mmap_ /*put mmap here*/,
																				  dst_address,
																				  TRANSFER_SIZE,
																				  &dst_doca_buf);
  if (result != DOCA_SUCCESS) {
		LOG_ERRORF("unable to allocate doca dest buffer: {}", doca_error_get_name(result));
		doca_buf_dec_refcount(src_doca_buf, nullptr);
		doca_buf_dec_refcount(dst_doca_buf, nullptr);
		return false;
	}

  result = doca_dma_task_memcpy_alloc_init(
    dma_ctx_,
    src_doca_buf,
    dst_doca_buf,
    { .ptr = static_cast<void*>(dma_job)},
    &dma_job->dma_task
  );
  if (result != DOCA_SUCCESS) {
    LOG_ERRORF("unable to allocate DOCA task: {}", doca_error_get_name(result));
    doca_buf_dec_refcount(src_doca_buf, nullptr);
    doca_buf_dec_refcount(dst_doca_buf, nullptr);
    return false;
  }

	dma_job->src_buf = src_doca_buf;
	dma_job->dest_buf = dst_doca_buf;
  dma_job->slot_id = slot_idx;

	result = doca_buf_set_data(src_doca_buf, src_address, TRANSFER_SIZE);
	if (result != DOCA_SUCCESS) {
		std::cerr << "failed to set data for DOCA buffer: " << doca_error_get_name(result) << '\n';
		return false;
	}

  // submit the job
  result = doca_task_submit(doca_dma_task_memcpy_as_task(dma_job->dma_task));
  if (result != DOCA_SUCCESS) {
    // free all the stuff
		std::cerr << "error while submitting dma job: " << doca_error_get_name(result) << '\n';
		doca_buf_dec_refcount(src_doca_buf, nullptr);
		doca_buf_dec_refcount(dst_doca_buf, nullptr);
    return false;
  }
  slot.num_writes_remaining = 1;
  agent_meta_->producer_idx.value += TRANSFER_SIZE; // we have written to the agent buffer
  AgentBufferMetadataWrite();
  total_num_bytes_written_ += TRANSFER_SIZE;

  return true;
}

bool TransferEngine::AgentDataWriteWithOffsetAndSize(uint32_t slot_idx, 
                                                     uint64_t slot_offset, 
                                                     uint64_t data_offset, 
                                                     size_t size)
{
  auto& slot = prefetch_slots_[slot_idx];

  if (dma_jobs_.empty()) {
    // THIS SHOULD NEVER HAPPEN
    LOG_ERROR("No DMA jobs available, this should never happen");
    return false;
  }
  auto dma_job = dma_jobs_.front();
  dma_jobs_.pop();
  struct doca_buf* src_doca_buf;
  struct doca_buf* dst_doca_buf;
	doca_error_t result;

  const size_t TRANSFER_SIZE = size;

  dma_job->user_event_type = AGENT_BUFFER_DATA_WRITE;

	// the src buffer is the memory containing the data from the transfer engine
  auto src_address = (void*)(slot.resp + slot_offset);
	result = doca_buf_inventory_buf_get_by_addr(doca_state_.buf_inv,
	                                        doca_state_.src_mmap /*put mmap here*/,
																				  src_address,
																				  TRANSFER_SIZE,
																				  &src_doca_buf);
  if (result != DOCA_SUCCESS) {
		LOG_ERRORF("unable to allocate doca source buffer: {}", doca_error_get_name(result));
		doca_buf_dec_refcount(src_doca_buf, nullptr);
		return false;
	}
	// the dst buffer is the buffer in the remote mmap
  auto dst_address = (void*)(agent_buffer_addr_ + dpf::buffer::AgentBuffer::DATA_OFFSET + data_offset);
	result = doca_buf_inventory_buf_get_by_addr(doca_state_.buf_inv, 
																				  agent_mmap_ /*put mmap here*/,
																				  dst_address,
																				  TRANSFER_SIZE,
																				  &dst_doca_buf);
  if (result != DOCA_SUCCESS) {
		LOG_ERRORF("unable to allocate doca dest buffer: {}", doca_error_get_name(result));
		doca_buf_dec_refcount(src_doca_buf, nullptr);
		doca_buf_dec_refcount(dst_doca_buf, nullptr);
		return false;
	}

  result = doca_dma_task_memcpy_alloc_init(
    dma_ctx_, 
    src_doca_buf,
    dst_doca_buf,
    { .ptr = static_cast<void*>(dma_job)},
    &dma_job->dma_task
  );
  if (result != DOCA_SUCCESS) {
    LOG_ERRORF("unable to allocate DOCA task: {}", doca_error_get_name(result));
    doca_buf_dec_refcount(src_doca_buf, nullptr);
    doca_buf_dec_refcount(dst_doca_buf, nullptr);
    return false;
  }
	
	dma_job->src_buf = src_doca_buf;
	dma_job->dest_buf = dst_doca_buf;
  dma_job->slot_id = slot_idx;

	result = doca_buf_set_data(src_doca_buf, src_address, TRANSFER_SIZE);
	if (result != DOCA_SUCCESS) {
		LOG_ERRORF("failed to set data for DOCA buffer: {}", doca_error_get_name(result));
		return false;
	}

  // submit the job
  result = doca_task_submit(doca_dma_task_memcpy_as_task(dma_job->dma_task));
  if (result != DOCA_SUCCESS) {
    // free all the stuff
		LOG_ERRORF("error while submitting dma job: {}", doca_error_get_name(result));
		doca_buf_dec_refcount(src_doca_buf, nullptr);
		doca_buf_dec_refcount(dst_doca_buf, nullptr);
    return false;
  }
  slot.num_writes_remaining = 1;

  return true;
}

bool TransferEngine::WriteToTransferResponseBuffer(uint32_t slot_idx)
{
  auto& slot = unified_slots_[slot_idx];
  auto data_size = *reinterpret_cast<uint32_t*>(slot.resp);
  auto producer_idx = resp_producer_idx_;
  uint32_t slot_offset = sizeof(uint32_t);
  resp_producer_idx_ = (resp_producer_idx_ + data_size) % PD3_RING_BUFFER_SIZE;
  ClientRespDataWrite(slot_idx, slot_offset, producer_idx, data_size);
  ClientRespMetadataWrite();

  return true;
}

bool TransferEngine::AgentDataRead()
{
  if (num_agent_buffer_reads_outstanding_ != 0) {
    return true;
  }

  // the dst buffer is the agent buffer in DPU memory
  auto current_consumer_idx = agent_meta_->consumer_idx.value;
  auto current_producer_idx = agent_meta_->producer_idx.value;
  auto current_consumer_pos = dpf::buffer::AgentBuffer::GetBufferIdx(current_consumer_idx);
  auto current_producer_pos = dpf::buffer::AgentBuffer::GetBufferIdx(current_producer_idx);

  // max size, since DMA reads won't work for larger sizes
  const uint64_t MAX_READ_SIZE = 2_MiB;

  bool wrapped = current_producer_pos <= current_consumer_pos;
  size_t read_size = current_producer_idx - current_consumer_idx;
  if (read_size > MAX_READ_SIZE) {
    read_size = MAX_READ_SIZE;
    current_producer_pos = dpf::buffer::AgentBuffer::GetBufferIdx(current_consumer_idx + read_size);
    wrapped = current_producer_pos <= current_consumer_pos;
  }

  if (wrapped) {
    // we will need to issue two reads
    AgentDataReadWithOffsetAndSize(current_consumer_pos, PD3_RING_BUFFER_SIZE - current_consumer_pos);
    num_agent_buffer_reads_outstanding_ = 1;
    if (current_producer_pos > 0) {
      AgentDataReadWithOffsetAndSize(0, current_producer_pos);
      num_agent_buffer_reads_outstanding_++;
    }
  } else {
    AgentDataReadWithOffsetAndSize(current_consumer_pos, read_size);
    num_agent_buffer_reads_outstanding_ = 1;
  }

  total_num_bytes_read_ += read_size;

  return true;
}

bool TransferEngine::AgentDataReadWithOffsetAndSize(uint64_t offset, size_t size)
{
  if (dma_jobs_.empty()) {
    // THIS SHOULD NEVER HAPPEN
    LOG_ERROR("No DMA jobs available, this should never happen");
    return false;
  }
  auto& dma_job = dma_jobs_.front();
  dma_jobs_.pop();
  struct doca_buf* src_doca_buf;
  struct doca_buf* dst_doca_buf;
  doca_error_t result;

  dma_job->user_event_type = AGENT_BUFFER_DATA_READ;

  char* dst_address = agent_buffer_dpu_addr_ + dpf::buffer::AgentBuffer::DATA_OFFSET + offset;

  result = doca_buf_inventory_buf_get_by_addr(doca_state_.buf_inv,
      doca_state_.dst_mmap /*this is the dpu-side agent mmap*/,
      dst_address,
      size,
      &dst_doca_buf);
  if (result != DOCA_SUCCESS) {
    LOG_ERRORF("unable to allocate doca destination buffer: {}", doca_error_get_name(result));
    doca_buf_dec_refcount(dst_doca_buf, nullptr);
    return false;
  }
  // the src buffer is the agent buffer in host memory
  auto src_address = (void*)(agent_buffer_addr_ + dpf::buffer::AgentBuffer::DATA_OFFSET + offset);
  result = doca_buf_inventory_buf_get_by_addr(doca_state_.buf_inv, 
      agent_mmap_ /*put mmap here*/,
      src_address,
      size,
      &src_doca_buf);
  if (result != DOCA_SUCCESS) {
    LOG_ERRORF("unable to allocate doca dest buffer: {}", doca_error_get_name(result));
    doca_buf_dec_refcount(src_doca_buf, nullptr);
    doca_buf_dec_refcount(dst_doca_buf, nullptr);
    return false;
  }

  result = doca_dma_task_memcpy_alloc_init(
    dma_ctx_,
    src_doca_buf,
    dst_doca_buf,
    { .ptr = static_cast<void*>(dma_job)},
    &dma_job->dma_task
  );
  if (result != DOCA_SUCCESS) {
    LOG_ERRORF("unable to allocate DOCA task: {}", doca_error_get_name(result));
    doca_buf_dec_refcount(src_doca_buf, nullptr);
    doca_buf_dec_refcount(dst_doca_buf, nullptr);
    return false;
  }


  dma_job->src_buf = src_doca_buf;
  dma_job->dest_buf = dst_doca_buf;
  dma_job->slot_id = -1; // no slot, this is an agent data read

  result = doca_buf_set_data(src_doca_buf, src_address, size);
  if (result != DOCA_SUCCESS) {
    std::cerr << "failed to set data for DOCA buffer: " << doca_error_get_name(result) << '\n';
    return false;
  }

  // submit the job
  result = doca_task_submit(doca_dma_task_memcpy_as_task(dma_job->dma_task));
  if (result != DOCA_SUCCESS) {
    // free all the stuff
    std::cerr << "error while submitting dma job: " << doca_error_get_name(result) << '\n';
    doca_buf_dec_refcount(src_doca_buf, nullptr);
    doca_buf_dec_refcount(dst_doca_buf, nullptr);
    return false;
  }

  return true;
}

bool TransferEngine::AgentBufferMetadataWrite()
{
  if (agent_metadata_write_outstanding_) {
    return true; // we already have a write outstanding, no need to kick off another one
  }
  if (agent_meta_->producer_idx.value == host_agent_meta_->producer_idx.value) {
    // nothing to do
    return true;
  }

  if (dma_jobs_.empty()) {
    // THIS SHOULD NEVER HAPPEN
    LOG_ERROR("No DMA jobs available, this should never happen");
    return false;
  }

  auto dma_job = dma_jobs_.front();
  dma_jobs_.pop();
  struct doca_buf* src_doca_buf;
  struct doca_buf* dst_doca_buf;
	doca_error_t result;

  const size_t TRANSFER_SIZE = sizeof(uint64_t);

  dma_job->user_event_type = AGENT_BUFFER_METADATA_WRITE;

  host_agent_meta_->producer_idx.value = agent_meta_->producer_idx.value;

	// the src buffer is the memory containing the consumer idx
  auto src_address = (void*)(&host_agent_meta_->producer_idx);
	result = doca_buf_inventory_buf_get_by_addr(doca_state_.buf_inv,
	                                        doca_state_.dst_mmap /*put mmap here*/,
																				  src_address,
																				  TRANSFER_SIZE,
																				  &src_doca_buf);
  if (result != DOCA_SUCCESS) {
		LOG_ERRORF("unable to allocate doca source buffer: {}", doca_error_get_name(result));
		doca_buf_dec_refcount(src_doca_buf, nullptr);
		return false;
	}
	// the dst buffer is the buffer in the agent mmap
  auto dst_address = (void*)(agent_buffer_addr_);
	result = doca_buf_inventory_buf_get_by_addr(doca_state_.buf_inv, 
																				  agent_mmap_ /*put mmap here*/,
																				  dst_address,
																				  TRANSFER_SIZE,
																				  &dst_doca_buf);
  if (result != DOCA_SUCCESS) {
		LOG_ERRORF("unable to allocate doca dest buffer: {}", doca_error_get_name(result));
		doca_buf_dec_refcount(src_doca_buf, nullptr);
		doca_buf_dec_refcount(dst_doca_buf, nullptr);
		return false;
	}

  result = doca_dma_task_memcpy_alloc_init(
    dma_ctx_,
    src_doca_buf,
    dst_doca_buf,
    { .ptr = static_cast<void*>(dma_job)},
    &dma_job->dma_task
  );
  if (result != DOCA_SUCCESS) {
    LOG_ERRORF("unable to allocate DOCA task: {}", doca_error_get_name(result));
    doca_buf_dec_refcount(src_doca_buf, nullptr);
    doca_buf_dec_refcount(dst_doca_buf, nullptr);
    return false;
  }

	dma_job->src_buf = src_doca_buf;
	dma_job->dest_buf = dst_doca_buf;
  dma_job->slot_id = -1; // no slot, this is a metadata write

	result = doca_buf_set_data(src_doca_buf, src_address, TRANSFER_SIZE);
	if (result != DOCA_SUCCESS) {
		LOG_ERRORF("failed to set data for DOCA buffer: {}", doca_error_get_name(result));
		return false;
	}

  // submit the job
  result = doca_task_submit(doca_dma_task_memcpy_as_task(dma_job->dma_task));
  if (result != DOCA_SUCCESS) {
    // free all the stuff
		LOG_ERRORF("error while submitting dma job: {}", doca_error_get_name(result));
		doca_buf_dec_refcount(src_doca_buf, nullptr);
		doca_buf_dec_refcount(dst_doca_buf, nullptr);
    return false;
  }

  agent_metadata_write_outstanding_ = true;
  return true;
}

void TransferEngine::SetRefresher(dpf::HostViewRefresher* refresher)
{
  refresher_ = refresher;
}

void TransferEngine::HandleRefresh(uint32_t slot_idx, bool read)
{
  auto& slot = unified_slots_[slot_idx];
  char* data = slot.req + sizeof(uint32_t) + sizeof(ProtocolHeader);
  size_t size = *reinterpret_cast<uint32_t*>(slot.req) - sizeof(ProtocolHeader);
  if (read) {
    refresher_->HandleReadRefresh(data, size);
  } else {
    refresher_->HandleWriteRefresh(data, size);
  }
}

void TransferEngine::HandleWriteRefresh(uint32_t slot_idx)
{
  HandleRefresh(slot_idx, false);
}

void TransferEngine::HandleReadRefresh(uint32_t slot_idx)
{
  HandleRefresh(slot_idx, true);
}

} // namespace offload
} // namespace dpf
