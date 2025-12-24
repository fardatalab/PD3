#pragma once

#include "rigtorp/SPSCQueue.h"
#include "types.hpp"
#include "unified_buffer.hpp"

#include "PD3/prefetcher/types.hpp"
#include "PD3/prefetcher/refresher.hpp"

#include "common/doca_common.hpp"
#include "common/types.hpp"
#include "PD3/literals/memory_literals.hpp"
#include "types/rdma_conn.hpp"

#include <thread>
#include <atomic>
#include <unordered_map>
#include <string>
#include <vector>
#include <queue>

#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>

#include <doca_buf.h>
#include <doca_buf_inventory.h>
#include <doca_ctx.h>
#include <doca_dma.h>
#include <doca_error.h>
#include <doca_log.h>

namespace dpf {
namespace offload {

struct TransferEngineConfig {
  /* rdma server config params */
  std::string server_addr;
  std::string server_port;
  uint64_t max_wr;
  uint64_t remote_segment_size = 1_GiB;
  bool use_rdma; // whether we want to init the rdma conns, useful for benchmarking
  bool testing_mode = false; // whether we want to run in testing mode

  /* dpu config params */
  std::string dpu_pcie_addr;

  /* dma config params */
  std::string export_desc_client_file_path;
  std::string export_desc_agent_file_path;
  std::string buf_client_file_path;
  std::string buf_agent_file_path;
  uint32_t workq_depth = 32;
  bool enable_transfer = true;
  bool enable_agent = false;

  // buffer config params
  uint64_t num_slots = 8;
  uint64_t num_prefetch_slots = 8;
  uint64_t slot_size = 8192 * 4;
};

enum class SlotStatus : uint16_t {
  EMPTY = 0,
  PENDING_DMA_WRITE = 1,
  PENDING_DMA_READ = 2,
  PENDING_RDMA_OP = 3,
  BLOCKED_DUE_TO_BACKPRESSURE = 4,
  COMPLETED = 5,
};

struct UnifiedBufferSlot {
  char* req;
  char* resp;
  SlotStatus status = SlotStatus::EMPTY;
  uint16_t num_reads_remaining = 0; // number of reads remaining in this slot
  uint16_t num_writes_remaining = 0; // number of writes remaining in this slot
};


struct BufferMetadata {
  dpf::common::CacheAlignedT<uint64_t> producer_idx;
  dpf::common::CacheAlignedT<uint64_t> progress_idx;
  dpf::common::CacheAlignedT<uint64_t> consumer_idx;
};

class TransferEngine {
  
  // request buffer idents
  static constexpr uint64_t UNKNOWN = 0;
  static constexpr uint64_t REQ_BUFFER_METADATA_READ = 0x1;
  static constexpr uint64_t REQ_BUFFER_METADATA_WRITE = 0x2;
  static constexpr uint64_t REQ_BUFFER_DATA_READ = 0x3;

  // response buffer idents
  static constexpr uint64_t RESP_BUFFER_METADATA_READ = 0x4;
  static constexpr uint64_t RESP_BUFFER_METADATA_WRITE = 0x5;
  static constexpr uint64_t RESP_BUFFER_DATA_READ = 0x6;
  static constexpr uint64_t RESP_BUFFER_DATA_WRITE = 0x7;

  // agent buffer idents
  static constexpr uint64_t AGENT_BUFFER_METADATA_READ = 0x8;
  static constexpr uint64_t AGENT_BUFFER_METADATA_WRITE = 0x9;
  static constexpr uint64_t AGENT_BUFFER_DATA_READ = 0x10;
  static constexpr uint64_t AGENT_BUFFER_DATA_WRITE = 0x11;

public:

  struct DmaJob {
    struct doca_dma_task_memcpy* dma_task = nullptr;
	  struct doca_buf* src_buf = nullptr;
	  struct doca_buf* dest_buf = nullptr;
    uint64_t user_event_type = 0;
    int slot_id = 0;
  };

  TransferEngine() = default;
  ~TransferEngine();

  bool Initialize(const TransferEngineConfig& config);
  bool InitializeNoDma(const TransferEngineConfig& config);
  void AddPrefetchRequestQueue(PrefetchRequestBatchQueue* prefetch_req_batch_q);
  // ClientRequestBatchQueue* GetRefreshRequestQueue();
  void SetRefresher(dpf::HostViewRefresher* refresher);
  
  /* if to be run on a dedicated thread */
  void Run();
  void RunNoDma();
  void Stop();

  /* Used by DMA callback handlers */
  void HandleDmaCompletion(DmaJob* dma_job);
  void EnqueueDmaJob(DmaJob* dma_job) { dma_jobs_.push(dma_job); }

  bool HandlePrefetchBatch(char* batch_ptr, size_t batch_size);
  bool HandlePrefetchBatchWithSlot(char* batch_ptr, size_t batch_size, uint32_t slot_idx);
  bool HasPrefetchSlots();

  // DEBUG methods 
  UnifiedBufferSlot* GetSlot(uint32_t slot_idx, bool prefetch=true);

  bool PollPrefetchSlot(uint32_t slot_idx);

private:

  /* Initialization functions */
  bool InitializeDma();
  bool SaveConfigsIntoState(const std::string& export_path,
                                         const std::string& buf_path,
                                         size_t* export_desc_len,
                                         char* export_desc,
                                         char** remote_addr,
                                         size_t* remote_addr_len,
                                         char** remote_addr_2,
                                         size_t* remote_addr_len_2);
  bool InitializeRdma();
  bool InitializeSlots();

  /* User defined functions */
  void HandleWriteRefresh(uint32_t slot_idx);
  void HandleReadRefresh(uint32_t slot_idx);
  void HandleRefresh(uint32_t slot_idx, bool read);
  
  /* Work functions */
  void WorkerThread();
  void WorkerThreadNoDma();
  int ProgressDmaEvent();
  void PollRdmaCompletions();
  void PollRdmaCompletionsNoDma();
  void RecycleSlots();

  /* DMA functions */
  bool ClientReqMetadataRead();
  bool ClientReqMetadataWrite();
  bool ClientReqDataRead(uint32_t slot_idx, 
                         uint64_t slot_offset,
                         uint64_t data_offset,
                         size_t size);
  bool ClientRespMetadataRead();
  bool ClientRespMetadataWrite();
  bool ClientRespDataWrite(uint32_t slot_idx,
                           uint64_t offset, 
                           uint64_t data_offset,
                           size_t size, 
                           uint64_t offset_2 = 0, 
                           size_t size_2 = 0);
  
  bool AgentDataWrite(uint32_t slot_idx);
  bool AgentDataWriteWithOffsetAndSize(uint32_t slot_idx,
                                       uint64_t slot_offset,
                                       uint64_t data_offset,
                                       size_t size);
  bool WriteToTransferResponseBuffer(uint32_t slot_idx);
  bool AgentDataRead();
  bool AgentDataReadWithOffsetAndSize(uint64_t offset, size_t size);
  bool AgentBufferMetadataWrite();
  
  /* RDMA functions */
  bool PostSend(uint64_t request_id, 
                struct ibv_sge* sg_list, 
                size_t num_sge, 
                uint32_t flags);
  bool PostRecv(uint64_t request_id, 
                struct ibv_sge* sg_list, 
                size_t num_sge);
  bool PostWrite(uint64_t request_id, 
                 struct ibv_sge* sg_list, 
                 size_t num_sge, 
                 uint64_t remote_addr, 
                 uint32_t remote_rkey, 
                 uint32_t flags);
  bool WaitForCompletion();

private:

  // config
  TransferEngineConfig config_;

  // Prefetch request queue
  std::vector<PrefetchRequestBatchQueue*> prefetch_req_batch_qs_;
  // Host view refresher
  dpf::HostViewRefresher* refresher_ = nullptr;
  
  // DMA stuff
  AppState doca_state_ = {};
  struct doca_dma* dma_ctx_ = nullptr;
  struct doca_mmap* remote_mmap_ = nullptr;
  struct doca_mmap* agent_mmap_ = nullptr;
  std::vector<DmaJob*> all_jobs_;
  std::queue<DmaJob*> dma_jobs_;

  // host side buffer tracking
  char* host_req_addr_;         // addr in host memory
  BufferMetadata* te_req_meta_; // ours
  BufferMetadata* host_req_meta_; // copied from host
  uint64_t req_consumer_idx_ = 0; 
  bool meta_read_outstanding_ = false;
  bool meta_write_outstanding_ = false;
  
  char* host_resp_addr_;         // addr in host memory
  BufferMetadata* te_resp_meta_; // ours
  BufferMetadata* host_resp_meta_; // copied from host
  uint64_t resp_producer_idx_ = 0;
  bool resp_meta_read_outstanding_ = false;
  bool resp_meta_write_outstanding_ = false;

  char* agent_buffer_addr_;      // addr in host memory
  size_t agent_buffer_len_;
  BufferMetadata* agent_meta_;   // ours
  BufferMetadata* host_agent_meta_; // copied to host
  char* agent_buffer_dpu_addr_; // ours
  int num_agent_buffer_reads_outstanding_ = 0;
  bool agent_metadata_write_outstanding_ = false;

  // book keeping for reads
  uint64_t total_num_bytes_written_ = 0;
  uint64_t total_num_bytes_read_ = 0;


  // unified buffer slots
  char* unified_buf_memory_ = nullptr;
  size_t unified_buf_mem_size_ = 0;
  size_t rdma_buf_mem_size_ = 0;
  std::vector<UnifiedBufferSlot> unified_slots_;
  std::vector<UnifiedBufferSlot> prefetch_slots_;

  struct Deque {
    uint64_t head_slot_idx = 0;
    uint64_t tail_slot_idx = 0;
    uint64_t num_slots = 0;
    uint64_t length = 0;
    uint64_t base_slot_idx = 0;
    
    // this is the number of empty slots
    uint64_t get_num_slots() {
      return num_slots;
    }

    uint64_t get_num_full_slots() {
      return length - num_slots;
    }

    uint64_t get_slot_at_idx_from_head(uint64_t idx) {
      return (head_slot_idx + idx) % length;
    }

    uint64_t head() {
      return base_slot_idx + head_slot_idx;
    }

    uint64_t tail() {
      return base_slot_idx + tail_slot_idx;
    }

    bool empty() {
      return num_slots == length;
    }

    bool full() {
      return num_slots == 0;
    }

    void advance_head() {
      head_slot_idx = (head_slot_idx + 1) % length;
      num_slots++;
    }

    void advance_tail() {
      tail_slot_idx = (tail_slot_idx + 1) % length;
      num_slots--;
    }

    void reset() {
      head_slot_idx = 0;
      tail_slot_idx = 0;
      num_slots = 0;
      base_slot_idx = 0;
    }

    uint64_t get_next(uint64_t slot_idx) {
      return (slot_idx + 1) % length;
    }

    bool is_next(uint64_t slot_idx) {
      return (slot_idx + 1) % length == tail_slot_idx;
    }
  };
  Deque transfer_deque_; // used for offload requests
  Deque prefetch_deque_; // used for prefetch requests
  
  // thread, if running on dedicated
  std::atomic_bool running_{false};
  std::thread worker_; 

  // RDMA members
  struct rdma_event_channel* event_channel_ = nullptr;
  struct rdma_cm_id* conn_id_ = nullptr;
  struct ibv_pd* pd_ = nullptr;
  struct ibv_cq* cq_ = nullptr;
  struct ibv_qp* qp_ = nullptr;
  struct ibv_mr* mr_ = nullptr;

  std::vector<RdmaIOContext*> transfer_io_contexts_;
  std::vector<RdmaIOContext*> prefetch_io_contexts_;
  uint64_t send_slots_ = 0;

  struct MemoryRegion {
    uint64_t base_address;
    uint32_t region_rkey;
  };
  std::vector<MemoryRegion> mem_regions_;

  uint64_t total_prefetched_batches_ = 0;
};


} // namespace offload
} // namespace dpf