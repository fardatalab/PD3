#pragma once

#include "PD3/transfer_engine/types.hpp"
#include "PD3/literals/memory_literals.hpp"

#include "rdma_backend/types.hpp"

#include <cstdint>
#include <string>
#include <set>
#include <vector>

#include <rdma/rdma_cma.h>

namespace dpf {

struct RemoteMemoryRegion {
  uint64_t addr;
  uint32_t rkey;
};

class SyncClient {

struct LocalBuffer {
  ~LocalBuffer();

  struct ibv_mr *buffer_mr;
  void *buffer;
};

public:
  
  SyncClient() = default;
  ~SyncClient();

  void Configure(const std::string& server_port, 
                 const std::string& server_addr);

  int Init(struct ibv_pd** pd);

  int InitializeMemoryRegions(std::vector<RemoteMemoryRegion>& regions);
  void SetMemoryRegions(const std::vector<RemoteMemoryRegion>& regions);

  int ReadSync(uint64_t source, void* dest, uint32_t length);
  int WriteSync(uint64_t dest, const void* source, uint32_t length);

private:
  
  bool AwaitCompletions(std::set<std::pair<ibv_wc_opcode, uint64_t>> expected_wcs);

private:
  
  std::string server_port_;
  std::string server_addr_;

  uint64_t segment_size_ = 1_GiB;
  
  struct rdma_cm_id* conn_id_;
  struct rdma_event_channel* event_channel_;
  struct ibv_cq* cq_;
  struct ibv_qp* qp_;
  struct ibv_pd* pd_;
  LocalBuffer read_buffer_;
  LocalBuffer write_buffer_;

  std::vector<RemoteMemoryRegion> remote_memory_regions_;
};
  
} // namespace dpf
