#pragma once

#include "sync_client.hpp"

#include "FASTER/status.h"
#include "FASTER/async.h"
#include "PD3/literals/memory_literals.hpp"

#include <cstdint>
#include <string>

using namespace dpf::literals;

class RdmaDeviceSync {
  
  static constexpr int MAX_NUM_THREADS = 16;

public:
  RdmaDeviceSync(const std::string& name);
  ~RdmaDeviceSync();

  void Reset();

  uint32_t sector_size() const {
    return 512;
  }

  FASTER::core::Status ReadAsync(uint64_t source, 
                                 void* dest, 
                                 uint32_t length,
                                 FASTER::core::AsyncIOCallback callback, 
                                 void* context);

  FASTER::core::Status WriteAsync(const void* source, uint64_t dest, 
                                  uint32_t length, 
                                  FASTER::core::AsyncIOCallback callback, 
                                  void* context);

  uint64_t GetFileSize(uint64_t segment);

  void RemoveSegment(uint64_t segment);

private:

  std::vector<dpf::SyncClient*> sync_clients_;
  std::vector<dpf::RemoteMemoryRegion> memory_regions_;
  struct ibv_pd* pd_;

  // dpf::LatencyWriter latency_writer_{"/data/ssankhe/fig21/device_sync_latency.jsonl"}; // comment during tput or lat testing 
  
  const uint64_t segment_size_ = 1_GiB;
};