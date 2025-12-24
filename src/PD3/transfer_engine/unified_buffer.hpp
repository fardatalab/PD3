/**
 * Unified buffers are accessible both by DMA hardware and RNIC
 * They expose a set number of slots for batched operations
 */
#pragma once

#include "types.hpp"

#include <vector>
#include <cstddef>
#include <cstdint>
#include <cstdlib>

namespace dpf {
namespace offload
{

class UnifiedBuffer 
{

public:
  
  UnifiedBuffer() = default;

  ~UnifiedBuffer()
  {
    if (raw_memory_ != nullptr) {
      free(raw_memory_);
    }
  }

  void Initialize(size_t num_slots, size_t batch_size)
  {
    size_t per_req_slot_size = sizeof(ReqSlotHeader) + batch_size;
    size_t per_resp_slot_size = sizeof(RespSlotHeader) + batch_size;
    raw_memory_size_= (per_req_slot_size * num_slots) + (per_resp_slot_size * num_slots);
    raw_memory_ = (char*)malloc(raw_memory_size_);
    if (!raw_memory_) {
      return;
    }

    for (int i = 0; i < num_slots; ++i) {
      auto slot_offset = per_req_slot_size * i;
      auto slot_offset_resp = per_resp_slot_size * i + (per_req_slot_size * num_slots);
      slots_.push_back(reinterpret_cast<ReqSlotHeader*>(raw_memory_ + slot_offset));
      resp_slots_.push_back(reinterpret_cast<RespSlotHeader*>(raw_memory_ + slot_offset_resp));
    }
  }

  char* raw_memory() const noexcept {
    return raw_memory_;
  }

  size_t raw_memory_size() const noexcept {
    return raw_memory_size_;
  }

private:
  char* raw_memory_ = nullptr;
  size_t raw_memory_size_ = 0;
  std::vector<ReqSlotHeader*> slots_;
  std::vector<RespSlotHeader*> resp_slots_;
};
  
} // namespace offload
} // namespace dpf