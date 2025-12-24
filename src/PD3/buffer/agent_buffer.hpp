#pragma once

#include "types.hpp"

#include "common/types.hpp"

#include "config.h"

#include <cstdint>
#include <cstddef>
#include <atomic>
#include <utility>
#include <vector>
#include <limits>
#include <algorithm>

namespace dpf {
namespace buffer {

class AgentBuffer {
  
  static constexpr uint64_t BUFFER_MASK = PD3_RING_BUFFER_SIZE - 1;

public:

  using CacheAlignedU64 = dpf::common::CacheAlignedT<uint64_t>;
  static constexpr size_t METADATA_OFFSET = 0;
  static constexpr size_t METADATA_SIZE = sizeof(CacheAlignedU64) + sizeof(CacheAlignedU64) + sizeof(CacheAlignedU64);
  static constexpr size_t DATA_OFFSET = METADATA_SIZE;
  static constexpr size_t DATA_SIZE = PD3_RING_BUFFER_SIZE;

  AgentBuffer() = default;
  ~AgentBuffer() = default;

  void Initialize();

  ReservationResult Reserve(size_t size, size_t contiguous_size = 0);
  bool Commit(uint64_t commit_idx);

  bool Produce(const char* data, size_t size, size_t contiguous_size = 0);
  bool ProduceBatch(char** data_list, size_t* size_list, size_t num_items, int* num_inserted, size_t contiguous_size = 0);
  bool ProduceInterleave(char* dlist_1, size_t sz1, char* dlist_2, size_t sz2, size_t num_items, int* num_inserted);

  char* buffer() noexcept {
    return buffer_;
  }

  const uint64_t producer_index() const noexcept {
    return producer_index_.value;
  }

  const uint64_t consumer_index() const noexcept {
    return consumer_index_.value;
  }
  
  static inline uint64_t GetBufferIdx(uint64_t idx) noexcept {
    return idx & BUFFER_MASK;
  }

  uint64_t capacity() const noexcept {
    return DATA_SIZE;
  }

  static inline size_t offset_to_buffer_idx(size_t offset) noexcept {
    return offsetof(AgentBuffer, buffer_) + offset;
  }

private:

  void FreeConsumedEntries();

private:

  CacheAlignedU64 producer_index_;
  CacheAlignedU64 consumer_index_;
  CacheAlignedU64 reservation_index_;

  char buffer_[PD3_RING_BUFFER_SIZE];

};

} // namespace buffer
} // namespace dpf