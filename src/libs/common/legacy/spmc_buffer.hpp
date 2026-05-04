#pragma once

#include "types.hpp"

#include "config.h"

#include <cstdint>
#include <cstddef>
#include <atomic>
#include <utility>
#include <vector>
#include <limits>
#include <algorithm>

namespace dpf {
namespace common {


struct ReservationResult {
  char* buffer_1;
  size_t size_1;
  char* buffer_2;
  size_t size_2;
  uint64_t commit_idx; // the index of the buffer to commit
};

static constexpr uint64_t CONSUMER_STATE_EMPTY = 0;
static constexpr uint64_t CONSUMER_STATE_RESERVED = 1;
static constexpr uint64_t CONSUMER_STATE_WRITTEN = 2;
static constexpr uint64_t CONSUMER_STATE_PROCESSING = 3;
static constexpr uint64_t CONSUMER_STATE_COMPLETED = 4;

struct alignas(8) RecordHeader {
  uint64_t size;
  std::atomic<uint64_t> consumer_state;
  // bool is_written;
  // char padding[7]; // Pad to 16 bytes total
};

struct ConsumerResult {
  char* buffer_1;
  size_t size_1;
  char* buffer_2;
  size_t size_2;
  uint64_t pop_idx;
};

// intended to be constructed in an allocated shared buffer
class SpmcBuffer {

public:
  static constexpr size_t METADATA_SIZE = sizeof(CacheAlignedAtomic) + sizeof(CacheAlignedAtomic) + sizeof(CacheAlignedT<uint64_t>);
  static constexpr size_t CONSUMER_INDEX_OFFSET = sizeof(CacheAlignedAtomic);

public:

  explicit SpmcBuffer();

  void Initialize();
  void Initialize(size_t num_consumers);

  ReservationResult Reserve(size_t size, size_t contiguous_size = 0);

  // True if the producer index was updated
  bool Commit(uint64_t producer_pos);

  /// @brief Get the next WRITTEN record in the buffer
  /// @return the record
  ConsumerResult Front();

  /// @brief Pop the next WRITTEN record in the buffer
  /// @param curr_pos the current position of the consumer index
  void Pop(uint64_t curr_pos);
  void Pop(uint64_t curr_pos, uint64_t thread_id);

  /// @brief Check if there is a contiguous sequence of records that have been completed, and
  /// update the consumer index
  void ConsumeRun();

  /// @brief Consume the buffer up to the producer index
  void ConsumeRunToProducerIndex();

  /// @brief Get the record at the given position
  /// @param pos the position to get the record at
  /// @return the record at the given position
  char* GetRecordAtPos(uint64_t& pos);

  /// @brief Update the consumer index if all threads have progressed
  void UpdateConsumerIndex();

  void StoreConsumerIndex(uint64_t consumer_index, int idx);

  uint64_t GetConsumerIndex(int idx) const {
    return consumer_index_buffer_[idx].value;
  }

  uint64_t producer_index() const noexcept {
    return producer_index_.value.load(std::memory_order_acquire);
  }
  uint64_t reservation_index() const noexcept {
    return reservation_index_.value;
  }
  uint64_t consumer_index() const noexcept {
    return consumer_index_.value.load(std::memory_order_acquire);
  }
  uint64_t consumer_res_index() const noexcept {
    return consumer_res_index_.value.load(std::memory_order_acquire);
  }

  uint64_t producer_buffer_idx() const noexcept {
    return GetBufferIdx(producer_index());
  }
  uint64_t consumer_buffer_idx() const noexcept {
    return GetBufferIdx(consumer_index());
  }
  uint64_t consumer_res_buffer_idx() const noexcept {
    return GetBufferIdx(consumer_res_index());
  }

  size_t metadata_size() const noexcept {
    return sizeof(producer_index_) + sizeof(consumer_index_) + sizeof(reservation_index_);
  }
  size_t consumer_index_offset() const noexcept {
    return offsetof(SpmcBuffer, consumer_index_);
  }
  size_t producer_index_offset() const noexcept {
    return offsetof(SpmcBuffer, producer_index_);
  }

  size_t capacity() const noexcept {
    return PD3_RING_BUFFER_SIZE - metadata_size();
  }

  const char* buffer() const noexcept {
    return &buffer_[0];
  }

  size_t GetBufferIdx(size_t idx) const noexcept {
    return idx & (PD3_RING_BUFFER_SIZE - 1);
  }

  static size_t offset_to_buffer_idx(size_t offset) noexcept {
    return offsetof(SpmcBuffer, buffer_) + offset;
  }

private:
  
  uint64_t GetMinConsumerIndex();
 
private:

  CacheAlignedAtomic producer_index_;
  CacheAlignedAtomic consumer_index_;
  CacheAlignedAtomic consumer_res_index_;
  CacheAlignedT<uint64_t> reservation_index_;

  size_t buffer_mask_;
  std::vector<CacheAlignedT<uint64_t>> consumer_index_buffer_;
  
  char buffer_[PD3_RING_BUFFER_SIZE];
};


class AgentBuffer {
  
  static constexpr uint64_t BUFFER_MASK = PD3_RING_BUFFER_SIZE - 1;

public:

  AgentBuffer() = default;
  ~AgentBuffer() = default;

  void Initialize();

  bool Produce(const char* data, size_t size, size_t contiguous_size = 0);
  bool ProduceBatch(char** data_list, size_t* size_list, size_t num_items, int* num_inserted, size_t contiguous_size = 0);

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

private:

  void FreeConsumedEntries();

private:

  CacheAlignedT<uint64_t> producer_index_;
  CacheAlignedT<uint64_t> consumer_index_;

  char buffer_[PD3_RING_BUFFER_SIZE];

};


} // namespace common
} // namespace dpf