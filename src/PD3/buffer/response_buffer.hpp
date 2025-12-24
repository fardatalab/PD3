/// This is a port of the DDS single producer, multiple consumer ring buffer (progressive)
#pragma once

#include "types.hpp"

#include "config.h"
#include "common/types.hpp"
#include "PD3/literals/memory_literals.hpp"

#include <cstddef>
#include <stdexcept>
#include <atomic>
#include <utility>
#include <vector>


namespace dpf {
namespace buffer {

class ResponseBuffer {

  using CacheAlignedU64 = dpf::common::CacheAlignedT<uint64_t>;
  using CacheAlignedAtomic = dpf::common::CacheAlignedAtomic;

public:

  static constexpr size_t METADATA_OFFSET = 0;
  static constexpr size_t METADATA_SIZE = sizeof(CacheAlignedU64) * 2 + sizeof(CacheAlignedAtomic) * 2;
  static constexpr size_t CONSUMER_METADATA_OFFSET = sizeof(CacheAlignedU64) * 2;
  static constexpr size_t CONSUMER_METADATA_SIZE = sizeof(CacheAlignedAtomic) * 2;
  static constexpr size_t DATA_OFFSET = METADATA_SIZE;
  static constexpr size_t DATA_SIZE = PD3_RING_BUFFER_SIZE;

  ResponseBuffer() = default;

  void Initialize();

  ReservationResult Reserve(size_t size, size_t contiguous_size = 0);
  bool Produce(const char* data, size_t size, size_t contiguous_size = 0);
  bool ProduceTwo(const char* data1, size_t size1, const char* data2, size_t size2);
  void Commit(uint64_t size);

  bool Consume(char* data, size_t& size);
  bool ConsumeNoCopy(ConsumeResult& result);


  char* buffer() { return &buffer_[0]; }
  uint64_t producer_index() { return producer_index_.value; }
  uint64_t consumer_index() { return consumer_index_.value.load(std::memory_order_relaxed); }
  uint64_t capacity() { return DATA_SIZE; }

  void StoreConsumerIndex(uint64_t index, uint32_t thread_id);

  static inline size_t offset_to_buffer_idx(size_t offset) noexcept {
    return offsetof(ResponseBuffer, buffer_) + offset;
  }

private:

  CacheAlignedU64 producer_index_;
  CacheAlignedU64 reservation_index_;
  CacheAlignedAtomic progress_;
  CacheAlignedAtomic consumer_index_;

  char buffer_[PD3_RING_BUFFER_SIZE];

};

} // namespace buffer
} // namespace dpf