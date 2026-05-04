#pragma once

#include "types.hpp"

#include "config.h"
#include "literals/memory_literals.hpp"

#include <cstddef>
#include <stdexcept>
#include <atomic>
#include <utility>

// This file contains the definition of a ring buffer class
// that is used to transfer data between the host and the DPU

namespace dpf {
namespace common {
  
class RingBuffer {
public:

  static constexpr size_t METADATA_SIZE = sizeof(CacheAlignedAtomic) * 2 + sizeof(CacheAlignedT<uint64_t>); 
  static constexpr size_t PRODUCER_METADATA_SIZE = sizeof(CacheAlignedAtomic) * 2;
  static constexpr size_t CONSUMER_INDEX_OFFSET = sizeof(CacheAlignedAtomic) * 2;

  RingBuffer() = default;

  void Initialize();

  /// @brief Produce data into the ring buffer. Note that the data is copied
  /// @param data pointer to data to be copied
  /// @param size size of the data to be copied
  /// @return true if success, false if failure
  bool Produce(const char* data, size_t size);

  /// @brief Produce two buffers into the ring buffer as one request. Note that the data is copied
  /// @param data pointer to first buffer to be copied
  /// @param data2 pointer to second buffer to be copied
  /// @param size1 size of the first buffer to be copied
  /// @param size2 size of the second buffer to be copied
  /// @return true if success, false if failure
  bool Produce(const char* data, const char* data2, size_t size1, size_t size2);

  /// @brief Consume data from the ring buffer. Note that the data is copied into the provided buffer
  /// @param data pointer to buffer to copy data into
  /// @param size size of the data to be copied
  /// @return true if success, false if failure
  bool Consume(char* data, size_t& size);

  /// Getters and setters
  uint64_t producer_index() const noexcept {
    return producer_index_.value.load(std::memory_order_relaxed);
  }

  uint64_t progress() const noexcept {
    return progress_.value.load(std::memory_order_relaxed);
  }

  uint64_t consumer_index() const noexcept {
    return consumer_index_.value;
  }

  void producer_index(uint64_t value) noexcept {
    producer_index_.value.store(value, std::memory_order_relaxed);
  }

  void progress(uint64_t value) noexcept {
    progress_.value.store(value, std::memory_order_relaxed);
  }

  void consumer_index(uint64_t value) noexcept {
    consumer_index_.value = value;
  }

private:

  CacheAlignedAtomic producer_index_;
  CacheAlignedAtomic progress_;
  CacheAlignedT<uint64_t> consumer_index_;  // note: this is written by the DPU

  char buffer_[PD3_RING_BUFFER_SIZE];
};

} // namespace common
} // namespace dpf
