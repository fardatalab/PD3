#pragma once

#include "types.hpp"

#include "config.h"
#include "literals/memory_literals.hpp"

#include <cstddef>
#include <stdexcept>
#include <atomic>
#include <utility>
#include <vector>

namespace dpf {
namespace common {

class ResponseBuffer {

public:

  ResponseBuffer() = default;

  void Initialize();
  void Initialize(size_t num_consumers);

  bool Produce(const char* data, size_t size);

  bool Consume(char* data, size_t& size);
  bool ConsumeNoCopy(ConsumeResult& result);

  void UndoConsume(uint64_t size);

  void Commit(uint64_t size);

  char* buffer() { return &buffer_[0]; }
  uint64_t producer_index() { return producer_index_.value; }
  uint64_t consumer_index() { return consumer_index_.value.load(std::memory_order_relaxed); }

  void StoreConsumerIndex(uint64_t index, uint32_t thread_id);

private:

  CacheAlignedT<uint64_t> producer_index_;
  CacheAlignedAtomic progress_;
  CacheAlignedAtomic consumer_index_;

  std::vector<uint64_t> consumer_index_history_;

  char buffer_[PD3_RING_BUFFER_SIZE];

};

} // namespace common
} // namespace dpf
