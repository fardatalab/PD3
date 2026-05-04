#pragma once

#include <cstddef>
#include <cstdint>
#include <atomic>

namespace dpf {
namespace buffer {

static constexpr uint64_t CONSUMER_STATE_EMPTY = 0;
static constexpr uint64_t CONSUMER_STATE_RESERVED = 1;
static constexpr uint64_t CONSUMER_STATE_WRITTEN = 2;
static constexpr uint64_t CONSUMER_STATE_PROCESSING = 3;
static constexpr uint64_t CONSUMER_STATE_COMPLETED = 4;

struct alignas(8) RecordHeader {
  uint64_t size;
  std::atomic<uint64_t> consumer_state;
};

struct ReservationResult {
  char* buffer_1 = nullptr;
  size_t size_1 = 0;
  char* buffer_2 = nullptr;
  size_t size_2 = 0;
  uint64_t commit_idx = 0; // the index of the buffer to commit
};

struct ConsumeResult {
  char* buffer_1 = nullptr;
  char* buffer_2 = nullptr;
  size_t size_1 = 0;
  size_t size_2 = 0;
};

} // namespace buffer
} // namespace dpf