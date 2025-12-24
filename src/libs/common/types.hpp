#pragma once

#include "PD3/literals/memory_literals.hpp"

#include <cstddef>
#include <atomic>

namespace dpf {
namespace common {

#if defined(__cpp_lib_hardware_interference_size)
static constexpr std::size_t CACHE_LINE_SIZE = std::hardware_destructive_interference_size;
#else
// Fallback value
static constexpr std::size_t CACHE_LINE_SIZE = 64;
#endif

struct alignas(CACHE_LINE_SIZE) CacheAlignedAtomic {
  std::atomic<uint64_t> value;
  char pad[CACHE_LINE_SIZE - sizeof(std::atomic<uint64_t>)];
};

template <typename T>
struct alignas(CACHE_LINE_SIZE) CacheAlignedT {
  static_assert(sizeof(T) < CACHE_LINE_SIZE, "Type T must be smaller than CACHE_LINE_SIZE");
  T value;
  char pad[CACHE_LINE_SIZE - sizeof(T)];
};

// static constexpr size_t PD3_RING_BUFFER_SIZE = 1_MiB;
// static constexpr size_t PD3_MAX_PRODUCER_ADVANCEMENT = 128_KiB;
struct ConsumeResult {
  char* buffer_1 = nullptr;
  char* buffer_2 = nullptr;
  size_t size_1 = 0;
  size_t size_2 = 0;
};

} // namespace common
} // namespace dpf