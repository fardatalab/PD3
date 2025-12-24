#pragma once

#include "types.hpp"

#include "common/types.hpp"

#include "config.h"

#include <atomic>
#include <thread>
#include <mutex>
#if defined(__x86_64__) || defined(_M_X64) || defined(__i386__) || defined(_M_IX86)
  #include <immintrin.h>   // _mm_pause
#endif

namespace dpf {
namespace buffer {

class SpinLock {

public:
  SpinLock() noexcept : flag_(ATOMIC_FLAG_INIT) {}

  SpinLock(const SpinLock&)            = delete;
  SpinLock& operator=(const SpinLock&) = delete;


  void lock() noexcept {
    if (!flag_.test_and_set(std::memory_order_acquire))
      return;

    // Contended: exponential back‑off with CPU pause
    std::uint32_t spins = 1;
    while (true) {
      for (std::uint32_t i = 0; i < spins; ++i) {
        cpu_relax();
        if (!flag_.test_and_set(std::memory_order_acquire))
          return;
      }
      if (spins < kMaxSpins) {
        spins <<= 1;
      } else {
        std::this_thread::yield();
      }
    }
  }

  // try to acquire the lock once; return true on success
  bool try_lock() noexcept {
      return !flag_.test_and_set(std::memory_order_acquire);
  }

  // release the lock
  void unlock() noexcept {
      flag_.clear(std::memory_order_release);
  }

private:
  static inline void cpu_relax() noexcept {
  #if defined(__x86_64__) || defined(_M_X64) || defined(__i386__) || defined(_M_IX86)
    _mm_pause();                // mapped to PAUSE (aka REP NOP) on x86
  #elif defined(__aarch64__) || defined(__arm__)
    asm volatile("yield" ::: "memory");
  #else
    // Fallback: no‑op
  #endif
  }

  static constexpr std::uint32_t kMaxSpins = 64;
  std::atomic_flag flag_;
};

class LocklessAgentBuffer {
  using CacheAlignedU64 = dpf::common::CacheAlignedT<uint64_t>;
  using CacheAlignedAtomic = dpf::common::CacheAlignedAtomic;
public:

  LocklessAgentBuffer() = default;
  ~LocklessAgentBuffer() = default;

  void Initialize();

  bool ProduceBatch(char** data_list, size_t* size_list, size_t num_items, int* num_inserted, size_t contiguous_size = 0);

  bool CheckAndReturn(uint64_t key);

  char* buffer() noexcept {
    return buffer_;
  }

  const uint64_t producer_index() const noexcept {
    return producer_index_.value;
  }

  const uint64_t consumer_index() const noexcept {
    return consumer_index_.value;
  }

  const uint64_t get_buffer_idx(uint64_t idx) const noexcept {
    return idx & (PD3_RING_BUFFER_SIZE - 1);
  }

private:
  
  void ConsumeRun();
  
private:
  
  CacheAlignedAtomic producer_index_;
  CacheAlignedAtomic consumer_index_;

  char buffer_[PD3_RING_BUFFER_SIZE];

};

class SpinlockAgentBuffer {

  using CacheAlignedU64 = dpf::common::CacheAlignedT<uint64_t>;
  // using LockType = std::mutex;
  using LockType = SpinLock;
public:

  SpinlockAgentBuffer() = default;
  ~SpinlockAgentBuffer() = default;

  void Initialize();

  bool ProduceBatch(char** data_list, size_t* size_list, size_t num_items, int* num_inserted, size_t contiguous_size = 0);

  bool CheckAndReturn(uint64_t key);

  char* buffer() noexcept {
    return buffer_;
  }

  const uint64_t producer_index() const noexcept {  
    return producer_index_.value;
  }

  const uint64_t consumer_index() const noexcept {
    return consumer_index_.value;
  }

  const uint64_t get_buffer_idx(uint64_t idx) const noexcept {
    return idx & (PD3_RING_BUFFER_SIZE - 1);
  }

private:
  

  void ConsumeRun();
  
private:

  LockType spin_lock_;
  CacheAlignedU64 producer_index_;
  CacheAlignedU64 consumer_index_;

  char buffer_[PD3_RING_BUFFER_SIZE];

};

} // namespace buffer
} // namespace dpf