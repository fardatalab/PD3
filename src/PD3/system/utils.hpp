#pragma once

#include <cstdint>
#include <cerrno>
#include <cstdlib>
#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

namespace dpf {

namespace detail {

inline uint64_t PageRoundUp(uint64_t x)
{
  auto pagesize = static_cast<uint64_t>(getpagesize());
  return (x + pagesize - 1) & ~(pagesize - 1);
}

inline uint64_t PageRoundDown(uint64_t x)
{
  auto pagesize = static_cast<uint64_t>(getpagesize());
  return x & ~(pagesize - 1);
}

static inline __attribute__((always_inline)) void
CpuPause()
{
#if defined(__i386__) || defined(__x86_64__)
  asm volatile("pause");
#elif defined(__aarch64__)
  asm volatile("yield");
#endif
}

static inline __attribute__((always_inline)) bool
IsPowerOfTwo(uint64_t x)
{
  return x && !(x & (x - 1));
}

}

}