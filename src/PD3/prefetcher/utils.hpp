#pragma once

static inline __attribute__((always_inline)) void
CpuPause()
{
#if defined(__i386__) || defined(__x86_64__)
  asm volatile("pause");
#elif defined(__aarch64__)
  asm volatile("yield");
#endif
}
