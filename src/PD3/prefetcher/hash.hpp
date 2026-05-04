#pragma once

#include <cstdint>
#include <cstddef>

#ifdef __aarch64__
#include <arm_acle.h>

template <typename KeyT>
struct CRCHash;

template <>
struct CRCHash<uint64_t> {
  size_t operator()(uint64_t value) const noexcept {
    uint32_t crc = __crc32cd(0, value);
    return (static_cast<uint64_t>(crc) << 32) | crc;
  }
};

template <typename KeyT>
struct FastHash {
  uint64_t seed;
};

template <>
struct FastHash<uint64_t> {
  uint64_t seed = 0x9e3779b97f4a7c15ull;

  static inline uint64_t mix(uint64_t x) noexcept {
    x ^= x >> 32;
    x *= 0xd6e8feb86659fd93ull;
    x ^= x >> 32;
    x *= 0xd6e8feb86659fd93ull;
    x ^= x >> 32;
    return x;
  }

  size_t operator()(uint64_t value) const noexcept {
    return static_cast<size_t>(mix(value + seed));
  }
};

#endif