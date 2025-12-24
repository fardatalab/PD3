#pragma once

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
#endif