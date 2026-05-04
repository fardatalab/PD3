#pragma once

#include <cstdint>
#include <cstddef>

// https://wiki.ubuntu.com/UnitsPolicy
namespace dpf {
inline namespace literals {
  inline namespace memory_literals {
    // base 2 (RAM)
    constexpr uint64_t operator""_B(unsigned long long value) { return value; }

    constexpr uint64_t operator""_KiB(unsigned long long value) { return 1024_B * value; }

    constexpr uint64_t operator""_MiB(unsigned long long value) { return 1024_KiB * value; }

    constexpr uint64_t operator""_GiB(unsigned long long value) { return 1024_MiB * value; }

    constexpr uint64_t operator""_TiB(unsigned long long value) { return 1024_GiB * value; }

    constexpr uint64_t operator""_PiB(unsigned long long value) { return 1024_TiB * value; }

    // base 10, (disk/file sizes)
    constexpr uint64_t operator""_kB(unsigned long long value) { return 1000_B * value; }

    constexpr uint64_t operator""_MB(unsigned long long value) { return 1000_kB * value; }

    constexpr uint64_t operator""_GB(unsigned long long value) { return 1000_MB * value; }

    constexpr uint64_t operator""_TB(unsigned long long value) { return 1000_GB * value; }

    constexpr uint64_t operator""_PB(unsigned long long value) { return 1000_TB * value; }
  } // namespace memory_literals
} // namespace literals
} // namespace aot
