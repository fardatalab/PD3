#pragma once

#include "PD3/literals/memory_literals.hpp"

#include <cstdint>
#include <cstddef>
#include <string>

namespace dpf {

struct ShmAttacherConfigOptions {
  std::string name;
  size_t max_capacity = 0;
  bool lock = false;
  bool sequential = false;
  bool random = false;
};

class ShmAttacher
{
public:

  ShmAttacher() noexcept = default;
  explicit ShmAttacher(const ShmAttacherConfigOptions& options);
  ~ShmAttacher();

  ShmAttacher(ShmAttacher&&) noexcept;
  ShmAttacher& operator=(ShmAttacher&&) noexcept;

  /// @brief Grow the attached memory segment
  void Grow(size_t bytes);
  
  /// @brief Lock the virtual region space into RAM
  void Lock();

  /// @brief Advise that expect page references in sequential order (aggressive read-ahead).
  void Sequential();

  /// @brief Expect page references in random order.
  void Random();

  // accessors
  void* addr() const noexcept { return addr_; }
  size_t capacity() const noexcept { return capacity_; }
  size_t max_capacity() const noexcept { return max_capacity_; }
  bool mirrored() const noexcept { return mirrored_; }

private:

  void* addr_ = nullptr;
  size_t capacity_ = 0;
  size_t max_capacity_ = 8_GiB;

  int32_t shm_fd_ = -1;

  bool mirrored_ = false;
};


/// @brief Attaches to a shared memory segment on the DPU device
class DeviceShmAttacher {

public:

};

}

