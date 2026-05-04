#pragma once

#include <cstddef>
#include <sys/types.h>  
#include <string_view>
#include <string>

/**
 * Shared memory allocator : creates a segment of shm
 */

namespace dpf {

struct ShmAllocatorConfigOptions {
  size_t capacity = 0;
  mode_t perms = 0600;
  bool mirrored = false;
  std::string name;
  bool lock = false;
  bool sequential = false;
  bool random = false;
};

class ShmAllocator {

public:

  ShmAllocator() = default;
  ~ShmAllocator();

  explicit ShmAllocator(const ShmAllocatorConfigOptions& options);

  ShmAllocator(ShmAllocator&&);
  ShmAllocator& operator=(ShmAllocator&&);

  /// @brief Expect page references in a sequential order (aggressive read ahead)
  void Sequential();

  /// @brief Expect page references in a random order (no read ahead)
  void Random();

  /// @brief Lock the virtual address space into RAM
  void Lock();

  void* addr() const noexcept { return addr_; }
  size_t capacity() const noexcept { return capacity_; }
  mode_t perms() const noexcept { return perms_; }
  bool mirrored() const noexcept { return mirrored_; }
  const std::string& name() const noexcept { return name_; }

private:
  // private members here
  size_t MappedCapacity() const noexcept;
  void OpenSegment();

private:
  // private members here
  void* addr_ = nullptr;
  size_t capacity_ = 0;
  mode_t perms_ = 0600;
  bool mirrored_ = false;
  std::string name_;

};

struct DeviceShmAllocatorConfigOptions {
  size_t capacity = 0;
  mode_t perms = 0600;
  bool mirrored = false;
  std::string name;
  // TODO: add device specific options here (for doca_mmap)
};

/// @brief Creates a segment of shared memory that can be zero-copy mapped to DPU devices
class DeviceShmAllocator {

public:

private:


};

}