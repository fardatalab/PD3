#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

#include <sys/types.h>

/**
 * DMA Buffer class: this buffer is managed across the host and DPU 
 * 
 */

namespace dpf {

/// @brief Host-side DMA buffer class
class DmaBuffer {

public:
  DmaBuffer() = default;
  ~DmaBuffer();

  /// TODO: add options here
  explicit DmaBuffer(const size_t capacity);

  DmaBuffer(DmaBuffer&&) noexcept;
  DmaBuffer& operator=(DmaBuffer&&) noexcept;

  DmaBuffer(const DmaBuffer&) = delete;
  DmaBuffer& operator=(const DmaBuffer&) = delete;

  void* addr() const noexcept { return addr_;}
  size_t capacity() const noexcept { return capacity_;}
  mode_t perms() const noexcept { return perms_;}
  const std::string& name() const noexcept { return name_;}

private:
  size_t capacity_;

  void* addr_;
  mode_t perms_;

  std::string name_;
};

/// @brief DPU-side DMA buffer class
class DmaBufferDPU {

public:
  DmaBufferDPU() = default;
  ~DmaBufferDPU();

  explicit DmaBufferDPU(const size_t capacity);

  DmaBufferDPU(DmaBufferDPU&&) noexcept;
  DmaBufferDPU& operator=(DmaBufferDPU&&) noexcept;

  DmaBufferDPU(const DmaBufferDPU&) = delete;
  DmaBufferDPU& operator=(const DmaBufferDPU&) = delete;

  void Allocate();
  void Attach();

  // the intent of this is to expose a buffer that is always in sync with the host buffer
  void Sync();

  void* addr() const noexcept { return addr_;}
  size_t capacity() const noexcept { return capacity_;}
  mode_t perms() const noexcept { return perms_;}
  const std::string& name() const noexcept { return name_;}

private:
  size_t capacity_;
  
  void* addr_;
  mode_t perms_;

  std::string name_;

};

}