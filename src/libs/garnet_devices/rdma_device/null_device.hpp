#pragma once

#include "rdma_context.hpp"

#include "FASTER/status.h"
#include "FASTER/async.h"

#include <cstdint>
#include <string>

class NullDevice {

public:
  NullDevice();
  ~NullDevice();

  void Reset() {}

  uint32_t sector_size() const {
    return 512;
  }

  FASTER::core::Status ReadAsync(uint64_t source, 
                                 void* dest, 
                                 uint32_t length,
                                 FASTER::core::AsyncIOCallback callback, 
                                 void* context);

  FASTER::core::Status WriteAsync(const void* source, uint64_t dest, 
                                  uint32_t length, 
                                  FASTER::core::AsyncIOCallback callback, 
                                  void* context);

  uint64_t GetFileSize(uint64_t segment) {
    return 1024;
  }

  void RemoveSegment(uint64_t segment) {}

};