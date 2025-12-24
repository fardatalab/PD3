#pragma once

#include "rdma_context.hpp"

#include "FASTER/status.h"
#include "FASTER/async.h"

#include <cstdint>
#include <string>

class RdmaDevice {

private:

  class AsyncIoContext : public FASTER::core::IAsyncContext {
  public:

    AsyncIoContext(void* context_, FASTER::core::AsyncIOCallback callback_)
      : context{ context_ },
      callback{ callback_ } {
    }

    AsyncIoContext(AsyncIoContext& other)
      : context{ other.context },
      callback{ other.callback } {}
  
  protected:

    FASTER::core::Status DeepCopy_Internal(FASTER::core::IAsyncContext*& context_copy) final {
      return FASTER::core::IAsyncContext::DeepCopy_Internal(*this, context_copy);
    }
  
  public:
    void* context;
    FASTER::core::AsyncIOCallback callback;
  };

public:
  RdmaDevice(const std::string& name);
  ~RdmaDevice();

  void Reset();

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

  bool TryComplete();

  uint64_t GetFileSize(uint64_t segment);

  void RemoveSegment(uint64_t segment);

  int ProcessCompletions(int timeout_secs);

private:

  dpf::RdmaContext rdma_context_;

};