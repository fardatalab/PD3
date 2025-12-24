#pragma once

#include "types.hpp"

#include "redy/rdma_client.hpp"

#include "FASTER/status.h"
#include "FASTER/async.h"

#include <cstdint>
#include <string>
#include <thread>
#include <vector>
#include <atomic>

#include <tbb/concurrent_queue.h>

class RedyDeviceSingle {

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
  RedyDeviceSingle(const std::string& name);
  ~RedyDeviceSingle();

  void Reset() {
    return;
  }

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
    return 0;
  }

  void RemoveSegment(uint64_t segment) {
    return;
  }

private:
  
  void WorkerThread(); 
  void IncrementProducer();
  void IncrementConsumer();

private:

  std::atomic_bool running_{true};

  // config items
  uint32_t queue_depth_;
  uint32_t batch_size_;

  size_t segment_size_;

  tbb::concurrent_queue<SingleRequestFrontEnd> request_queue_;

  // worker attributes
  dpf::RdmaClient redy_client_;
  std::vector<RequestBatchFrontEnd> frontend_requests_;
  std::vector<RequestBatchFrontEnd> preflight_requests_;
  size_t producer_ = 0;
  size_t consumer_ = 0; // used for preflight reqs
  size_t preflight_size_ = 0;
  size_t slots_ = 0;

  std::thread redy_worker_;

};