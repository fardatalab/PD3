#pragma once

#include "FASTER/auto_ptr.h"
#include "FASTER/async.h"

#include "PD3/transfer_engine/transfer_client.hpp"

#include <string>
#include <cstddef>
#include <unordered_map>
#include <vector>
#include <mutex>
#include <thread>
#include <tbb/concurrent_unordered_map.h>

namespace dpf {

template <typename K, typename V>
using ConcurrentUnorderedMap = tbb::concurrent_unordered_map<K, V>;

class RdmaContext {

private:

 static constexpr uint32_t BATCH_SIZE = 100;
 
 struct RdmaCallbackContext {

  RdmaCallbackContext(FASTER::core::IAsyncContext* context, 
                      FASTER::core::AsyncIOCallback callback,
                      std::byte* buffer,
                      uint32_t length)
    : context{ context }, callback{ callback }, buffer{ buffer }, length{ length } { }

  FASTER::core::IAsyncContext* context; // callback context
  FASTER::core::AsyncIOCallback callback; // callers callback function 
  std::byte* buffer;  // where to transfer the read data into
  uint32_t length;
 };

public:

  RdmaContext() = default;
  ~RdmaContext();

  void Initialize(const std::string& name, uint64_t segment_size);

  FASTER::core::Status RemoveSegmentAsync(uint64_t segment);
  
  FASTER::core::Status ReadAsync(uint64_t source, void* dest, uint32_t length, 
                                 FASTER::core::AsyncIOCallback callback, 
                                 FASTER::core::IAsyncContext& context);

  FASTER::core::Status WriteAsync(const void* source, uint64_t dest, uint32_t length, 
                                  FASTER::core::AsyncIOCallback callback, 
                                  FASTER::core::IAsyncContext& context);

  int ProcessCompletions(int timeout_secs);
  
  uint64_t GetSegmentSize(uint64_t segment) const noexcept {
    return segment_size_;
  }

  void RemoveAllSegments();

private:

  uint64_t segment_size_ = 1024;
  ConcurrentUnorderedMap<uint64_t, uint64_t> segment_size_map_;
  ConcurrentUnorderedMap<uint16_t, RdmaCallbackContext*> pending_read_requests_;
  ConcurrentUnorderedMap<uint16_t, RdmaCallbackContext*> pending_write_requests_;

  dpf::offload::TransferClient offload_client_;
};

} // namespace dpf