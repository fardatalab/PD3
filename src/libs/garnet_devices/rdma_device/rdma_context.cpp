#include "rdma_context.hpp"

#include "PD3/system/logger.hpp"

// #include "offload_engine/types.hpp"
#include <iostream>
#include <cassert>
#include <chrono>
#include <stdexcept>
#include <cstring>
#include <cstddef>

namespace dpf {

using OffloadRequest = dpf::offload::OffloadRequest;
using OffloadRequestOp = dpf::offload::OffloadRequestOp;
using OffloadRequestStatus = dpf::offload::OffloadRequestStatus;

void RdmaContext::Initialize(const std::string& name, uint64_t segment_size)
{
  segment_size_ = segment_size;
  offload::TransferClientConfig config;
  config.pcie_address = "81:00.0";
  config.mmap_export_path = name + "/export_desc.txt";
  config.buf_details_path = name + "/buf_details.txt";
  offload_client_.Initialize(config);
}

FASTER::core::Status RdmaContext::RemoveSegmentAsync(uint64_t segment)
{
  return FASTER::core::Status::Ok;
}

FASTER::core::Status RdmaContext::ReadAsync(uint64_t source, void* dest, uint32_t length, 
                                            FASTER::core::AsyncIOCallback callback, 
                                            FASTER::core::IAsyncContext& context)
{
  FASTER::core::Thread::acquire_id();
  uint64_t segment = source / segment_size_;
  assert(source % segment_size_ + length <= segment_size_);

  auto new_source = source % segment_size_;

  auto io_context = FASTER::core::alloc_context<RdmaCallbackContext>(sizeof(RdmaCallbackContext));
  if (!io_context.get()) {
    return FASTER::core::Status::OutOfMemory;
  }

  FASTER::core::IAsyncContext* caller_context_copy;
  context.DeepCopy(caller_context_copy);
  
  new(io_context.get()) RdmaCallbackContext(caller_context_copy, callback, (std::byte*)dest, length);
  auto req = offload::TransferRequest{
    .address = source,
    .bytes = length,
    .is_read = true,
  };
  // get the request ID from the offload client
  uint16_t req_id = offload_client_.SubmitRequest(req, nullptr, 0);
  pending_read_requests_.insert(std::make_pair(req_id, io_context.get()));

  io_context.release();
  FASTER::core::Thread::release_id();
  return FASTER::core::Status::Ok;
}

FASTER::core::Status RdmaContext::WriteAsync(const void* source, uint64_t dest, uint32_t length, 
                                            FASTER::core::AsyncIOCallback callback, 
                                            FASTER::core::IAsyncContext& context)
{
  FASTER::core::Thread::acquire_id();
  auto segment = dest / segment_size_;
  auto offset = dest % segment_size_;
  assert(offset + length <= segment_size_);

  auto new_dest = dest % segment_size_;

  auto io_context = FASTER::core::alloc_context<RdmaCallbackContext>(sizeof(RdmaCallbackContext));
  if (!io_context.get()) {
    return FASTER::core::Status::OutOfMemory;
  }

  FASTER::core::IAsyncContext* caller_context_copy;
  context.DeepCopy(caller_context_copy);

  new(io_context.get()) RdmaCallbackContext(caller_context_copy, callback, (std::byte*)source, length);

  // submit the request and get the request ID
  auto req = offload::TransferRequest{
    .address = dest,
    .bytes = length,
    .is_read = false,
  };
  uint16_t req_id = offload_client_.SubmitRequest(req, (const char*)source, length);
  pending_write_requests_.insert(std::make_pair(req_id, io_context.get()));
  io_context.release();
  FASTER::core::Thread::release_id();
  return FASTER::core::Status::Ok;
}

int RdmaContext::ProcessCompletions(int timeout_secs)
{
  auto start_time = std::chrono::high_resolution_clock::now();

  while (true) {
    auto current_time = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::seconds>(current_time - start_time);
    if (duration.count() > timeout_secs) {
      return -1;
    }

    do {
      // we drain the queue while we have completions to read
      // once we don't (i.e. the call returns nullptr, we break)
      auto completion = offload_client_.PollCompletions();
      if (completion.response == nullptr) {
        break;
      }
      auto req_id = completion.response->req_id;
      if (pending_read_requests_.find(req_id) == pending_read_requests_.end()) {
        LOG_ERRORF("Got a completion for a request we don't have: {}", req_id);
        break;
      }
      auto req = pending_read_requests_[req_id];
      if (completion.response->is_read) {
        // read request
        std::memcpy(req->buffer, completion.data, completion.response->bytes);
        req->callback(req->context, FASTER::core::Status::Ok, completion.response->bytes);
      } else {
        // write request
        req->callback(req->context, FASTER::core::Status::Ok, completion.response->bytes);
      }
      
    } while (true);
  }
}

void RdmaContext::RemoveAllSegments()
{
  return;
}

}