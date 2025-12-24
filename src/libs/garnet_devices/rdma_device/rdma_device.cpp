#include "rdma_device.hpp"

#include <iostream>

RdmaDevice::RdmaDevice(const std::string& name) {
  std::cout << "RdmaDevice constructor" << std::endl;
}

RdmaDevice::~RdmaDevice() {}

void RdmaDevice::Reset() 
{
  rdma_context_.RemoveAllSegments();
}

FASTER::core::Status RdmaDevice::ReadAsync(uint64_t source, 
                                          void* dest, 
                                          uint32_t length, 
                                          FASTER::core::AsyncIOCallback callback, 
                                          void* context) 
{
  AsyncIoContext ctx{context, callback};
  auto callback_ = [](FASTER::core::IAsyncContext* ctxt, FASTER::core::Status result, size_t bytes_transferred) {
    FASTER::core::CallbackContext<AsyncIoContext> context{ ctxt };
    context->callback((FASTER::core::IAsyncContext*)context->context, result, bytes_transferred);
  };
  return rdma_context_.ReadAsync(source, dest, length, callback_, ctx);
}

FASTER::core::Status RdmaDevice::WriteAsync(const void* source, 
                                          uint64_t dest, 
                                          uint32_t length, 
                                          FASTER::core::AsyncIOCallback callback, 
                                          void* context) 
{
  AsyncIoContext ctx{context, callback};
  auto callback_ = [](FASTER::core::IAsyncContext* ctxt, FASTER::core::Status result, size_t bytes_transferred) {
    FASTER::core::CallbackContext<AsyncIoContext> context{ ctxt };
    context->callback((FASTER::core::IAsyncContext*)context->context, result, bytes_transferred);
  };
  return rdma_context_.WriteAsync(source, dest, length, callback_, ctx);
}

bool RdmaDevice::TryComplete() {
  return true;
}

uint64_t RdmaDevice::GetFileSize(uint64_t segment) {
  return rdma_context_.GetSegmentSize(segment);
}

void RdmaDevice::RemoveSegment(uint64_t segment) {
  rdma_context_.RemoveSegmentAsync(segment);
}

int RdmaDevice::ProcessCompletions(int timeout_secs) {
  return rdma_context_.ProcessCompletions(timeout_secs);
}