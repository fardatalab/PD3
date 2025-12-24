#include "rdma_sync_device.hpp"

#include "PD3/system/logger.hpp"

#include <iostream>

RdmaDeviceSync::RdmaDeviceSync(const std::string& name) {
  std::cout << "RdmaDeviceSync constructor" << std::endl;
  for (int i = 0; i < MAX_NUM_THREADS; ++i) {
    sync_clients_.push_back(new dpf::SyncClient());
    sync_clients_.back()->Configure("10.10.1.100", "12345");
    sync_clients_.back()->Init(&pd_);
  }
  // initialize memory regions
  if (memory_regions_.empty()) {
    sync_clients_[0]->InitializeMemoryRegions(memory_regions_);
  }
  for (int i = 0; i < MAX_NUM_THREADS; ++i) {
    sync_clients_[i]->SetMemoryRegions(memory_regions_);
  }
  std::cout << "RdmaDeviceSync constructor done" << std::endl;
}

RdmaDeviceSync::~RdmaDeviceSync() {
  for (int i = 0; i < MAX_NUM_THREADS; ++i) {
    delete sync_clients_[i];
  }
  sync_clients_.clear();
}

void RdmaDeviceSync::Reset() 
{
  for (int i = 0; i < MAX_NUM_THREADS; ++i) {
    delete sync_clients_[i];
  }
  sync_clients_.clear();
}

FASTER::core::Status RdmaDeviceSync::ReadAsync(uint64_t source, 
                                          void* dest, 
                                          uint32_t length, 
                                          FASTER::core::AsyncIOCallback callback, 
                                          void* context) 
{
  // latency_writer_.ContextStart(); // comment during tput or lat testing 
  FASTER::core::Status status = FASTER::core::Status::Ok;

  FASTER::core::Thread::acquire_id();

  // perform segment conversion
  uint64_t segment = source / segment_size_;
  auto new_source = source % segment_size_;

  // kick off an RDMA read on this thread
  auto thread_id = FASTER::core::Thread::id();
  auto sync_client = sync_clients_[thread_id];

  // MEMORY ACCESS LATENCY
  // latency_writer_.MemoryAccessStart(); // comment during tput or lat testing 
  if (sync_client->ReadSync(source, dest, length)) {
    return FASTER::core::Status::IOError;
  }
  // latency_writer_.MemoryAccessEnd(); // comment during tput or lat testing 

  FASTER::core::Thread::release_id();

  // CONSUME LATENCY
  // latency_writer_.ConsumeStart(); // comment during tput or lat testing 
  callback((FASTER::core::IAsyncContext*)context, status, length);
  // latency_writer_.ConsumeEnd(); // comment during tput or lat testing 

  // latency_writer_.ContextEnd(); // comment during tput or lat testing 
  // latency_writer_.Write(); // comment during tput or lat testing 

  return FASTER::core::Status::Ok;
}

FASTER::core::Status RdmaDeviceSync::WriteAsync(const void* source, 
                                          uint64_t dest, 
                                          uint32_t length, 
                                          FASTER::core::AsyncIOCallback callback, 
                                          void* context) 
{
  FASTER::core::Status status = FASTER::core::Status::Ok;

  FASTER::core::Thread::acquire_id();

  auto thread_id = FASTER::core::Thread::id();
  auto sync_client = sync_clients_[thread_id];

  dpf::LOG_INFOF("WriteAsync: thread_id: {}, dest: {}, length: {}", thread_id, dest, length);
  

  if (sync_client->WriteSync(dest, source, length)) {
    return FASTER::core::Status::IOError;
  }

  FASTER::core::Thread::release_id();

  callback((FASTER::core::IAsyncContext*)context, status, length);

  return FASTER::core::Status::Ok;
}

// bool RdmaDeviceSync::TryComplete() {
//   return true;
// }

uint64_t RdmaDeviceSync::GetFileSize(uint64_t segment) {
  return segment_size_;
}

void RdmaDeviceSync::RemoveSegment(uint64_t segment) {
  return;
}

// int RdmaDeviceSync::ProcessCompletions(int timeout_secs) {
//   return rdma_context_sync_.ProcessCompletions(timeout_secs);
// }