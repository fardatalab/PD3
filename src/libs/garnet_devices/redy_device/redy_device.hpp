#pragma once

#include "rigtorp/SPSCQueue.h"
#include "types.hpp"

#include "redy/rdma_client.hpp"

#include "FASTER/status.h"
#include "FASTER/async.h"

#include <cstdint>
#include <string>
#include <thread>
#include <vector>
#include <atomic>

class RequestBatchRing
{
private:
    // the size of this ring
    int ring_capacity;

    // the cursor for application threads
    int app_cursor;

    // the cursor for cache client threads
    int cache_cursor;

    // the actual batches
    std::vector<RequestBatchFrontEnd*> batches;

public:
    RequestBatchRing(int ring_capacity_, int batch_capacity_) {
        ring_capacity = ring_capacity_;
        app_cursor = 0;
        cache_cursor = 0;

        for (int i = 0; i != ring_capacity; i++) {
            RequestBatchFrontEnd *batch = new RequestBatchFrontEnd(batch_capacity_);
            batches.push_back(batch);
        }
    }

    ~RequestBatchRing() {
        for (int i = 0; i != ring_capacity; i++) {
            delete batches[i];
        }
        batches.clear();
    }

    // getters
    int GetRingCapacity() {
        return ring_capacity;
    }

    // increment the cursors
    void IncrementCacheCursor();
    void IncrementAppCursor();

    // whether we can move app cursor
    bool AppCursorIncrementable();

    // get a batch on the ring for cache, the consumer
    RequestBatchFrontEnd* GetCacheBatch();

    // get a batch on the ring for app, the producer
    RequestBatchFrontEnd* GetAppBatch();

    // resize this ring
    void Resize(int ring_capacity_, int batch_capacity_);
};


template <typename T>
class ObjectPool {

public:
  ObjectPool() = default;

  ObjectPool(const ObjectPool&) = delete;
  ObjectPool& operator=(const ObjectPool&) = delete;

  ~ObjectPool() {
    for (auto& obj : objects_) {
      delete obj;
    }
  }

  void Init(size_t max_size) {
    max_size_ = max_size;
    free_objects_.reserve(max_size_ + 1);
    objects_.reserve(max_size_);
    for (size_t i = 0; i < max_size_; ++i) {
      auto obj = new T();
      free_objects_.push_back(obj);
      objects_.push_back(obj);
    }
    free_objects_.push_back(nullptr);
    head_.store(0);
    tail_.store(max_size_);
  }

  T* Allocate() {
    const auto current_head = head_.load(std::memory_order_relaxed);
    if (current_head == tail_.load(std::memory_order_acquire)) {
      return nullptr;
    }
    T* ptr = free_objects_[current_head];
    head_.store((current_head + 1) % free_objects_.size(), std::memory_order_release);

    return ptr;
  }

  void Deallocate(T* ptr) {
    const auto current_tail = tail_.load(std::memory_order_relaxed);
    const auto next_tail = (current_tail + 1) % free_objects_.size();
    if (next_tail == head_.load(std::memory_order_acquire)) {
      // double deallocate
      std::cerr << "Double deallocate" << std::endl;
      return;
    }
    free_objects_[current_tail] = ptr;
    tail_.store(next_tail, std::memory_order_release);
  }

private:
  size_t max_size_;

  std::vector<T*> free_objects_;
  std::vector<T*> objects_;

  alignas(64) std::atomic_uint64_t head_;
  alignas(64) std::atomic_uint64_t tail_;
};

struct alignas(64) RedyFrontend {
  dpf::RdmaClient redy_client;
  std::thread worker;
  // frontend batches, mirroring the backend batches for callbacks etc.
  std::vector<RequestBatchFrontEnd> inflight_requests;
  RequestBatchRing* preflight_requests;
  alignas(64) rigtorp::SPSCQueue<SingleRequestFrontEnd*>* pending_requests;
  alignas(64) ObjectPool<SingleRequestFrontEnd> request_pool;
};

class RedyDevice {

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
  RedyDevice(const std::string& name);
  ~RedyDevice();

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

  uint64_t GetFileSize(uint64_t segment);

  void RemoveSegment(uint64_t segment);

private:

  void WorkerThread(int thread_id);

private:

  std::atomic_bool running_{true};

  // config items
  uint32_t queue_depth_;
  uint32_t batch_size_;

  // protection domain shared by all clients
  struct ibv_pd* pd_ = nullptr;

//  dpf::LatencyWriter latency_writer_{"/data/ssankhe/fig21/device_redy_latency.jsonl"};

  size_t segment_size_;
  std::vector<RedyFrontend*> redy_workers_;
  std::vector<std::thread> worker_threads_;
};
