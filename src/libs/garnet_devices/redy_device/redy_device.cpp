#include "redy_device.hpp"
#include "types.hpp"

#include "PD3/system/logger.hpp"

#include <iostream>

/**
 * Increment the cache cursor.
 *
*/
void RequestBatchRing::IncrementCacheCursor() {
    cache_cursor = (cache_cursor + 1) % ring_capacity;
}

/**
 * Increment the application cursor.
 *
*/
void RequestBatchRing::IncrementAppCursor() {
    app_cursor = (app_cursor + 1) % ring_capacity;
}

/**
* Whether we can increment the app cursor.
* Incrementing the app cursor shouldn't cross the cache cursor.
*
*/
bool RequestBatchRing::AppCursorIncrementable() {
    return (app_cursor + 1) % ring_capacity != cache_cursor;
}

/**
 * Get a batch on the ring for cache, the consumer.
 *
*/
RequestBatchFrontEnd* RequestBatchRing::GetCacheBatch() {
    RequestBatchFrontEnd *batch = NULL;

    // check if app cursor is working on the batch
    if (cache_cursor != app_cursor) {
        batch = batches[cache_cursor];
    }

    return batch;
}

/**
 * Get a batch on the ring for app, the producer.
 * This only works for one producer.
 *
*/
RequestBatchFrontEnd* RequestBatchRing::GetAppBatch() {
    RequestBatchFrontEnd *batch = NULL;

    // check if we can still use the current batch
    if (batches[app_cursor]->GetBatchSize() < batches[app_cursor]->GetBatchCapacity()) {
        batch = batches[app_cursor];
    }
    // otherwise, check if we can move the cursor
    else {
        int next_cursor = (app_cursor + 1) % ring_capacity;
        if (next_cursor != cache_cursor) {
            app_cursor = next_cursor;
            batch = batches[app_cursor];
        }
    }

    return batch;
}

/**
 * Resize this ring.
 *
*/
void RequestBatchRing::Resize(int ring_capacity_, int batch_capacity_) {
    // clean existing data structures
    for (int i = 0; i != ring_capacity; i++) {
        delete batches[i];
    }
    batches.clear();

    // create new data structures
    ring_capacity = ring_capacity_;
    app_cursor = 0;
    cache_cursor = 0;

    for (int i = 0; i != ring_capacity; i++) {
        RequestBatchFrontEnd *batch = new RequestBatchFrontEnd(batch_capacity_);
        batches.push_back(batch);
    }
}

RedyDevice::RedyDevice(const std::string& name) {
  std::cout << "RedyDevice constructor" << std::endl;

  const uint32_t QUEUE_DEPTH = 16;
  const uint32_t BATCH_SIZE = 30;
  const uint32_t RING_CAPACITY = 1024;

  queue_depth_ = QUEUE_DEPTH;
  batch_size_ = BATCH_SIZE;

  segment_size_ = 1024 * 1024 * 1024;

  JSON config;
  config["server_addr"] = "10.10.1.100";
  config["server_port"] = "51216";
  config["queue_depth"] = QUEUE_DEPTH;
  config["batch_size"] = BATCH_SIZE;

  // create each of the RedyFrontendReq objects (max 32 threads)
  for (int i = 0; i < FASTER::core::Thread::kMaxNumThreads; ++i) {
    auto redy_fe = new RedyFrontend();
    redy_fe->redy_client.Configure(config);
    redy_fe->redy_client.Init(&pd_, true);
    redy_fe->request_pool.Init(BATCH_SIZE * QUEUE_DEPTH);
    redy_fe->pending_requests = new rigtorp::SPSCQueue<SingleRequestFrontEnd*>(BATCH_SIZE * QUEUE_DEPTH);

    for (int j = 0; j < QUEUE_DEPTH; ++j) {
      RequestBatchFrontEnd batch(BATCH_SIZE);
      redy_fe->inflight_requests.push_back(std::move(batch));
    }

    // redy_fe->preflight_requests = new RequestBatchRing(RING_CAPACITY, BATCH_SIZE);
    redy_workers_.push_back(redy_fe);
  }
  for (int i = 0; i < FASTER::core::Thread::kMaxNumThreads; ++i) {
    // start workers
    worker_threads_.push_back(std::thread([this](int id) {
      this->WorkerThread(id);
    }, i));
  }
}

RedyDevice::~RedyDevice()
{
  running_ = false;
  for (auto& thread: worker_threads_) {
    if (thread.joinable()) thread.join();
  }
  for (int i = 0; i < redy_workers_.size(); ++i) {
    auto redy_fe_ptr = redy_workers_[i];
    if (redy_fe_ptr->pending_requests != nullptr) {
      delete redy_fe_ptr->pending_requests;
    }
    if (redy_fe_ptr->pending_requests != nullptr) {
      delete redy_fe_ptr->pending_requests;
    }
    delete redy_fe_ptr;
  }
  ibv_dealloc_pd(pd_);
}

void RedyDevice::Reset()
{
  return; // no-op
}

FASTER::core::Status RedyDevice::ReadAsync(
  uint64_t source,
  void* dest,
  uint32_t length,
  FASTER::core::AsyncIOCallback callback,
  void* context)
{
  // latency_writer_.ContextStart();
  AsyncIoContext ctx{context, callback};

  FASTER::core::Thread::acquire_id();
  auto thread_id = FASTER::core::Thread::id();

  // segment conversion
  uint64_t segment = source / segment_size_;
  auto new_source = source % segment_size_;

  // dpf::LOG_INFOF("ReadAsync: segment: {} offset: {} length: {}", segment, new_source, length);

  auto& redy_client = redy_workers_[thread_id];

  SingleRequestFrontEnd* req = nullptr;
  while ((req = redy_client->request_pool.Allocate()) == nullptr) {
    std::this_thread::yield();
  }

  req->is_read = true;
  req->segment_id = segment;
  req->offset = new_source;
  req->app_address = (uint64_t)dest;
  req->bytes = length;

  // callback
  callback_context* ctxt = new callback_context();
  ctxt->callback = callback;
  ctxt->context_pointer = (uint64_t)context;

  req->callback = FASTERCallback;
  req->context = (void*)ctxt;

  redy_client->pending_requests->push(req);

  FASTER::core::Thread::release_id();

  // latency_writer_.ContextEnd();
  // latency_writer_.Write();

  return FASTER::core::Status::Ok;
}

FASTER::core::Status RedyDevice::WriteAsync(
  const void* source,
  uint64_t dest,
  uint32_t length,
  FASTER::core::AsyncIOCallback callback,
  void* context
) {

  FASTER::core::Thread::acquire_id();
  auto thread_id = FASTER::core::Thread::id();
  // segment conversion
  uint64_t segment = dest / segment_size_;
  auto new_source = dest % segment_size_;

  auto& redy_fe = redy_workers_[thread_id];
  // send a synchronous RDMA write using the Redy client  
  dpf::LOG_INFOF("WriteAsync: segment: {} offset: {} length: {}", segment, new_source, length);
  redy_fe->redy_client.OneSidedRequest(false, (uint32_t)segment, new_source, length, (char*)source);

  FASTER::core::Thread::release_id();

  callback((FASTER::core::IAsyncContext*)context, FASTER::core::Status::Ok, length);

  return FASTER::core::Status::Ok;
}

uint64_t RedyDevice::GetFileSize(uint64_t segment) {
  return 0;
}

void RedyDevice::RemoveSegment(uint64_t segment) {
  return; // no-op, segments are pre-created
}

void RedyDevice::WorkerThread(int thread_id)
{
  // TODO: do we need to pin to a core

  // prepare environment
  auto& redy_fe = redy_workers_[thread_id];
  // auto preflight_ring = redy_fe->preflight_requests;

  while (running_.load(std::memory_order_relaxed)) {
    for (int i = 0; i < queue_depth_; ++i) {
      auto& dst_batch = redy_fe->inflight_requests[i];
      if (dst_batch.GetBatchSize() == 0) {
        bool execute_batch = false;
        // drain all requests in the buffer until either (1) empty, (2) the batch is full
        SingleRequestFrontEnd** req_ptr = nullptr;
        while (true) {
          req_ptr = redy_fe->pending_requests->front();
          if (req_ptr == nullptr) {
            // check if the batch size is non-zero
            if (dst_batch.GetBatchSize() > 0) {
              execute_batch = true;
            }
            break;
          }
          auto req = *req_ptr;
          // dpf::LOG_INFOF("ReadAsync: segment: {} offset: {} length: {}", req->segment_id, req->offset, req->bytes);
          // else, we need to push this request onto the batch 
          auto curr_index = dst_batch.GetBatchSize();
          std::memcpy(dst_batch.GetRequestByIndex(curr_index), req, sizeof(SingleRequestFrontEnd));
          dst_batch.SetBatchSize(curr_index + 1);
          redy_fe->request_pool.Deallocate(req);
          redy_fe->pending_requests->pop();
          if (dst_batch.BatchFull()) {
            execute_batch = true;
            break;
          }
        }

        if (execute_batch) {
          //latency_writer_.MemoryAccessStart();
          int size = dst_batch.GetBatchSize();
          int next_slot = 0;
          for (int j = 0; j != size; ++j) {
            SingleRequestFrontEnd* req = dst_batch.GetRequestByIndex(j);
            // dpf::LOG_INFOF("ReadAsync: segment: {} offset: {} length: {}", req->segment_id, req->offset, req->bytes);
            redy_fe->redy_client.BatchRequest(i, req->is_read, req->segment_id, req->offset, req->app_address, req->bytes);
            next_slot++;
          }
          redy_fe->redy_client.ExecuteBatch(i);
        }

        // this is available
        // RequestBatchFrontEnd* src_batch = preflight_ring->GetCacheBatch();
        // if (src_batch != nullptr) {
        //   int size = src_batch->GetBatchSize();
        //   dst_batch.SetBatchSize(size);
        //   std::memcpy(dst_batch.GetRequests(), src_batch->GetRequests(), size * sizeof(SingleRequestFrontEnd));
        //   src_batch->InitBatch();
        //   preflight_ring->IncrementCacheCursor();
    
      } else { // the size is not 0, we should see if completed
        if (redy_fe->redy_client.PollMessageAt(i)) {
          //latency_writer_.MemoryAccessEnd();
          char* data;
          uint32_t bytes_read = 0;
          data = redy_fe->redy_client.GetResponseAt(i, &bytes_read);
          int num_requests = dst_batch.GetBatchSize();
          uint32_t location = 0;
          for (int j = 0; j != num_requests; ++j) {
            auto req = dst_batch.GetRequestByIndex(j);
            if (req->is_read) {
              // dpf::LOG_INFOF("Completion: segment: {} offset: {} length: {}", req->segment_id, req->offset, req->bytes);
              std::memcpy((void*)req->app_address, data + location, req->bytes);
              location += req->bytes;
            }
            //latency_writer_.ConsumeStart();
            req->callback(0, req->bytes, req->context);
            //latency_writer_.ConsumeEnd();
          }
          redy_fe->redy_client.RemoveResponseAt(i);
          dst_batch.InitBatch();
          //latency_writer_.ContextEnd();
          //latency_writer_.Write();
        }
      }
    }
  }
}
