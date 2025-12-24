#include "redy_device_single.hpp"

RedyDeviceSingle::RedyDeviceSingle(const std::string& name) {
  std::cout << "RedyDeviceSingle constructor\n";

  const uint32_t QUEUE_DEPTH = 16;
  const uint32_t BATCH_SIZE = 128;
  const uint32_t RING_CAPACITY = 1024;

  segment_size_ = 1024 * 1024 * 1024; // 1GB

  queue_depth_ = QUEUE_DEPTH;
  batch_size_ = batch_size_;

  JSON config;
  config["server_addr"] = "10.10.1.100";
  config["server_port"] = "1234";
  config["queue_depth"] = QUEUE_DEPTH;
  config["batch_size"] = BATCH_SIZE;

  redy_client_.Configure(config);
  redy_client_.Init();

  // initialize the worker attributes
  for (int i = 0; i < queue_depth_; ++i) {
    RequestBatchFrontEnd fe(batch_size_);
    frontend_requests_.push_back(std::move(fe));
  }
  preflight_size_ = queue_depth_ * 4;
  for (int i = 0; i < preflight_size_; ++i) {
    RequestBatchFrontEnd fe(batch_size_);
    preflight_requests_.push_back(std::move(fe));
  }
  producer_ = 0;
  consumer_ = 0;
  slots_ = preflight_size_;

  redy_worker_ = std::thread([this]() {
    this->WorkerThread();
  });
}

RedyDeviceSingle::~RedyDeviceSingle()
{
  running_ = false;
  if (redy_worker_.joinable()) {
    redy_worker_.join();
  }
}

FASTER::core::Status RedyDeviceSingle::ReadAsync(uint64_t source,
                                                 void* dest,
                                                 uint32_t length,
                                                 FASTER::core::AsyncIOCallback callback,
                                                 void* context)
{
  AsyncIoContext ctx{context, callback};

  FASTER::core::Thread::acquire_id();
  auto thread_id = FASTER::core::Thread::id();
  // segment conversion
  uint64_t segment = source / segment_size_;
  auto new_source = source % segment_size_;

  SingleRequestFrontEnd req;
  req.is_read = true;
  req.segment_id = segment;
  req.offset = new_source;
  req.app_address = (uint64_t)dest;
  req.bytes = length;
  // callback
  callback_context* ctxt = new callback_context();
  ctxt->callback = callback;
  // copy context to heap
  FASTER::core::IAsyncContext* caller_context_copy;
  ctx.DeepCopy(caller_context_copy);
  ctxt->context_pointer = (uint64_t)caller_context_copy;

  req.callback = FASTERCallback;
  req.context = (void*)ctxt;

  request_queue_.push(std::move(req));

  return FASTER::core::Status::Pending;
}


FASTER::core::Status RedyDeviceSingle::WriteAsync(const void* source,
                                                  uint64_t dest,
                                                  uint32_t length,
                                                  FASTER::core::AsyncIOCallback callback,
                                                  void* context)
{
  AsyncIoContext ctx{context, callback};

  FASTER::core::Thread::acquire_id();
  auto thread_id = FASTER::core::Thread::id();
  // segment conversion
  uint64_t segment = dest / segment_size_;
  auto new_source = dest % segment_size_;

  SingleRequestFrontEnd req;
  req.is_read = false;
  req.segment_id = segment;
  req.offset = new_source;
  req.app_address = (uint64_t)source;
  req.bytes = length;
  // callback
  callback_context* ctxt = new callback_context();
  ctxt->callback = callback;
  // copy context to heap
  FASTER::core::IAsyncContext* caller_context_copy;
  ctx.DeepCopy(caller_context_copy);
  ctxt->context_pointer = (uint64_t)caller_context_copy;

  req.callback = FASTERCallback;
  req.context = (void*)ctxt;

  request_queue_.push(std::move(req));

  return FASTER::core::Status::Pending;
}

void RedyDeviceSingle::WorkerThread()
{
  while (running_.load(std::memory_order_relaxed)) {
    // try to fill frontend batches
    while (slots_ > 0) {
      SingleRequestFrontEnd req;
      auto popped = request_queue_.try_pop(req);
      if (popped) {
        auto& curr_batch = preflight_requests_[producer_];
        auto curr_req = curr_batch.GetRequestByIndex(curr_batch.GetBatchSize());
        curr_req->is_read = req.is_read;
        curr_req->segment_id = req.segment_id;
        curr_req->offset = req.offset;
        curr_req->callback = req.callback;
        curr_req->context = req.context;
        curr_req->app_address = req.app_address;
        curr_req->bytes = req.bytes;
        curr_batch.IncrementBatchSize();
        if (curr_batch.BatchFull() || !req.is_read) {
          IncrementProducer();
        }
      } else {
        break;
      }
    }
    // poll batches and send out
    for (int i = 0; i < queue_depth_; ++i) {
      auto& dst_batch = frontend_requests_[i];
      if (dst_batch.GetBatchSize() == 0) {
        // this is available
        if (slots_ < preflight_size_) {
          // we have stuff to consume
          auto& src_batch = preflight_requests_[consumer_];
          int size = src_batch.GetBatchSize();
          dst_batch.SetBatchSize(size);
          std::memcpy(dst_batch.GetRequests(), src_batch.GetRequests(), size * sizeof(SingleRequestFrontEnd));
          src_batch.InitBatch();
          IncrementConsumer();

          for (int j = 0; j != size; ++j) {
            SingleRequestFrontEnd* req = dst_batch.GetRequestByIndex(j);
            redy_client_.BatchRequest(i, req->is_read, req->segment_id, req->offset, req->app_address, req->bytes);
          }
          redy_client_.ExecuteBatch(i);
        }
      } else {
        if (redy_client_.PollMessageAt(i)) {
          char* data;
          uint32_t bytes_read;
          data = redy_client_.GetResponseAt(i, &bytes_read);
          int num_requests = dst_batch.GetBatchSize();
          uint32_t location = 0;
          for (int j = 0; j != num_requests; ++j) {
            auto req = dst_batch.GetRequestByIndex(j);
            if (req->is_read) {
              std::memcpy((void*)req->app_address, data + location, req->bytes);
              location += req->bytes;
            }
            req->callback(0, req->bytes, req->context);
          }
          redy_client_.RemoveResponseAt(i);
          dst_batch.InitBatch();
        }
      }
    }
  }
}

void RedyDeviceSingle::IncrementProducer()
{
  producer_ = (producer_++) % preflight_size_;
  slots_--;
}

void RedyDeviceSingle::IncrementConsumer()
{
  consumer_ = (consumer_++) % preflight_size_;
  slots_++;
}
