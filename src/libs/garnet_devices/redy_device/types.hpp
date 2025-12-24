#pragma once

#include "FASTER/status.h"
#include "FASTER/async.h"

#include <cstdint>

typedef void (*RDMCCallback)(uint32_t error_code, uint32_t bytes, void* context);

struct callback_context {
  FASTER::core::AsyncIOCallback callback;
  uint64_t context_pointer;
};

struct SingleRequestFrontEnd {
  bool is_read;
  uint32_t segment_id;
  uint32_t offset;
  uint64_t app_address;
  uint32_t bytes;

  RDMCCallback callback;
  void* context;
};

inline void FASTERCallback(uint32_t error_code, uint32_t bytes, void* context)
{
  callback_context* ctxt = (callback_context*)context;
  ctxt->callback((FASTER::core::IAsyncContext *)ctxt->context_pointer, FASTER::core::Status::Ok, bytes);
  delete ctxt;
}

class alignas(64) RequestBatchFrontEnd
{
private:
    // the maximum batch size
    int batch_capacity;

    // the current batch size
    int batch_size;

    // whether this batch is ready to fire
    bool ready_to_fire;

    // the pointer to the list of requests
    SingleRequestFrontEnd *requests;

public:
    RequestBatchFrontEnd(int batch_capacity_) {
        batch_capacity = batch_capacity_;
        batch_size = 0;
        ready_to_fire = false;
        requests = new SingleRequestFrontEnd[batch_capacity];
    }
    ~RequestBatchFrontEnd() {
        if (requests)
            delete[] requests;
    }

    RequestBatchFrontEnd(RequestBatchFrontEnd&& other) {
        batch_capacity = other.batch_capacity;
        batch_size = other.batch_size;
        ready_to_fire = other.ready_to_fire;
        requests = other.requests;
        other.requests = nullptr;
    }

    RequestBatchFrontEnd& operator=(RequestBatchFrontEnd&& other) {
        batch_capacity = other.batch_capacity;
        batch_size = other.batch_size;
        ready_to_fire = other.ready_to_fire;
        requests = other.requests;
        other.requests = nullptr;
        return *this;
    }

    // getters and setters
    int GetBatchCapacity() {
        return batch_capacity;
    }
    int GetBatchSize() {
        return batch_size;
    }
    bool GetReadyToFire() {
        return ready_to_fire;
    }
    void SetBatchSize(int batch_size_) {
        batch_size = batch_size_;
    }
    void SetReadyToFire(bool ready_to_fire_) {
        ready_to_fire = ready_to_fire_;
    }
    SingleRequestFrontEnd* GetRequests() {
        return requests;
    }
    SingleRequestFrontEnd* GetRequestByIndex(int index) {
        return &requests[index];
    }

    // init the batch
    void InitBatch() {
        batch_size = 0;
    }

    // increase the batch size by 1
    void IncrementBatchSize() {
        batch_size++;
    }

    // maximize batch size
    void MaximizeBatchSize() {
        batch_size = batch_capacity;
    }

    // check if batch is full
    bool BatchFull() {
        return batch_size == batch_capacity;
    }
};