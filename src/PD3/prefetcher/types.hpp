#pragma once

// Contains types common to all DPU prefetcher components.

#include "message_queue.hpp"

#include "book_keeper/types.hpp"
#include "PD3/transfer_engine/types.hpp"

#include <cstdint>
#include <string>

using ClientRequestQueue = MessageQueue<ClientRequestT>;

namespace dpf {

// TODO: make this a template
struct ClientRequestBatch {
  int capacity;  // max number of requests in the batch
  uint64_t bytes;  // total number of bytes in the batch
  int size; // number of requests in this batch
  ClientRequestT* requests;
};

using ClientRequestBatchQueue = MessageQueue<dpf::ClientRequestBatch*>;

struct PrefetchRequestBatch {
  uint64_t* keys;
  int size;
};

using PrefetchRequestBatchQueue = MessageQueue<dpf::PrefetchRequestBatch*>;

struct RefreshRequestBatch {
  int capacity;  // max number of requests in the batch
  uint64_t bytes;  // total number of bytes in the batch
  int size; // number of requests in this batch
  ClientRequestT* requests;
  bool read;
};
using RefreshRequestBatchQueue = MessageQueue<dpf::RefreshRequestBatch*>;

}
