#pragma once

#include "types.hpp"
#ifdef __aarch64__
#include "host_view.hpp"
#include "hash.hpp"
#include "hugepage_allocator.hpp"
#endif

#include <cstddef>
#include <cstdint>
#include <atomic>
#include <thread>
#include <vector>

namespace dpf {

static constexpr size_t CACHELINE_SIZE = 64; // need to define this since ARM sets default to 256
static constexpr size_t MESSAGE_QUEUE_SIZE = 1 << 14;

// TODO: define relevant constructors 
struct HostViewShard {
  int core_id;
  uint64_t client_req_batch_idx; // TODO: does this have to be cache line aligned?
  std::vector<ClientRequestBatch*> client_req_batches; // from network engine
  std::vector<PrefetchRequestBatch*> prefetch_req_batches; // to transfer engine
  uint64_t prefetch_req_batch_idx; 
  alignas(CACHELINE_SIZE) ClientRequestBatchQueue client_req_batch_q{MESSAGE_QUEUE_SIZE};
  PrefetchRequestBatchQueue* prefetch_req_batch_q;
  ClientRequestBatchQueue* refresh_req_batch_q;
  uint64_t min_key;
  uint64_t max_key;
#ifdef __aarch64__
  using CuckooSetT = cuckoo_set::cuckoo_set<CRCHash<uint64_t>, huge_page_allocator<cuckoo_set::Bucket>>;
  CuckooSetT host_records_{64 * 1024 * 1024};
  CuckooSetT::iterator host_records_it_[cuckoo_set::MAX_LOOKUP_BATCH_SZ];
#endif
};

}