#pragma once

#include "types.hpp"
#include "hash.hpp"
#include "hugepage_allocator.hpp"
#include "refresher.hpp"

#ifdef __aarch64__
#include "host_view.hpp"
#endif

#include "PD3/transfer_engine/transfer_engine.hpp"

#include "json.h"

#include <atomic>
#include <thread>
#include <chrono>

#define CACHELINE_SIZE 64

namespace dpf {

class Prefetcher {

  static constexpr size_t MESSAGE_QUEUE_SIZE = 1 << 11;

#ifdef __aarch64__
  using CuckooSetT = cuckoo_set::cuckoo_set<CRCHash<uint64_t>, huge_page_allocator<cuckoo_set::Bucket>>;
#endif

  struct HostViewShard {
    int core_id;
    std::thread host_view_thread;
    uint64_t client_req_batch_idx;
    std::vector<ClientRequestBatch*> client_req_batches; // from network engine to prefetcher
    std::vector<PrefetchRequestBatch*> prefetch_req_batches; // to transfer engine
    uint64_t prefetch_req_batch_idx;
    alignas(CACHELINE_SIZE) ClientRequestBatchQueue* client_req_batch_q;
    alignas(CACHELINE_SIZE) PrefetchRequestBatchQueue* prefetch_req_batch_q;
    alignas(CACHELINE_SIZE) RefreshRequestBatchQueue* refresh_req_batch_q = nullptr;

#ifdef __aarch64__
    CuckooSetT host_records_{32 * 1024 * 1024};
    CuckooSetT::iterator host_records_it_[cuckoo_set::MAX_LOOKUP_BATCH_SZ];
#endif
  };

public:

  Prefetcher() = default;
  ~Prefetcher();

  // will need a config file to initialize the prefetcher
  void Initialize(const JSON& config, bool prefetching_enabled);
  void InitializeTest(const JSON& config, bool prefetching_enabled);

  void Run();
  void RunTest();
  void Stop();

  bool PassClientRequest(ClientRequestT req);
  bool Flush(int shard_id);
  bool FlushAll();

private:

  void WorkerThreadBatched(int shard_id);

private:

  // std::vector<ClientRequestBatch*> client_req_batches_;     // from net engine to prefetcher
  HostViewRefresher refresher_;
  std::vector<HostViewShard> host_view_shards_;
  int num_shards_ = 1;

  uint64_t batch_capacity_ = 128;

  std::atomic_bool stop_flag_;

  dpf::offload::TransferEngine transfer_engine_;
#ifdef __aarch64__
  using CuckooSetT = cuckoo_set::cuckoo_set<CRCHash<uint64_t>, huge_page_allocator<cuckoo_set::Bucket>>;
  CuckooSetT host_records_{32 * 1024 * 1024};
  CuckooSetT::iterator host_records_it_[cuckoo_set::MAX_LOOKUP_BATCH_SZ];
#endif

  // Statistics for PassClientRequest()
  static constexpr size_t REQUEST_INTERVAL = 10'000'000;
  size_t req_count_ = 0;
  std::chrono::steady_clock::time_point interval_start_{};
  long min_interval_us_ = std::numeric_limits<long>::max();
};

} // namespace dpf
