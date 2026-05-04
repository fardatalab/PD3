#pragma once

#include "refresher.hpp"
#include "types.hpp"
#include "host_view_shard.hpp"

#include "PD3/transfer_engine/transfer_engine.hpp"

#include "common/json.hpp"

#include <atomic>
#include <thread>
#include <chrono>

#define CACHELINE_SIZE 64

namespace dpf {

class Prefetcher {

  static constexpr size_t MESSAGE_QUEUE_SIZE = 1 << 11;

public:

  Prefetcher() = default;
  ~Prefetcher();

  // will need a config file to initialize the prefetcher
  void Initialize(const JSON& config, bool prefetching_enabled);
  void InitializeTest(const JSON& config, bool prefetching_enabled);
  
  // used for testing, no transfer engine here
  void InitializeBare(const JSON& config); 

  void Run();
  void RunBare();
  void RunTest();
  void Stop();
  void StopBare();

  bool PassClientRequest(ClientRequestT req);
  bool Flush();
  bool Flush(int32_t shard_id);

  std::vector<PrefetchRequestBatchQueue*> GetPrefetchQueues();
  HostViewRefresher* GetRefresher();
  
  void BootstrapHostView(uint64_t n);
  void BootstrapHostView(const std::vector<uint64_t>& keys);

  // testing functions
  void SetShardMinMaxKeys(int shard_id, uint64_t min_key, uint64_t max_key);
  bool HostViewContains(uint64_t key);

private:

  // void WorkerThreadBatched();
  void WorkerThreadBatched(int32_t shard_id);
  void ProcessClientRequestBatch(HostViewShard* shard, ClientRequestBatch* batch);
  void ProcessRefreshRequestBatch(HostViewShard* shard, ClientRequestBatch* batch);

private:

  std::vector<ClientRequestBatch*> client_req_batches_; // TODO: switch these to request rings
  std::vector<PrefetchRequestBatch*> prefetch_req_batches_;
  uint64_t client_req_batch_idx_;
  uint64_t prefetch_req_batch_idx_;

  uint64_t batch_capacity_ = 128;
  int32_t num_shards_ = 1;

  HostViewRefresher refresher_;
  std::vector<HostViewShard*> host_view_shards_;
  std::vector<std::thread> host_view_threads_;

  std::atomic_bool stop_flag_;

  dpf::offload::TransferEngine transfer_engine_;

  // Statistics for PassClientRequest()
  static constexpr size_t REQUEST_INTERVAL = 10'000'000;
  size_t req_count_ = 0;
  std::chrono::steady_clock::time_point interval_start_{};
  long min_interval_us_ = std::numeric_limits<long>::max();
};

} // namespace dpf
