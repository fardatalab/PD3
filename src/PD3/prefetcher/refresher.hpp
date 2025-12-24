#pragma once

#include "types.hpp"

#include "PD3/network_engine/client_types.hpp"
#include "PD3/system/logger.hpp"

#include "user_defined/garnet.hpp"

#include <vector>

namespace dpf {

static constexpr size_t MESSAGE_QUEUE_SIZE = 1 << 10;
static constexpr size_t BATCH_BUFFER_MULTIPLIER = 10;

class HostViewRefresher {

  struct RefreshShard {
    int shard_id;
    RefreshRequestBatchQueue* refresh_req_batch_q;
    std::vector<RefreshRequestBatch*> refresh_req_batches;
    uint64_t refresh_req_batch_idx;
  };

public:

  HostViewRefresher() = default;
  ~HostViewRefresher()
  {
    delete[] refresh_keys_;
    for (auto& shard : refresh_shards_) {
      for (auto batch : shard.refresh_req_batches) {
        delete[] batch->requests;
        delete batch;
      }
      shard.refresh_req_batches.clear();
      delete shard.refresh_req_batch_q;
    }
  }

  void Initialize(int num_shards, int queue_depth, int batch_capacity)
  {
    num_shards_ = num_shards;
    queue_depth_ = queue_depth;
    batch_capacity_ = batch_capacity;
    refresh_shards_.resize(num_shards);
    for (int i = 0; i < num_shards; i++) {
      refresh_shards_[i].shard_id = i;
      refresh_shards_[i].refresh_req_batch_q = new RefreshRequestBatchQueue(MESSAGE_QUEUE_SIZE);
      refresh_shards_[i].refresh_req_batches.reserve(queue_depth);
      for (int j = 0; j < queue_depth; j++) {
        RefreshRequestBatch* batch = new RefreshRequestBatch;
        batch->bytes = 0;
        batch->capacity = batch_capacity_;
        batch->size = 0;
        batch->requests = new ClientRequestT[batch_capacity_];
        refresh_shards_[i].refresh_req_batches.push_back(batch);
      }
      refresh_shards_[i].refresh_req_batch_idx = 0;
    }

    refresh_keys_ = new ClientRequestT[batch_capacity_ * BATCH_BUFFER_MULTIPLIER];
  }

  RefreshRequestBatchQueue* GetRefreshQueue(int shard_id)
  {
    return refresh_shards_[shard_id].refresh_req_batch_q;
  }

  void HandleReadRefresh(char* data, size_t size)
  {
    auto num_keys = ParseRemoteWrite(data, size, refresh_keys_);
    for (int i = 0; i < num_keys; i++) {
      auto key = refresh_keys_[i];
      auto shard_id = key % num_shards_;
      PassRefreshRequest(shard_id, key, true);
    }
  }

  void HandleWriteRefresh(char* data, size_t size)
  {
    auto num_keys = ParseRemoteWrite(data, size, refresh_keys_);
    for (int i = 0; i < num_keys; i++) {
      auto key = refresh_keys_[i];
      auto shard_id = key % num_shards_;
      PassRefreshRequest(shard_id, key, false);
    }
  }

  void PassRefreshRequest(int shard_id, ClientRequestT req, bool read)
  {
    auto& shard = refresh_shards_[shard_id];
    auto& batch = shard.refresh_req_batches[shard.refresh_req_batch_idx];
    batch->requests[batch->size++] = req;
    batch->bytes += sizeof(req);
    batch->read = read;
    if (batch->size == batch->capacity) {
      shard.refresh_req_batch_idx = (shard.refresh_req_batch_idx + 1) % shard.refresh_req_batches.size();
      if (!shard.refresh_req_batch_q->try_push(batch)) {
        LOG_ERROR("Failed to push refresh request batch to prefetcher thread");
      }
    }
  }

private:
  int num_shards_;
  int queue_depth_;
  int batch_capacity_;
  std::vector<RefreshShard> refresh_shards_;

  ClientRequestT* refresh_keys_;
};


} // namespace dpf
