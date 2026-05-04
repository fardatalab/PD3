#pragma once

#include "types.hpp"

#include "PD3/network_engine/client_types.hpp"
#include "PD3/system/logger.hpp"

#include <vector>

namespace dpf {


class HostViewRefresher {

  static constexpr size_t MESSAGE_QUEUE_SIZE = 1 << 11;
  static constexpr size_t BATCH_BUFFER_MULTIPLIER = 10;

  struct RefreshShard {
    int shard_id;
    ClientRequestBatchQueue* refresh_req_batch_q;
    std::vector<ClientRequestBatch*> refresh_req_batches;
    uint64_t refresh_req_batch_idx;
  };

public:

  HostViewRefresher() = default;

  ~HostViewRefresher() {
    // delete[] refresh_keys_;
    for (auto& shard : refresh_shards_) {
      for (auto batch : shard.refresh_req_batches) {
        delete[] batch->requests;
        delete batch;
      }
      shard.refresh_req_batches.clear();
      delete shard.refresh_req_batch_q;
    }
  }

  void Initialize(int num_shards, int queue_depth, int batch_capacity) {
    num_shards_ = num_shards;
    queue_depth_ = queue_depth;
    batch_capacity_ = batch_capacity;
    refresh_shards_.resize(num_shards);
    for (int i = 0; i < num_shards; ++i) {
      refresh_shards_[i].shard_id = i;
      refresh_shards_[i].refresh_req_batch_q = new ClientRequestBatchQueue(MESSAGE_QUEUE_SIZE);
      refresh_shards_[i].refresh_req_batches.reserve(queue_depth);
      for (int j = 0; j < queue_depth; ++j) {
        ClientRequestBatch* batch = new ClientRequestBatch;
        batch->bytes = 0;
        batch->capacity = batch_capacity_;
        batch->size = 0;
        batch->requests = new ClientRequestT[batch_capacity];
        refresh_shards_[i].refresh_req_batches.push_back(batch);
      }
      refresh_shards_[i].refresh_req_batch_idx = 0;
    }
    // refresh_keys_ = new ClientRequestT[batch_capacity * BATCH_BUFFER_MULTIPLIER];
  }

  ClientRequestBatchQueue* GetRefreshQueue(int shard_id) {
    if (shard_id > refresh_shards_.size()) {
      return nullptr;
    }
    return refresh_shards_[shard_id].refresh_req_batch_q;
  }

  void SetScopedMode(bool read) {
    scoped_mode_ = read;
  }

  void PassRefreshRequest(ClientRequestT req)
  {
    auto shard_id = req % num_shards_; // TODO: optimize
    auto& shard = refresh_shards_[shard_id];
    ClientRequestBatch* batch = shard.refresh_req_batches[shard.refresh_req_batch_idx];
    batch->requests[batch->size++] = req;
    batch->bytes += sizeof(req);
    batch->is_read = scoped_mode_;
    if (batch->size == batch->capacity) {
      // send the batch to the prefetcher thread
      shard.refresh_req_batch_idx = (shard.refresh_req_batch_idx + 1) % shard.refresh_req_batches.size();
      // TODO: reset the batch ids
      if (!shard.refresh_req_batch_q->try_push(batch)) {
        std::cout << "Failed to push host view refresh batch to prefetcher thread\n";
        return;
      }
    }
    return;
  }

  void FlushRefreshRequest()
  {
    for (auto&& shard : refresh_shards_) {
      auto curr_batch = shard.refresh_req_batches[shard.refresh_req_batch_idx];
      curr_batch->is_read = scoped_mode_;
      if (curr_batch->size > 0) {
        // send to the prefetcher queue
        shard.refresh_req_batch_idx = (shard.refresh_req_batch_idx + 1) % shard.refresh_req_batches.size();
        if (!shard.refresh_req_batch_q->try_push(curr_batch)) {
          std::cout << "Failed to push host view refresh batch to prefetcher thread\n";
          return;
        }
      }
    }
  }

  // void HandleReadRefresh(char* data, size_t size) { /* TODO */}
  // void HandleWriteRefresh(char* data, size_t size) { /* TODO */}

private:
  
  int num_shards_;
  int queue_depth_;
  int batch_capacity_;

  bool scoped_mode_;

  std::vector<RefreshShard> refresh_shards_;
  // ClientRequestT* refresh_keys_;
};

}