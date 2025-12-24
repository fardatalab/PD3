// The entry point of the prefetcher app that runs on the DPU.

#include "prefetcher.hpp"
#include "types.hpp"
#include "utils.hpp"

#include "PD3/system/logger.hpp"

#include <atomic>
#include <csignal>
#include <iostream>
#include <iomanip>
#include <sstream>
#include <string>

#include <cassert>
#include <thread>

static const size_t MESSAGE_QUEUE_SIZE = 1 << 10;

namespace dpf {

static uint64_t convert_key_from_hex_to_bytes(uint64_t key)
{
  std::stringstream ss;
  ss << std::hex << std::uppercase << std::setfill('0') // Fill with '0' for padding
            << std::setw(8) << key;  // Convert to hexadecimal
  std::string hex_str = ss.str();

  // convert to integer
  uint64_t key_bytes = *reinterpret_cast<const uint64_t*>(hex_str.c_str());
  return key_bytes;
}

Prefetcher::~Prefetcher() {
  for (auto& shard : host_view_shards_) {
    for (auto batch : shard.client_req_batches) {
      delete[] batch->requests;
      delete batch;
    }
    shard.client_req_batches.clear();
    for (auto batch : shard.prefetch_req_batches) {
      delete[] batch->keys;
      delete batch;
    }
    shard.prefetch_req_batches.clear();
  }
}

void Prefetcher::Run() {
  stop_flag_.store(false);
  int shard_id = 0;
  for (auto& shard: host_view_shards_) {
    // TODO: add the shard ID for the thread 
    shard.host_view_thread = std::thread(&Prefetcher::WorkerThreadBatched, this, shard_id);
    shard_id++;
  }
  std::cout << "Started prefetcher thread\n";
  transfer_engine_.Run();
  std::cout << "Started transfer engine\n";
}

void Prefetcher::RunTest() {
  stop_flag_.store(false);
  host_view_shards_[0].host_view_thread = std::thread(&Prefetcher::WorkerThreadBatched, this, 0);
}

void Prefetcher::Stop() {
  stop_flag_.store(true);
  for (auto& shard: host_view_shards_) {
    if (shard.host_view_thread.joinable()) {
      shard.host_view_thread.join();
    }
  }

  transfer_engine_.Stop();

  // print statistics for PassClientRequest()
  double min_interval_s = static_cast<double>(min_interval_us_) / 1'000'000.0;
  double peak_throughput =
      static_cast<double>(REQUEST_INTERVAL) / min_interval_s;

  std::cout << "=== PassClientRequest Stats ===\n";
  std::cout << "REQUEST_INTERVAL: " << REQUEST_INTERVAL << "\n";
  std::cout << "req_count_: " << req_count_ << "\n";
  std::cout << "min_interval_s: " << min_interval_s << "\n";
  std::cout << "peak_throughput: " << peak_throughput << " req/s\n";
}

void Prefetcher::InitializeTest(const JSON& config, bool prefetching_enabled) {
  uint64_t queue_depth = 128;
  uint64_t batch_capacity = 1024;
  int num_shards = 1;
  HostViewShard shard;
  shard.core_id = 8;
  shard.client_req_batch_idx = 0;
  for (auto i = 0; i < queue_depth; i++) {
    ClientRequestBatch* batch = new ClientRequestBatch;
    batch->bytes = 0;
    batch->capacity = batch_capacity_;
    batch->size = 0;
    batch->requests = new ClientRequestT[batch_capacity_];
    shard.client_req_batches.push_back(batch);
  }
  shard.client_req_batch_idx = 0;
  host_view_shards_.push_back(std::move(shard));

  for (auto i = 0; i < queue_depth; i++) {
    PrefetchRequestBatch* batch = new PrefetchRequestBatch;
    batch->keys = new uint64_t[batch_capacity_];
    batch->size = 0;
    shard.prefetch_req_batches.push_back(batch);
  }
  shard.prefetch_req_batch_idx = 0;

}

void Prefetcher::Initialize(const JSON& config, bool prefetching_enabled) {
  std::cout << "Initializing prefetcher\n";
  // configure the offload server
  if (config.count("offload_server")) {
    auto stanza = config["offload_server"];
    offload::TransferEngineConfig transfer_engine_config;
    try {
      // rdma server config params
      transfer_engine_config.server_addr = stanza["server_addr"].get<std::string>();
      transfer_engine_config.server_port = stanza["server_port"].get<std::string>() ;
      transfer_engine_config.max_wr = stanza["max_wr"].get<uint64_t>();
      transfer_engine_config.use_rdma = stanza["use_rdma"].get<bool>();

      // dpu config params
      transfer_engine_config.dpu_pcie_addr = stanza["dpu_pcie_addr"].get<std::string>();

      // dma config params
      transfer_engine_config.enable_transfer = stanza["enable_transfer"].get<bool>();
      transfer_engine_config.export_desc_client_file_path = stanza["export_desc_client_file_path"].get<std::string>();
      transfer_engine_config.buf_client_file_path = stanza["buf_client_file_path"].get<std::string>();

      transfer_engine_config.enable_agent = prefetching_enabled;
      transfer_engine_config.export_desc_agent_file_path = stanza["export_desc_agent_file_path"].get<std::string>();
      transfer_engine_config.buf_agent_file_path = stanza["buf_agent_file_path"].get<std::string>(); 
      
      transfer_engine_config.num_slots = stanza["num_slots"].get<uint64_t>();
      transfer_engine_config.num_prefetch_slots = stanza["num_prefetch_slots"].get<uint64_t>();
      transfer_engine_config.slot_size = stanza["slot_size"].get<uint64_t>();
      std::cout << "Initializing transfer engine\n";

      transfer_engine_.Initialize(transfer_engine_config);
      std::cout << "Initialized transfer engine\n";

    } catch (const std::exception& e) {
      std::cout << "Error parsing offload server config: " << e.what() << "\n";
      throw std::runtime_error("Error parsing offload server config: " + std::string(e.what()));
    }
  } else {
    std::cout << "Offload server config not found in prefetcher config\n";
    throw std::runtime_error("Offload server config not found in prefetcher config");
  }
  // configure the client request batch sizes and queue depth
  if (config.count("prefetcher")) {
    auto stanza = config["prefetcher"];
    try {
         // initialize the client request batch
      uint64_t queue_depth = 4096;
      if (stanza.count("queue_depth")) {
        queue_depth = stanza["queue_depth"].get<uint64_t>();
      }
      batch_capacity_ = 500;
      if (stanza.count("batch_capacity")) {
        batch_capacity_ = stanza["batch_capacity"].get<uint64_t>();
      }
      num_shards_ = 1;
      if (stanza.count("num_shards")) {
        num_shards_ = stanza["num_shards"].get<int>();
      }

      refresher_.Initialize(num_shards_, queue_depth, batch_capacity_);
      transfer_engine_.SetRefresher(&refresher_);

      for (auto i = 0; i < num_shards_; ++i) {
        HostViewShard shard;
        shard.core_id = i;
        shard.client_req_batch_idx = 0;
        for (auto j = 0; j < queue_depth; ++j) {
          ClientRequestBatch* batch = new ClientRequestBatch;
          batch->bytes = 0;
          batch->capacity = batch_capacity_;
          batch->size = 0;
          batch->requests = new ClientRequestT[batch_capacity_];
          shard.client_req_batches.push_back(batch);
        }
        for (auto i = 0; i < queue_depth; i++) {
          PrefetchRequestBatch* batch = new PrefetchRequestBatch();
          batch->keys = new uint64_t[batch_capacity_];
          batch->size = 0;
          shard.prefetch_req_batches.push_back(batch);
        }
        shard.prefetch_req_batch_idx = 0;
        shard.client_req_batch_q = new ClientRequestBatchQueue(MESSAGE_QUEUE_SIZE);
        shard.prefetch_req_batch_q = new PrefetchRequestBatchQueue(MESSAGE_QUEUE_SIZE);
        shard.refresh_req_batch_q = refresher_.GetRefreshQueue(i);
        transfer_engine_.AddPrefetchRequestQueue(shard.prefetch_req_batch_q);
        host_view_shards_.push_back(std::move(shard));
      }


    } catch (const std::exception& e) {
      throw std::runtime_error("Error parsing prefetcher config: " + std::string(e.what()));
    }
  }
}

bool Prefetcher::PassClientRequest(ClientRequestT req) {
  auto shard_id = req % num_shards_;
  auto& shard = host_view_shards_[shard_id];
  ClientRequestBatch* batch = shard.client_req_batches[shard.client_req_batch_idx];
  batch->requests[batch->size++] = req;
  batch->bytes += sizeof(req);
  if (batch->size == batch->capacity) {
    // send the batch to the prefetcher thread
    shard.client_req_batch_idx = (shard.client_req_batch_idx + 1) % shard.client_req_batches.size();
    if (!shard.client_req_batch_q->try_push(batch)) {
      std::cout << "Failed to push client request batch to prefetcher thread\n";
      return false;
    }
  }
  return false;
}

bool Prefetcher::Flush(int shard_id) {
  auto& shard = host_view_shards_[shard_id];
  ClientRequestBatch* batch = shard.client_req_batches[shard.client_req_batch_idx];
  shard.client_req_batch_idx = (shard.client_req_batch_idx + 1) % shard.client_req_batches.size();
  return shard.client_req_batch_q->try_push(batch);
}

bool Prefetcher::FlushAll() {
  int i = 0;
  for (auto& shard : host_view_shards_) {
    if (!Flush(i)) {
      return false;
    }
    i++;
  }
  return true;
}

void Prefetcher::WorkerThreadBatched(int shard_id) {
  std::cout << "Started Prefetcher thread (batched)...\n";

  auto& shard = host_view_shards_[shard_id];

  // Pin this thread to a CPU core (core 8 for example)
  int core_id = shard.core_id; 
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(core_id, &cpuset);
  int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
  if (rc != 0) {
    std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
  }

  while (!stop_flag_.load(std::memory_order_relaxed)) {
    bool cpu_pause = true;
    ClientRequestBatch** batch = shard.client_req_batch_q->front();
    if (batch) {
      auto batch_ptr = *batch;
      shard.client_req_batch_q->pop();
      cpu_pause = false;

      // process the client requests
      // only works on ARM
      auto prefetch_batch = shard.prefetch_req_batches[shard.prefetch_req_batch_idx];
      shard.prefetch_req_batch_idx = (shard.prefetch_req_batch_idx + 1) % shard.prefetch_req_batches.size();
#ifdef __aarch64__
      using cuckoo_set::MAX_LOOKUP_BATCH_SZ;
      for (size_t i = 0; i < batch_ptr->size; i += MAX_LOOKUP_BATCH_SZ) { 
        auto batch_size = std::min(MAX_LOOKUP_BATCH_SZ, (size_t)(batch_ptr->size - i));
        host_records_.find_batched(batch_ptr->requests + i, batch_size, host_records_it_);
        for (auto j = 0; j < batch_size; j++) {
          if (host_records_it_[j].is_null()) {
            prefetch_batch->keys[prefetch_batch->size++] = batch_ptr->requests[i + j];
          }
        }
      }
#endif
      if (prefetch_batch->size > 0) {
        auto pushed = shard.prefetch_req_batch_q->try_push(prefetch_batch);
        if (!pushed) {
          LOG_ERROR("Failed to push prefetch request batch to prefetch_req_batch_q_");
        }
      }
    }
    
    // process a refresh batch
    RefreshRequestBatch** refresh_batch = nullptr;
    refresh_batch = shard.refresh_req_batch_q->front();
    if (refresh_batch) {
      auto refresh_batch_ptr = *refresh_batch;
      shard.refresh_req_batch_q->pop();
      cpu_pause = false;

      // process the refresh requests
      if (refresh_batch_ptr->read) {
#ifdef __aarch64__
      for (auto i = 0; i < refresh_batch_ptr->size; i++) {
        host_records_.insert(refresh_batch_ptr->requests[i]);
      }
#endif
      } else {
#ifdef __aarch64__
        for (auto i = 0; i < refresh_batch_ptr->size; i += MAX_LOOKUP_BATCH_SZ) {
          auto batch_size = std::min(MAX_LOOKUP_BATCH_SZ, (size_t)(refresh_batch_ptr->size - i));
          host_records_.find_batched(refresh_batch_ptr->requests + i, batch_size, host_records_it_);
          for (auto j = 0; j < batch_size; j++) {
            if (!host_records_it_[j].is_null()) {
              host_records_.erase(host_records_it_[j]);
            }
          }
        }
  #endif  
      }
    }

    if (cpu_pause) {
      CpuPause();
    }
  }
  std::cout << "Stopped Prefetcher thread (batched)...\n";
}

} // namespace dpf
