// The entry point of the prefetcher app that runs on the DPU.

#include "prefetcher.hpp"
#include "types.hpp"
#include "utils.hpp"

#include "PD3/config.hpp"

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
  for (auto shard : host_view_shards_) {
    for (auto batch : shard->client_req_batches) {
      delete[] batch->requests;
      delete batch;
    }
    shard->client_req_batches.clear();
    for (auto batch : shard->prefetch_req_batches) {
      delete[] batch->keys;
      delete batch;
    }
    shard->prefetch_req_batches.clear();
    delete(shard);
  }
}

void Prefetcher::Run() {
  stop_flag_.store(false);
  std::cout << "Starting prefetcher thread\n";
  int shard_id = 0;
  for (auto shard : host_view_shards_) {
    host_view_threads_.push_back(std::thread(&Prefetcher::WorkerThreadBatched, this, shard_id));
    shard_id++;
  }
  std::cout << "Started prefetcher thread\n";
  transfer_engine_.Run();
  std::cout << "Started transfer engine\n";
}

void Prefetcher::RunBare() {
  stop_flag_.store(false);
  int shard_id = 0;
  for (auto shard : host_view_shards_) {
    host_view_threads_.push_back(std::thread(&Prefetcher::WorkerThreadBatched, this, shard_id));
    shard_id++;
  }
  std::cout << "Started prefetcher thread\n";
}

void Prefetcher::RunTest() {
  stop_flag_.store(false);
  host_view_threads_.push_back(std::thread(&Prefetcher::WorkerThreadBatched, this, 0));
}

void Prefetcher::Stop() {
  stop_flag_.store(true);
  for (auto&& thread : host_view_threads_) {
    if (thread.joinable()) {
      thread.join();
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

void Prefetcher::StopBare() {
  stop_flag_.store(true);
  for (auto&& thread : host_view_threads_) {
    if (thread.joinable()) {
      thread.join();
    }
  }
}


void Prefetcher::InitializeTest(const JSON& config, bool prefetching_enabled) {
  uint64_t queue_depth = 128;
  uint64_t batch_capacity = 1024;
  for (auto i = 0; i < queue_depth; i++) {
    ClientRequestBatch* batch = new ClientRequestBatch;
    batch->bytes = 0;
    batch->capacity = batch_capacity_;
    batch->size = 0;
    batch->requests = new ClientRequestT[batch_capacity_];
    client_req_batches_.push_back(batch);
  }
  client_req_batch_idx_ = 0;

  for (auto i = 0; i < queue_depth; i++) {
    PrefetchRequestBatch* batch = new PrefetchRequestBatch;
    batch->keys = new uint64_t[batch_capacity_];
    batch->size = 0;
    prefetch_req_batches_.push_back(batch);
  }
  prefetch_req_batch_idx_ = 0;

}

void Prefetcher::Initialize(const JSON& config, bool prefetching_enabled) {
  std::cout << "Initializing prefetcher\n";
  // configure the offload server
  if (config.count("offload_server")) {
    auto stanza = config["offload_server"];
    offload::TransferEngineConfig transfer_engine_config;
    try {
      // rdma server config params
      transfer_engine_config.local_addr = stanza["local_addr"].get<std::string>();
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
      uint64_t queue_depth = 8192;
      if (stanza.count("queue_depth")) {
        queue_depth = stanza["queue_depth"].get<uint64_t>();
      }
      batch_capacity_ = 500;
      if (stanza.count("batch_capacity")) {
        batch_capacity_ = stanza["batch_capacity"].get<uint64_t>();
      }
      num_shards_ = 1;
      if (stanza.count("num_shards")) {
        num_shards_ = stanza["num_shards"].get<uint64_t>();
      }
      
      // initialize the refresher here
      refresher_.Initialize(num_shards_, queue_depth, batch_capacity_);
      transfer_engine_.SetRefresher(&refresher_);
      for (auto i = 0; i < num_shards_; ++i) {
        HostViewShard* shard = new HostViewShard();
        shard->core_id = i;
        shard->client_req_batch_idx = 0;
        shard->refresh_req_batch_q = refresher_.GetRefreshQueue(i);
        for (auto j = 0; j < queue_depth + 1; ++j) {
          ClientRequestBatch* batch = new ClientRequestBatch;
          batch->bytes = 0;
          batch->capacity = batch_capacity_;
          batch->size = 0;
          batch->requests = new ClientRequestT[batch_capacity_];
          shard->client_req_batches.push_back(batch);
        }
        shard->client_req_batch_idx = 1;
  
        for (auto j = 0; j < queue_depth; ++j) {
          PrefetchRequestBatch* batch = new PrefetchRequestBatch();
          batch->keys = new uint64_t[batch_capacity_];
          batch->size = 0;
          shard->prefetch_req_batches.push_back(batch);
        }
        shard->prefetch_req_batch_idx = 0;
        host_view_shards_.push_back(shard);
        // TODO: place the transfer engine AddPrefetchRequestQueue() here
      }

    } catch (const std::exception& e) {
      throw std::runtime_error("Error parsing prefetcher config: " + std::string(e.what()));
    }
  }
}

void Prefetcher::InitializeBare(const JSON& config)
{
  std::cout << config << '\n';
  if (config.count("prefetcher")) {
    LOG_INFO("Here");
    auto stanza = config["prefetcher"];
    try {
      uint64_t queue_depth = 8192;
      if (stanza.count("queue_depth")) {
        queue_depth = stanza["queue_depth"].get<uint64_t>();
        if (stanza.count("queue_depth")) {
          queue_depth = stanza["queue_depth"].get<uint64_t>();
        }
      }
      batch_capacity_ = 500;
      if (stanza.count("batch_capacity")) {
        batch_capacity_ = stanza["batch_capacity"].get<uint64_t>();
      }
      num_shards_ = 1;
      if (stanza.count("num_shards")) {
        LOG_INFO("here");
        num_shards_ = stanza["num_shards"].get<uint64_t>();
      }
      refresher_.Initialize(num_shards_, queue_depth, batch_capacity_);
      for (auto i = 0; i < num_shards_; ++i) {
        HostViewShard* shard = new HostViewShard();
        shard->core_id = i;
        shard->client_req_batch_idx = 0;
        shard->refresh_req_batch_q = refresher_.GetRefreshQueue(i);
        for (auto j = 0; j < queue_depth + 1; ++j) {
          ClientRequestBatch* batch = new ClientRequestBatch;
          batch->bytes = 0;
          batch->capacity = batch_capacity_;
          batch->size = 0;
          batch->requests = new ClientRequestT[batch_capacity_];
          batch->is_read = true;
          shard->client_req_batches.push_back(batch);
        }
        shard->client_req_batch_idx = 1;
        for (auto j = 0; j < queue_depth; ++j) {
          PrefetchRequestBatch* batch = new PrefetchRequestBatch();
          batch->keys = new uint64_t[batch_capacity_];
          batch->size = 0;
          shard->prefetch_req_batches.push_back(batch);
        }
        shard->prefetch_req_batch_idx = 0;
        shard->prefetch_req_batch_q = new PrefetchRequestBatchQueue{MESSAGE_QUEUE_SIZE};
        host_view_shards_.push_back(shard);
      }
    } catch (const std::exception& e) {
      throw std::runtime_error("Error parsing prefetcher config: " + std::string(e.what()));
    }
  }
}

bool Prefetcher::PassClientRequest(ClientRequestT req) {
  auto shard_id = req % num_shards_; // TODO: add optimization for 2 
  // LOG_INFOF("shard_id: {}, num_shards_: {}", shard_id, num_shards_);
  auto& shard = host_view_shards_[shard_id];
  ClientRequestBatch* batch = shard->client_req_batches[shard->client_req_batch_idx];
  batch->requests[batch->size++] = req;
  batch->bytes += sizeof(req);
  if (batch->size == batch->capacity) {
    // send the batch to the prefetcher thread
    // LOG_INFOF("batch->size {} == batch->capacity {}", batch->size, batch->capacity);
    shard->client_req_batch_idx = (shard->client_req_batch_idx + 1) % shard->client_req_batches.size();
    if (!shard->client_req_batch_q.try_push(batch)) {
      std::cout << "Failed to push client request batch to prefetcher thread\n";
      return false;
    }
  }
  return true;
}

bool Prefetcher::Flush() {
  for (int i = 0; i < num_shards_; i++) {
    Flush(i);
  }
  return true;
}

bool Prefetcher::Flush(int32_t shard_id) {
  auto& shard = host_view_shards_[shard_id];
  auto batch = shard->client_req_batches[shard->client_req_batch_idx];
  if (batch->size > 0) {
    if (!shard->client_req_batch_q.try_push(batch)) {
      std::cout << "Failed to push client request batch to prefetcher thread\n";
      return false;
    }
  }
  return true;
}

void Prefetcher::BootstrapHostView(uint64_t n)
{
  for (uint64_t i = 0; i < n; ++i) {
    auto shard_id = i % num_shards_;
#ifdef __aarch64__
    host_view_shards_[shard_id]->host_records_.insert(i);
#endif
  }
}

void Prefetcher::BootstrapHostView(const std::vector<uint64_t>& keys)
{
  for (auto i = 0; i < keys.size(); ++i) {
    auto shard_id = keys[i] % num_shards_;
#ifdef __aarch64__
    host_view_shards_[shard_id]->host_records_.insert(keys[i]);
#endif
  }
}

void Prefetcher::SetShardMinMaxKeys(int shard_id, uint64_t min_key, uint64_t max_key) 
{
  if (shard_id >= num_shards_) {
    return;
  }
  auto shard = host_view_shards_[shard_id];
  shard->max_key = max_key;
  shard->min_key = min_key;
  return;
}

bool Prefetcher::HostViewContains(uint64_t key)
{
  auto shard_id = key % num_shards_;
  bool output = false;
#ifdef __aarch64__
  auto it = host_view_shards_[shard_id]->host_records_.find(key);
  output = !it.is_null();
#endif
  return output;
}

std::vector<PrefetchRequestBatchQueue*> Prefetcher::GetPrefetchQueues()
{
  std::vector<PrefetchRequestBatchQueue*> output;
  for (auto shard : host_view_shards_) {
    output.push_back(shard->prefetch_req_batch_q);
  }
  return output;
}

HostViewRefresher* Prefetcher::GetRefresher()
{
  return &refresher_;
}

void Prefetcher::WorkerThreadBatched(int32_t shard_id) {
  std::cout << "Started Prefetcher thread (batched)...\n";

  // Pin this thread to a CPU core (core 8 for example)
  int core_id = 8 + shard_id; 
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(core_id, &cpuset);
  int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
  if (rc != 0) {
    std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
  }

  auto& shard = host_view_shards_[shard_id];

  while (!stop_flag_.load(std::memory_order_relaxed)) {
    bool cpu_pause = true;
    ClientRequestBatch** batch = shard->client_req_batch_q.front();
    if (batch) {
      auto batch_ptr = *batch;
      shard->client_req_batch_q.pop();
      cpu_pause = false;
      ProcessClientRequestBatch(shard, batch_ptr);
    }

    batch = nullptr; 
    batch = shard->refresh_req_batch_q->front();
    if (batch) {
      auto batch_ptr = *batch;
      shard->refresh_req_batch_q->pop();
      cpu_pause = false;
      ProcessRefreshRequestBatch(shard, batch_ptr);
    }
    
    if (cpu_pause) {
      CpuPause();
    }
  }
  std::cout << "Stopped Prefetcher thread (batched)...\n";
}

void Prefetcher::ProcessClientRequestBatch(HostViewShard* shard, ClientRequestBatch* batch) {
  auto prefetch_batch = shard->prefetch_req_batches[shard->prefetch_req_batch_idx];
//   shard->prefetch_req_batch_idx = (shard->prefetch_req_batch_idx + 1) % shard->prefetch_req_batches.size();

// #ifdef __aarch64__
// using dpf::cuckoo_set::MAX_LOOKUP_BATCH_SZ;
//   for (size_t i = 0; i < batch->size; i += MAX_LOOKUP_BATCH_SZ) {
//     auto batch_size = std::min(MAX_LOOKUP_BATCH_SZ, (size_t)(batch->size - i));
//     shard->host_records_.find_batched(batch->requests + i, batch_size, shard->host_records_it_);
//     for (auto j = 0; j < batch_size; j++) {
//       if (shard->host_records_it_[j].is_null()) {
//         prefetch_batch->keys[prefetch_batch->size++] = batch->requests[i + j];
//       }
//     }
//   }
// #endif

//   if (prefetch_batch->size > 0) {
//     auto pushed = shard->prefetch_req_batch_q->try_push(prefetch_batch);
//     if (!pushed) {
//       std::cout << "Failed to push prefetch request batch to output queue\n";
//       return;
//     }
//   }

  if (batch->is_read) [[likely]] {
#if PD3_USE_MINMAX_FILTER
#ifdef __aarch64__
      for (size_t i = 0; i < batch->size; ++i) {
        if (batch->requests[i] > shard->max_key) {
          prefetch_batch->keys[prefetch_batch->size++] = batch->requests[i]; 
        } else {
          auto it = shard->host_records_.find(batch->requests[i]);
          if (it.is_null()) {
            prefetch_batch->keys[prefetch_batch->size++] = batch->requests[i];
          }
        }
      }
#endif
#else 
#ifdef __aarch64__
      using dpf::cuckoo_set::MAX_LOOKUP_BATCH_SZ;
      for (size_t i = 0; i < batch->size; i += MAX_LOOKUP_BATCH_SZ) {
        auto batch_size = std::min(MAX_LOOKUP_BATCH_SZ, (size_t)(batch->size - i));
        shard->host_records_.find_batched(batch->requests + i, batch_size, shard->host_records_it_);
        for (size_t j = 0; j < batch_size; j++) {
          if (shard->host_records_it_[j].is_null()) {
            prefetch_batch->keys[prefetch_batch->size++] = batch->requests[i + j];
          }
        }
      }
#endif
#endif 
      if (prefetch_batch->size > 0) {
#if PD3_HV_ADD_ON_REMOTE_READ
        for (size_t i = 0; i < prefetch_batch->size; ++i) {
#ifdef __aarch64__
          shard->host_records_.insert(prefetch_batch->keys[i]);
#endif
        }
#endif
        shard->prefetch_req_batch_idx = (shard->prefetch_req_batch_idx + 1) % shard->prefetch_req_batches.size();
        auto pushed = shard->prefetch_req_batch_q->try_push(prefetch_batch);
        if (!pushed) {
          LOG_ERROR("Failed to push prefetch request batch to prefetch_req_batch_q_");
        }
      }
  } else {
#if PD3_HV_ADD_ON_CLIENT_WRITE
#ifdef __aarch64__
    for (size_t i = 0; i < batch->size; ++i) {
      shard->host_records_.insert(batch->requests[i]);
#if PD3_USE_MINMAX_FILTER
      shard->max_key = std::max(shard->max_key, batch->requests[i]);
      shard->min_key = std::min(shard->min_key, batch->requests[i]);
#endif
    }
#endif
#endif
  }

  // TODO: the below is behavior on read
  // TODO: we need to add consistency checks here. On detection of an item that isn't in the host view, what do we do? We can add it or let it go
  // on write, we either add to the host view, or do nothing
}

void Prefetcher::ProcessRefreshRequestBatch(HostViewShard* shard, ClientRequestBatch* batch) {
  if (batch->is_read) {
    // handle remote reads here
#if PD3_HV_ADD_ON_REMOTE_READ
#ifdef __aarch64__
    for (size_t i = 0; i < batch->size; ++i) {
      shard->host_records_.insert(batch->requests[i]);
    }
#endif
#endif
  } else {
    // handle remote writes here
#ifdef __aarch64__
    for (size_t i = 0; i < batch->size; ++i) {
#if PD3_USE_MINMAX_FILTER
      if (batch->requests[i] < shard->max_key && batch->requests[i] > shard->min_key) {
        auto it = shard->host_records_.find(batch->requests[i]);
        if (!it.is_null()) {
          shard->host_records_.erase(it);
        }
      }
#else 
      // LOG_INFOF("erasing record i: {}", batch->requests[i]);
      auto it = shard->host_records_.find(batch->requests[i]);
      if (!it.is_null()) {
        shard->host_records_.erase(it);
      }
#endif
    }
#endif
    // TODO: consistency has to be thought through carefully here
    // if the write is applied before the client acknowledges it, then we will run into issues of false positives
    // if the write isn't applied, then we will get false negatives 
    // option 1: client calls check and return for newly evicted data, even before the write completes. falls back to local if not
    // option 2: the refresh batch is only sent when the client has successfully consumed the write ack
  }
}

} // namespace dpf
