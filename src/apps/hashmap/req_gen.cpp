#include "req_gen.hpp"
#include "types.hpp"

#include "benchmarking/leap.hpp"

namespace pd3
{

void ReqGen::Initialize(size_t total_keys, size_t local_keys, ZipfParam param) {
  if (initialized_) {
    std::cerr << "ReqGen: Already initialized" << std::endl;
    return;
  }
  if (param.zipf) {
    zipf_ = std::make_unique<ycsbc::ZipfianGenerator>(total_keys);
    lru_ = std::make_unique<LRUCache>(local_keys);
    // warm up cache
    for (int i = 0; i < local_keys; ++i) {
      lru_->access(i);
    }
    // generate zipf sequence
  }
  num_total_keys_ = total_keys;
  num_local_keys_ = local_keys;
  initialized_ = true;
  return;
}

void ReqGen::GenerateTestRequests(char* send_buffer, uint64_t requests_to_send) {
  uint64_t offset = 0;
  srand(0);
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis(0, num_total_keys_ - 1);
  uint64_t total_local = 0;
  uint64_t total = 0;
  for (uint64_t i = 0; i < requests_to_send; ++i) {
    auto req = reinterpret_cast<pd3::network_hashmap::Request*>(send_buffer + offset);
    req->type = pd3::network_hashmap::RequestType::kGet;
    req->key = dis(gen);
    req->local = req->key < num_local_keys_;
    offset += ON_WIRE_REQUEST_SIZE;
    if (req->local) total_local++;
    total++;
  }
  double hit_ratio = (double)total_local / total;
  double miss_ratio = 1 - hit_ratio;
  std::cout << "Cache miss ratio: " << hit_ratio << " Miss ratio: " << miss_ratio << '\n';
  return;
}

void ReqGen::GenerateRequestsWithMisses(char* send_buffer, uint64_t requests_to_send, uint32_t miss_rate)
{
  uint64_t offset = 0;
  srand(0);
  uint64_t threshold = (1 - (double)miss_rate/100) * num_total_keys_;
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis(0, num_total_keys_ - 1);
  uint64_t total_local = 0;
  uint64_t total = 0;
  for (uint64_t i = 0; i < requests_to_send; ++i) {
    auto req = reinterpret_cast<pd3::network_hashmap::Request*>(send_buffer + offset);
    req->type = pd3::network_hashmap::RequestType::kGet;
    req->key = dis(gen);
    req->local = req->key < threshold;
    if (req->local) {
      req->key = req->key % num_local_keys_; // TODO: without this, segfault on server
    }
    offset += ON_WIRE_REQUEST_SIZE;
    if (req->local) total_local++;
    total++;
  }
  auto remote_requests = total - total_local;
}

void ReqGen::GenerateYCSBUniformRequests(char* send_buffer, uint64_t requests_to_send, bool leap)
{
  if (leap) {
    GenerateYCSBUniformWithLeap(send_buffer, requests_to_send);
  } else {
    GenerateYCSBUniformNoLeap(send_buffer, requests_to_send);
  }
  return;
}

void ReqGen::GenerateYCSBZipfRequests(char* send_buffer, uint64_t requests_to_send, bool leap)
{
  if (leap) {
    GenerateYCSBZipfWithLeap(send_buffer, requests_to_send);
  } else {
    GenerateYCSBZipfNoLeap(send_buffer, requests_to_send);
  }
  return;
}

void ReqGen::GenerateYCSBUniformNoLeap(char* send_buffer, uint64_t requests_to_send)
{
  std::cout << "Generating: " << requests_to_send << " requests\n";
  uint64_t offset = 0;
  // srand(0);
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<> dis(0, num_total_keys_ - 1);
  uint64_t total_local = 0;
  uint64_t total = 0;
  for (uint64_t i = 0; i < requests_to_send; ++i) {
    auto req = reinterpret_cast<pd3::network_hashmap::Request*>(send_buffer + offset);
    req->type = pd3::network_hashmap::RequestType::kGet;
    req->key = dis(gen);
    req->local = req->key < num_local_keys_;
    offset += ON_WIRE_REQUEST_SIZE;
    if (req->local) total_local++;
    total++;
  }
  auto remote_requests = total - total_local;
  double hit_ratio = (double)total_local / total;
  double miss_ratio = 1 - hit_ratio;
  std::cout << "Cache Hit ratio: " << hit_ratio << " Miss ratio: " << miss_ratio << '\n';
  return;
}

void ReqGen::GenerateYCSBUniformWithLeap(char* send_buffer, uint64_t requests_to_send)
{
  // TODO
}

void ReqGen::GenerateYCSBZipfNoLeap(char* send_buffer, uint64_t requests_to_send)
{
  if (!zipf_) {
    throw std::runtime_error("zipf_ parameter not set during initialization");
  }
  uint64_t offset = 0;
  uint64_t total_local = 0;
  uint64_t total = 0;
  for (uint64_t i = 0; i < requests_to_send; ++i) {
    auto req = reinterpret_cast<pd3::network_hashmap::Request*>(send_buffer + offset);
    req->type = pd3::network_hashmap::RequestType::kGet;
    auto key = zipf_->Next();
    if (lru_->access(key)) {
      req->local = true;
      req->key = key % num_local_keys_;
    } else {
      req->local = false;
      if (key < num_local_keys_) {
        key += num_local_keys_;
      }
      req->key = key;
    }
    offset += ON_WIRE_REQUEST_SIZE;
    if (req->local) total_local++;
    total++;
  }
  auto remote_requests = total - total_local;
  double hit_ratio = (double)total_local / total;
  double miss_ratio = 1 - hit_ratio;
  std::cout << "Cache Hit Ratio: " << hit_ratio << " Miss Ratio: " << miss_ratio << '\n';
  return; 
}

void ReqGen::GenerateYCSBZipfWithLeap(char* send_buffer, uint64_t requests_to_send)
{
  std::cout << "Generating YCSB workload with LEAP\n";
  dpf::benchmark::Leap leap;
  dpf::benchmark::LeapConfig leap_config;
  leap_config.num_splits = 5;
  leap_config.max_prefetch_window_size = 10;
  leap_config.access_history_size = 50;
  leap_config.page_cache_size = 20000; // 80MB
  leap.Initialize(leap_config);
  
  uint64_t offset = 0;
  uint64_t total_local = 0;
  uint64_t total = 0;
  for (uint64_t i = 0; i < requests_to_send; ++i) {
    auto* req   = reinterpret_cast<pd3::network_hashmap::Request*>(send_buffer + offset);
    req->type   = pd3::network_hashmap::RequestType::kGet;
    req->key    = zipf_->Next();
    req->local  = lru_->access(req->key);        // true == cache hit
    offset     += ON_WIRE_REQUEST_SIZE;

    uint64_t page_id = (req->key * 16) / 4096; // TODO: fix both
    if (leap.IsPrefetchedCandidate(page_id)) {
      req->local = true;
    }
    leap.ProcessAccess(page_id);

    if (req->local) {
      req->key = req->key % num_local_keys_;
    } else {
      if (req->key < num_local_keys_) {
        req->key += num_local_keys_;
      }
    }

    if (req->local) ++total_local;
    ++total;
  }
  auto remote_requests = total - total_local;
  double hit_ratio = (double)total_local / total;
  double miss_ratio = 1 - hit_ratio;
  std::cout << "Cache Hit Ratio: " << hit_ratio << " Miss Ratio: " << miss_ratio << '\n';
}
  
} // namespace pd3
