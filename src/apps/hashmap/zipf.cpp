// #include 

// #include <random>
// #include <unordered_map>
// #include <list>
// #include <cmath>
// #include <iostream>

// #include "benchmarking/leap.hpp"

// #include "types.hpp"

// const size_t ON_WIRE_REQUEST_SIZE = sizeof(pd3::network_hashmap::Request);
// const size_t ON_WIRE_RESPONSE_SIZE = sizeof(pd3::network_hashmap::Response);
// const size_t DATABASE_SIZE = size_t(16) * size_t(1024) * size_t(1024) * size_t(1024);
// const size_t LOCAL_MEMORY_SIZE = size_t(1) * 1024 * 1024 * 1024;
// const size_t VALUE_SIZE = 8;
// const size_t RECORD_SIZE = 16;
// const size_t PAGE_SIZE = 4096;

// /* ---------- very small LRU helper ---------- */
// class LRUCache {
//  public:
//   explicit LRUCache(std::size_t capacity) : cap_(capacity) {}

//   /** @return true  ⇢ cache hit
//    *          false ⇢ cache miss (key inserted) */
//   bool access(uint64_t key) {
//     auto it = map_.find(key);
//     if (it == map_.end()) {                      // miss
//       if (list_.size() == cap_) {                // full → evict LRU (back)
//         map_.erase(list_.back());
//         list_.pop_back();
//       }
//       list_.push_front(key);                     // new becomes MRU
//       map_[key] = list_.begin();
//       return false;
//     }
//     /* hit: move entry to MRU position */
//     list_.splice(list_.begin(), list_, it->second);
//     return true;
//   }

//  private:
//   std::size_t cap_;
//   std::list<uint64_t>                                        list_; // MRU front, LRU back
//   std::unordered_map<uint64_t, std::list<uint64_t>::iterator> map_;
// };

// /* ---------- Zipfian integer generator (θ≈1) ------------- */
// class ZipfGenerator {
//  public:
//   ZipfGenerator(uint64_t n, double theta, uint64_t seed = 0)
//       : n_(n), theta_(theta),
//         zeta_n_(zeta(n_, theta_)),
//         zeta_2_(zeta(2, theta_)),
//         alpha_(1.0 / (1.0 - theta_)),
//         /* ---------- FIX: correct η formula ---------- */
//         eta_((1.0 - std::pow(2.0 / static_cast<double>(n_), 1.0 - theta_)) /
//              (1.0 -  (zeta_2_ / zeta_n_))),
//         uni_(0.0, 1.0),
//         gen_(seed) {}

//   uint64_t next() {
//     double u  = uni_(gen_);
//     double uz = u * zeta_n_;

//     uint64_t k;
//     if (uz < 1.0) {
//       k = 0;
//     } else if (uz < 1.0 + std::pow(0.5, theta_)) {
//       k = 1;
//     } else {
//       k = static_cast<uint64_t>(
//               1 + n_ *
//                   std::pow(eta_ * u - eta_ + 1.0, alpha_));
//       if (k >= n_) k = n_ - 1;       // clamp
//     }
//     return k;                        // 0‑based
//   }

//  private:
//   static double zeta(uint64_t n, double theta) {
//     double sum = 0.0;
//     for (uint64_t i = 1; i <= n; ++i)
//       sum += 1.0 / std::pow(static_cast<double>(i), theta);
//     return sum;
//   }

//   uint64_t n_;
//   double   theta_, zeta_n_, zeta_2_, alpha_, eta_;
//   std::uniform_real_distribution<double> uni_;
//   std::mt19937_64 gen_;
// };

// /* ---------- Zipfian request generator with LRU ---------- */
// void GenerateZipfianRequests(char* send_buffer,
//                              uint64_t  requests_to_send,
//                              double    theta) {
//   uint64_t offset          = 0;
//   const uint64_t num_keys  = DATABASE_SIZE      / RECORD_SIZE;
//   const uint64_t cache_cap = (DATABASE_SIZE * 0.8)  / RECORD_SIZE;
//   const uint64_t num_local_keys = LOCAL_MEMORY_SIZE / RECORD_SIZE;

//   //ZipfGenerator zipf(num_keys, theta, /*seed=*/0);
//   LRUCache      cache(cache_cap);

//   uint64_t total_local = 0, total = 0;

//   // warm up
//   // for (uint64_t i = 0; i < cache_cap; ++i) {
//   //   cache.access(i);
//   // }

//   for (uint64_t i = 0; i < requests_to_send; ++i) {
//     auto* req   = reinterpret_cast<pd3::network_hashmap::Request*>(send_buffer + offset);
//     req->type   = pd3::network_hashmap::RequestType::kGet;
//     req->key = rand() % num_keys;
//     req->local = req->key < cache_cap;
//     // req->local = req->key < num_keys;
//     if (req->local)
//       req->key = req->key % 100000;
//     //req->key    = zipf.next();
//     //req->local  = cache.access(req->key);        // true == cache hit
//     offset     += ON_WIRE_REQUEST_SIZE;
//     // if (req->local) {
//     //   // make sure key is in num local keys
//     //   if (req->key > cache_cap) {
//     //     req->key = req->key % cache_cap;
//     //   }
//     // // if (!req->local) {
//     // //   if (req->key < 10000) req->key += 10000;
//     // // } else {
//     // //   if (req->key > 10000) {
//     // //     req->key = req->key % 10000;
//     // //   }
//     // }

//     if (req->local) ++total_local;
//     ++total;
//   }

//   auto remote_requests = total - total_local;
//   // auto num_to_change = remote_requests % 250;
//   // offset = 0;
//   // if (num_to_change != 0) {
//   //   for (uint64_t i = 0; i < requests_to_send; ++i) {
//   //     auto req = reinterpret_cast<pd3::network_hashmap::Request*>(send_buffer + offset);
//   //     if (!req->local) {
//   //       req->local = true;
//   //       req->key = 10;
//   //       num_to_change--;
//   //       total_local++;
//   //     } 
//   //     if (num_to_change == 0) break;
//   //     offset += ON_WIRE_REQUEST_SIZE;
//   //   }
//   // }
//   // remote_requests = total - total_local;

//   std::cout << "Cache hit ratio: "  << (double)total_local / total
//             << "  Miss ratio: "     << 1.0 - (double)total_local / total
//             << '\n';
// }

// void GenerateZipfianRequestsWithLeap(char* send_buffer,
//                              uint64_t  requests_to_send,
//                              double    theta) {
//   uint64_t offset          = 0;
//   const uint64_t num_keys  = DATABASE_SIZE      / VALUE_SIZE;
//   const uint64_t cache_cap = LOCAL_MEMORY_SIZE  / VALUE_SIZE;

//   ZipfGenerator zipf(num_keys, theta, /*seed=*/0);
//   LRUCache      cache(cache_cap);

//   uint64_t total_local = 0, total = 0;
//   using namespace dpf;
//   benchmark::Leap leap;
//   benchmark::LeapConfig leap_config;
//   leap_config.num_splits = 5;
//   leap_config.max_prefetch_window_size = 10;
//   leap_config.access_history_size = 50;
//   leap_config.page_cache_size = 10000; // 40MB
//   leap.Initialize(leap_config);

//   // warm up
//   for (uint64_t i = 0; i < cache_cap; ++i) {
//     cache.access(i);
//   }

//   for (uint64_t i = 0; i < requests_to_send; ++i) {
//     auto* req   = reinterpret_cast<pd3::network_hashmap::Request*>(send_buffer + offset);
//     req->type   = pd3::network_hashmap::RequestType::kGet;
//     req->key    = zipf.next();
//     req->local  = cache.access(req->key);        // true == cache hit
//     offset     += ON_WIRE_REQUEST_SIZE;

//     uint64_t page_id = (req->key * VALUE_SIZE) / PAGE_SIZE;
//     if (leap.IsPrefetchedCandidate(page_id)) {
//       req->local = true;
//     }
//     leap.ProcessAccess(page_id);

//     if (req->local) ++total_local;
//     ++total;
//   }

//   std::cout << "Cache hit ratio: "  << (double)total_local / total
//             << "  Miss ratio: "     << 1.0 - (double)total_local / total
//             << '\n';
// }