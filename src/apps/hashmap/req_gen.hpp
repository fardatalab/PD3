#pragma once

#include <cstddef>
#include <cstdint>
#include <iostream>
#include <random>
#include <memory>

#include "types.hpp"
#include "zipf.hpp"
#include "lru.hpp"

namespace pd3 {

struct ZipfParam {
  uint64_t total_requests = 0;
  bool zipf = false;
};

class ReqGen {

  static constexpr size_t ON_WIRE_REQUEST_SIZE = sizeof(pd3::network_hashmap::Request);

public:
  ReqGen() = default;
  ~ReqGen() = default;

  void Initialize(size_t total_keys, size_t local_keys, ZipfParam param = {});
   
  // request generation methods
  void GenerateTestRequests(char* send_buffer, uint64_t requests_to_send);
  void GenerateRequestsWithMisses(char* send_buffer, uint64_t requests_to_send, uint32_t miss_rate);
  void GenerateYCSBUniformRequests(char* send_buffer, uint64_t requests_to_send, bool leap = false);
  void GenerateYCSBZipfRequests(char* send_buffer, uint64_t requests_to_send, bool leap = false);
  
private:
  
  // uniform
  void GenerateYCSBUniformNoLeap(char* send_buffer, uint64_t requests_to_send);
  void GenerateYCSBUniformWithLeap(char* send_buffer, uint64_t requests_to_send);
  // zipfian
  void GenerateYCSBZipfNoLeap(char* send_buffer, uint64_t requests_to_send);
  void GenerateYCSBZipfWithLeap(char* send_buffer, uint64_t requests);

private:
  
  size_t num_total_keys_;
  size_t num_local_keys_;

  std::unique_ptr<ycsbc::ZipfianGenerator> zipf_;
  std::vector<uint64_t> zipf_requests_;
  std::unique_ptr<LRUCache> lru_;

  bool initialized_ = false;
};

}