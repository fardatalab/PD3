#pragma once

#include "PD3/prefetcher/prefetcher.hpp"

#include <cstdint>
#include <unordered_map>

namespace dpf {
namespace user_defined {

class GarnetParser {

  static constexpr size_t BUFFER_SIZE = 1024 * 32;

  struct ClientState {
    char local_buffer[BUFFER_SIZE];  // to accomodate 128 MGET requests
    uint32_t local_buffer_sz = 0;
    uint32_t num_requests = 0;
  };

public:
  
  GarnetParser() = default;
  ~GarnetParser() = default;
  
  void SetPrefetcher(dpf::Prefetcher* prefetcher) {
    prefetcher_ = prefetcher;
  }
  
  void ProcessPacket(uint32_t src_port, uint32_t dst_port, char* payload, uint32_t payload_sz);

  void PrintStatistics();

  void ParseGetRequests(ClientState& client_state);

private:

  dpf::Prefetcher* prefetcher_ = nullptr;

  std::unordered_map<uint32_t, ClientState> client_states_;

};

} // namespace user_defined
} // namespace dpf