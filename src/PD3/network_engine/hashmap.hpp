#pragma once

#include "PD3/prefetcher/prefetcher.hpp"

#include <cstdint>
#include <unordered_map>

namespace dpf {
namespace user_defined {

  struct HashmapRequest {
    uint8_t type;
    uint64_t key;
    bool local;
  }__attribute__((packed));

class HashmapParser {

  struct ClientState {
    char local_buffer[sizeof(HashmapRequest)];
    uint32_t local_buffer_sz = 0;
    uint32_t num_requests = 0;
  };

public:
  HashmapParser() = default;
  ~HashmapParser() = default;

  void SetPrefetcher(dpf::Prefetcher* prefetcher) {
    prefetcher_ = prefetcher;
  }

  void ProcessPacket(uint32_t src_port, uint32_t dst_port, char* payload, uint32_t payload_sz);

  void PrintStatistics();

private:

  std::unordered_map<uint32_t, ClientState> client_states_;
  dpf::Prefetcher* prefetcher_ = nullptr;

};

} // namespace user_defined
} // namespace dpf