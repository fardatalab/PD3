#include "hashmap.hpp"

#include <cstring>
#include <iostream>

namespace dpf {
namespace user_defined {

void HashmapParser::ProcessPacket(uint32_t src_port, uint32_t dst_port, char* payload, uint32_t payload_sz) {
  if (payload_sz == 0) {
    return;
  }
  if (client_states_.find(src_port) == client_states_.end()) {
    client_states_[src_port] = ClientState();
  }
  auto& client_state = client_states_[src_port];
  auto requests_in_msg = (payload_sz + client_state.local_buffer_sz) / sizeof(HashmapRequest);
  // first request
  size_t offset = 0;
  size_t curr_requests = 0;
  if (client_state.local_buffer_sz != 0) {
    HashmapRequest req;
    std::memcpy(&req, client_state.local_buffer, client_state.local_buffer_sz);
    std::memcpy((char*)&req + client_state.local_buffer_sz, payload, sizeof(HashmapRequest) - client_state.local_buffer_sz);
    offset = sizeof(HashmapRequest) - client_state.local_buffer_sz;
    client_state.local_buffer_sz = 0;
    curr_requests++;
    if (req.key > 268435456) {
      std::cout << "Key: " << req.key << " is greater than 268435456 inside" << std::endl;
    }
    prefetcher_->PassClientRequest(req.key);
  }

  while (curr_requests < requests_in_msg) {
    auto req = (HashmapRequest*)(payload + offset);
    offset += sizeof(HashmapRequest);
    if (req->key > 268435456) {
      std::cout << "Key: " << req->key << " is greater than 268435456" << std::endl;
    }
    prefetcher_->PassClientRequest(req->key);
    curr_requests++;
  }
  client_state.local_buffer_sz = payload_sz - offset;
  if (client_state.local_buffer_sz > 0) {
    std::memcpy(client_state.local_buffer, payload + offset, client_state.local_buffer_sz);
  }
  client_state.num_requests += curr_requests;
}

void HashmapParser::PrintStatistics() {
  for (auto& [src_port, client_state] : client_states_) {
    std::cout << "Client " << src_port << " received and processed " << client_state.num_requests << " requests\n";
  }
}

} // namespace user_defined
} // namespace dpf