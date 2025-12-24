#include "garnet.hpp"

#include "PD3/system/logger.hpp"

#include <cstring>
#include <iostream>
#include <charconv>

namespace dpf {
namespace user_defined {

void GarnetParser::ProcessPacket(uint32_t src_port, uint32_t dst_port, char* payload, uint32_t payload_sz) 
{
  if (payload_sz == 0) {
    return;
  }
  if (client_states_.find(src_port) == client_states_.end()) {
    client_states_[src_port] = ClientState();
  }
  auto& client_state = client_states_[src_port];
  auto remaining_bytes = BUFFER_SIZE - client_state.local_buffer_sz;

  if (payload_sz > remaining_bytes) {
    LOG_ERROR("GarnetParser::ProcessPacket: payload_sz > remaining_bytes");
    return;
  }

  std::memcpy(client_state.local_buffer + client_state.local_buffer_sz, payload, payload_sz);
  client_state.local_buffer_sz += payload_sz;
  ParseGetRequests(client_state);
  return;
}

void GarnetParser::ParseGetRequests(ClientState& client_state) 
{
  char* buffer = client_state.local_buffer;
  uint32_t buffer_size = client_state.local_buffer_sz;
  uint32_t parsed_bytes = 0;
  
  // Fixed RESP format for GET with 8-char hex key:
  // *2\r\n$3\r\nGET\r\n$8\r\n<8_hex_chars>\r\n
  // Total length: 27 bytes
  static constexpr uint32_t COMMAND_LENGTH = 27;
  static constexpr char EXPECTED_PREFIX[] = "*2\r\n$3\r\nGET\r\n$8\r\n";
  static constexpr uint32_t PREFIX_LENGTH = 17;
  static constexpr uint32_t HEX_KEY_OFFSET = 17;
  static constexpr uint32_t HEX_KEY_LENGTH = 8;
  
  while (parsed_bytes + COMMAND_LENGTH <= buffer_size) {
    char* current_pos = buffer + parsed_bytes;
    
    // Fast prefix check using memcmp
    if (std::memcmp(current_pos, EXPECTED_PREFIX, PREFIX_LENGTH) != 0) {
      // Not a valid GET command, skip one byte and continue
      parsed_bytes++;
      continue;
    }
    
    // Check suffix \r\n
    if (current_pos[25] != '\r' || current_pos[26] != '\n') {
      parsed_bytes++;
      continue;
    }
    
    // Fast hex to uint64_t conversion
    char* hex_start = current_pos + HEX_KEY_OFFSET;
    uint64_t key_value = *reinterpret_cast<uint64_t*>(hex_start);
    
    // Successfully parsed a GET request
    client_state.num_requests++;
    parsed_bytes += COMMAND_LENGTH;
    prefetcher_->PassClientRequest(key_value);
    
    continue;
    
skip_command:
    parsed_bytes++;
  }
  
  // Compact the buffer by removing parsed data
  if (parsed_bytes > 0) {
    uint32_t remaining_data = buffer_size - parsed_bytes;
    if (remaining_data > 0) {
      std::memmove(buffer, buffer + parsed_bytes, remaining_data);
    }
    client_state.local_buffer_sz = remaining_data;
  }
}

void GarnetParser::PrintStatistics() {
  for (auto& client_state : client_states_) {
    std::cout << "Client " << client_state.first << " processed " << client_state.second.num_requests << " requests" << std::endl;
  }
}

} // namespace user_defined
} // namespace dpf