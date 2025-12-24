#include "catch2/catch_test_macros.hpp"

#include <iostream>
#include <cstring>

size_t ParseGetRequests(char* buffer, size_t buffer_size) 
{
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
      // LOG_ERROR("GarnetParser::ParseGetRequests: Not a valid GET command");
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
    parsed_bytes += COMMAND_LENGTH;
    std::cout << "Parsed key: " << key_value << std::endl;
    continue;
    
skip_command:
    parsed_bytes++;
  }
  
  return parsed_bytes;
}


TEST_CASE("Garnet Parsing", "[garnet]") {
  char payload[] = "*2\r\n$3\r\nGET\r\n$8\r\n01B27889\r\n";
  size_t payload_sz = 27;
  size_t parsed_bytes = ParseGetRequests(payload, payload_sz);
  CHECK(parsed_bytes == 27);
}

TEST_CASE("Garnet Parsing Erroneous", "[garnet]") {
  char payload[] = "*2\r\n$3\r\nINET\r\n$8\r\n00000000\r\n";
  size_t payload_sz = 28;
  size_t parsed_bytes = ParseGetRequests(payload, payload_sz);
  CHECK(parsed_bytes == 2);
}