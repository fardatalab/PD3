/**
 * Contains the protocol definitions for the RDNMA client <> server
 */
#pragma once

#include <cstdint>

namespace dpf {

enum class RequestHeader : uint32_t {
  // init request from client to server
  C2S_INIT = 0x01,

  // memory regions exchange
  S2C_MEMORY_REGIONS = 0x02,
  C2S_MEMORY_REGIONS = 0x03,

  // batch requests
  C2S_BATCH_REQUEST = 0x04,

  // transfer engine requests
  C2S_TRANSFER_REQUEST = 0x05,
  C2S_PREFETCH_REQUEST = 0x06,

  // one sided requests
  C2S_ONE_SIDED_REQUEST = 0x07
};

inline const char* request_header_str(RequestHeader header) {
  switch (header) {
    case RequestHeader::C2S_INIT:
      return "C2S_INIT";
    case RequestHeader::S2C_MEMORY_REGIONS:
      return "S2C_MEMORY_REGIONS";
    case RequestHeader::C2S_MEMORY_REGIONS:
      return "C2S_MEMORY_REGIONS";
    case RequestHeader::C2S_BATCH_REQUEST:
      return "C2S_BATCH_REQUEST";
    default:
      return "UNKNOWN";
  }
}

struct ProtocolHeader {
  RequestHeader header;
};

// reply from the server to the client for the data regions
struct ReplyMemoryRegions {
  uint32_t num_regions;
};

struct RegionDetails {
  uint64_t base_address;
  uint64_t size;
  uint32_t region_rkey;
};

// reply from the server to the client with receive buffer details
struct ReplyReceiveBuffers {
  uint32_t num_slots;
  uint32_t buffer_rkey;
};

struct BatchIORequest {
  uint32_t num_requests;
  uint64_t local_request_ptr;  // state that is carried in each operation, this is copied back into the response
};

using BatchIOResponse = BatchIORequest;

struct SingleIORequest {
  uint64_t address;
  uint32_t bytes;
  bool is_read;
};

}