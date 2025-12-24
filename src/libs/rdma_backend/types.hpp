#pragma once

#include <cstdint>

namespace dpf {
namespace remote_memory_details {

struct MemoryRegion {
  uint64_t address;
  uint32_t rkey;
}__attribute__((packed));

struct RemoteMemoryDetails {
  uint64_t num_entries;
  MemoryRegion regions[32];
}__attribute__((packed));

struct RemoteMemoryDetailsRequest {
  uint32_t req;
}__attribute__((packed)); 

} // namespace remote_memory_details  
} // namespace dpf
