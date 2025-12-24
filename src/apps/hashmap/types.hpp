#pragma once

// #include "config.hpp"

#include <cstdint>
#include <cstddef>
#include <cassert>
#include <string>
#include <vector>

namespace pd3 {
namespace network_hashmap {

enum class RequestType : uint8_t {
  kGet = 0,
  kPut = 1,
};

static constexpr uint64_t RECORD_SIZE = 8;
using ValueType = char[RECORD_SIZE];

struct Request {
  RequestType type;
  uint64_t key;
#ifdef MEASURE_LATENCY
  uint64_t timestamp;
#endif
  bool local;
}__attribute__((packed));

struct Response {
  uint64_t key;
#ifdef MEASURE_LATENCY
  uint64_t timestamp;
#endif
  ValueType value;
}__attribute__((packed));

struct MemoryBuffer {
  uint64_t address;
  uint64_t rkey;
}__attribute__((packed));

struct RemoteMemoryDetails {
  uint64_t num_entries;
  MemoryBuffer buffers[16];
}__attribute__((packed));


} // namespace network_hashmap
} // namespace pd3