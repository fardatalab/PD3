#pragma once

#include <cstdint>
#include <infiniband/verbs.h>

#include "PD3/buffer/types.hpp"

namespace dpf {
namespace offload {

enum class OffloadRequestStatus : uint16_t {
  Pending,
  Completed,
  Error
};

enum class OffloadRequestOp : uint8_t {
  Read,
  Write,
  CreateSegment,
  RemoveSegment,
  RemoveAll
};

inline const char* offload_request_op_to_str(OffloadRequestOp op)
{
  switch (op) {
    case OffloadRequestOp::Read:
      return "Read";
    case OffloadRequestOp::Write:
      return "Write";
    case OffloadRequestOp::CreateSegment:
      return "CreateSegment";
    case OffloadRequestOp::RemoveSegment:
      return "RemoveSegment";
    case OffloadRequestOp::RemoveAll:
      return "RemoveAll";
    default:
      return "Unknown";
  }
}

struct OffloadRequest {
  OffloadRequestOp op;  // operation
  uint16_t req_id;  // request id
  uint32_t length;  // in case of read, number of bytes to read, in case of write, number of bytes following the request header
  uint64_t remote_offset;  // offset within the segment for a read or write
  uint64_t segment;  // segment id for the write
};

// Slightly better laid out
struct TransferRequest {
  uint64_t address;
  uint32_t bytes;
  uint16_t req_id;
  bool is_read;
};

static constexpr size_t kTransferRequestSize = sizeof(TransferRequest);

struct TransferResponse {
  uint32_t aligned_bytes;
  uint32_t bytes;
  uint16_t req_id;
  bool is_read;
};

static constexpr size_t kTransferResponseSize = sizeof(TransferRequest);

struct PrefetchRequest {
  uint64_t key;
};

static constexpr size_t kPrefetchRequestSize = sizeof(PrefetchRequest);

struct PrefetchResponse {
  uint64_t size;
  uint64_t consumer_state;
  uint64_t key;
};

static constexpr size_t kPrefetchResponseSize = sizeof(PrefetchResponse);

static constexpr size_t kOffloadRequestSize = sizeof(OffloadRequest);

struct OffloadResponse {
  OffloadRequestOp op;  // operation
  uint16_t req_id;  // request id
  OffloadRequestStatus status;  // status of the request
  uint32_t length; // length of the data following the response header
};

struct RemoteSegmentRequest {
  OffloadRequestOp op;
  uint64_t segment;
}__attribute__((packed));

struct RemoteSegmentResponse {
  OffloadRequestOp op;
  OffloadRequestStatus status;
  uint32_t remote_key;
  uint64_t segment;
  uint64_t address;
  uint64_t offset;
}__attribute__((packed));

struct ReqSlotHeader {
  uint64_t resp_addr;
  uint32_t num_requests;
};

struct RespSlotHeader {
  uint32_t num_requests;
  uint64_t batch_size;
};


} // namespace offload
} // namespace dpf