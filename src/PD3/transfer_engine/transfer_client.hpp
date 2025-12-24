#pragma once

#include "types.hpp"

#include "common/doca_common.hpp"
// buffers
#include "PD3/buffer/response_buffer.hpp"
#include "PD3/buffer/request_buffer.hpp"

#include <string>

namespace dpf {
namespace offload {

struct TransferClientConfig {
  std::string pcie_address;
  std::string mmap_export_path;
  std::string buf_details_path;
};

struct CompletionOutput {
  TransferResponse* response;
  char* data;
};

class TransferClient 
{

public:

  TransferClient() noexcept = default;
  ~TransferClient();

  bool Initialize(const TransferClientConfig& config);

  /// @brief Submits an offload request for memory disaggregation
  /// @param request request header structure
  /// @param data (optional) data to be written to the remote memory
  /// @param data_size (optional) size of the data to be written to the remote memory
  /// @return request id
  bool SubmitRequest(TransferRequest& request, const char* data, size_t data_size);

  CompletionOutput PollCompletions();

  dpf::buffer::RequestBuffer* request_queue() const { return request_buffer_; }
  dpf::buffer::ResponseBuffer* response_queue() const { return response_buffer_; }

private:
  
  dpf::buffer::RequestBuffer* request_buffer_;
  dpf::buffer::ResponseBuffer* response_buffer_;

  char* raw_memory_;
  size_t total_size;

  char* completion_buffer_;
  size_t completion_buffer_size_ = 0;
  size_t completion_buffer_pos_ = 0;
  size_t last_read_size_ = 0;
  size_t consumer_idx_ = 0;
  bool refresh_needed_ = false;

  AppState state_;

};

} // namespace offload
} // namespace dpf