#include "transfer_client.hpp"

#include "common/doca_common.hpp"

#include "config.h"
#include "PD3/system/logger.hpp"

#include <iostream>

#include <doca_buf.h>
#include <doca_buf_inventory.h>
#include <doca_dma.h>
#include <doca_error.h>
#include <doca_log.h>
#include <doca_mmap.h>

namespace dpf {
namespace offload {

[[nodiscard]] constexpr std::size_t round_up_64(std::size_t n) noexcept
{
  return (n + 63u) & ~std::size_t{63u};   // add 63, then clear lower 6 bits
}

TransferClient::~TransferClient() {
  host_destroy_core_objects(&state_);
  free(raw_memory_);
}

static doca_error_t
save_config_info_to_files(const void *export_desc, size_t export_desc_len, const char *src_buffer, size_t src_buffer_len,
			  const char *export_desc_file_path, const char *buffer_info_file_path)
{
	FILE *fp;
	uint64_t buffer_addr = (uintptr_t)src_buffer;
	uint64_t buffer_len = (uint64_t)src_buffer_len;

	fp = fopen(export_desc_file_path, "wb");
	if (fp == NULL) {
		// DOCA_LOG_ERR("Failed to create the DMA copy file");
		return DOCA_ERROR_IO_FAILED;
	}

	if (fwrite(export_desc, 1, export_desc_len, fp) != export_desc_len) {
		// DOCA_LOG_ERR("Failed to write all data into the file");
		fclose(fp);
		return DOCA_ERROR_IO_FAILED;
	}

	fclose(fp);

	fp = fopen(buffer_info_file_path, "w");
	if (fp == NULL) {
		// DOCA_LOG_ERR("Failed to create the DMA copy file");
		return DOCA_ERROR_IO_FAILED;
	}

	fprintf(fp, "%" PRIu64 "\n", buffer_addr);
	fprintf(fp, "%" PRIu64 "", buffer_len);

	fclose(fp);

	return DOCA_SUCCESS;
}

bool TransferClient::Initialize(const TransferClientConfig& config)
{
  const void* export_desc;
  size_t export_desc_len;
  char* buf_details;
  size_t buf_details_size;
  doca_error_t result;

  auto req_buffer_size = sizeof(dpf::buffer::RequestBuffer);
  auto final_req_buf_size = round_up_64(req_buffer_size);
  auto resp_buffer_size = sizeof(dpf::buffer::ResponseBuffer);
  auto final_resp_buf_size = round_up_64(resp_buffer_size);

  auto total_size = final_req_buf_size + final_resp_buf_size;

  char* src_buffer = (char*)malloc(total_size);

	/* Open the relevant DOCA device */
	result = open_doca_device_with_pci(config.pcie_address.c_str(), NULL, &state_.dev);
	if (result != DOCA_SUCCESS) {
    LOG_ERRORF("error opening device: {}", doca_error_get_name(result));
    return false;
  }

	/* Init all DOCA core objects */
	result = host_init_core_objects(&state_);
	if (result != DOCA_SUCCESS) {
		host_destroy_core_objects(&state_);
		return false;
	}

	/* Allow exporting the mmap to DPU for read only operations */
	result = doca_mmap_set_permissions(state_.src_mmap, DOCA_ACCESS_FLAG_PCI_READ_WRITE);
	if (result != DOCA_SUCCESS) {
    LOG_ERRORF("error setting mmap permissions: {}", doca_error_get_name(result));
		host_destroy_core_objects(&state_);
		return false;
	}

	/* Populate the memory map with the allocated memory */
	result = doca_mmap_set_memrange(state_.src_mmap, src_buffer, total_size);
	if (result != DOCA_SUCCESS) {
    LOG_ERRORF("error setting memrange: {}", doca_error_get_name(result));
		return false;
	}
	result = doca_mmap_start(state_.src_mmap);
	if (result != DOCA_SUCCESS) {
    LOG_ERRORF("error starting mmap: {}", doca_error_get_name(result));
		return false;
	}

  result = doca_mmap_export_pci(state_.src_mmap, state_.dev, &export_desc, &export_desc_len);
	if (result != DOCA_SUCCESS) {
		return false;
	}

  save_config_info_to_files(export_desc, 
                            export_desc_len, 
                            src_buffer, total_size,
                            config.mmap_export_path.c_str(), 
                            config.buf_details_path.c_str());
  
  // initialize the request and response buffers
	auto req_buffer_start = src_buffer;
	request_buffer_ = new (req_buffer_start) dpf::buffer::RequestBuffer();
	request_buffer_->Initialize();

	// TODO: make this cleaner
	completion_buffer_ = new char[8192 * 8];
	completion_buffer_size_ = 8192 * 8;

  return true;
}

bool TransferClient::SubmitRequest(TransferRequest& request, const char* data, size_t data_size)
{
	if (request.is_read) {
		return request_buffer_->Produce(reinterpret_cast<const char*>(&request), sizeof(request));
	} else {
		return request_buffer_->Produce(reinterpret_cast<const char*>(&request), data, sizeof(request), data_size);
	}
}

CompletionOutput TransferClient::PollCompletions()
{
  CompletionOutput output;
  if (last_read_size_ <= completion_buffer_pos_ or refresh_needed_) {
    auto producer_idx = response_buffer_->producer_index();
    if (producer_idx == consumer_idx_) {
      return output;
    }
    size_t data_to_read = 0;
    if (producer_idx < consumer_idx_) {
      auto buf_end_bytes = PD3_RING_BUFFER_SIZE - consumer_idx_;
      data_to_read = buf_end_bytes + producer_idx;
      std::memcpy(completion_buffer_, response_buffer_->buffer() + consumer_idx_, buf_end_bytes);
			std::memcpy(completion_buffer_ + buf_end_bytes, response_buffer_->buffer(), producer_idx);
			completion_buffer_pos_ = 0;
			last_read_size_ = data_to_read;
    } else {
      data_to_read = producer_idx - consumer_idx_;
			std::memcpy(completion_buffer_, response_buffer_->buffer() + consumer_idx_, data_to_read);
			// check whether this is 0
			auto t_resp = reinterpret_cast<TransferResponse*>(completion_buffer_);
			if (t_resp->aligned_bytes == 0) {
				return output;
			}
			completion_buffer_pos_ = 0;
			last_read_size_ = data_to_read;
    }
    refresh_needed_ = false;
  }
  auto response = reinterpret_cast<TransferResponse*>(completion_buffer_ + completion_buffer_pos_);
	if (response->aligned_bytes == 0) {
		refresh_needed_ = true;
		return output;
	}
	output.response = response;
	if (output.response->is_read) {
		output.data = completion_buffer_ + completion_buffer_pos_ + sizeof(TransferResponse);
	} else {
		output.data = nullptr;
	}
	completion_buffer_pos_ += output.response->aligned_bytes;
	consumer_idx_ += output.response->aligned_bytes;
	return output;
}

} // namespace offload
} // namespace dpf