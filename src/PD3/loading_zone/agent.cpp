#include "agent.hpp"

#include "PD3/system/logger.hpp"

#include <doca_buf.h>
#include <doca_buf_inventory.h>
#include <doca_dma.h>
#include <doca_error.h>
#include <doca_log.h>
#include <doca_mmap.h>

namespace dpf {
namespace agent {

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

[[nodiscard]] constexpr std::size_t round_up_64(std::size_t n) noexcept
{
  return (n + 63u) & ~std::size_t{63u};   // add 63, then clear lower 6 bits
}


void Agent::Initialize(const AgentConfig& config) {
  const void* export_desc;
  size_t export_desc_len;
  char* buf_details;
  size_t buf_details_size;
  doca_error_t result;

  auto agent_buffer_size = sizeof(dpf::buffer::AgentBuffer);
  auto final_agent_buf_size = round_up_64(agent_buffer_size);

  char* src_buffer = (char*)malloc(final_agent_buf_size);

  result = dpf_open_doca_device_with_pci(config.pcie_address.c_str(), NULL, &state_.dev);
  if (result != DOCA_SUCCESS) {
    LOG_ERRORF("error opening device: {}", doca_error_get_name(result));
    return;
  }

  /* Init all DOCA core objects */
  result = dpf_host_init_core_objects(&state_);
  if (result != DOCA_SUCCESS) {
    LOG_ERRORF("error initializing DOCA core objects: {}", doca_error_get_name(result));
    return;
  }

  /* Allow exporting the mmap to DPU for read write operations */
  result = doca_mmap_set_permissions(state_.src_mmap, DOCA_ACCESS_FLAG_PCI_READ_WRITE);
  if (result != DOCA_SUCCESS) {
    LOG_ERRORF("error setting mmap permissions: {}", doca_error_get_name(result));
    dpf_host_destroy_core_objects(&state_);
    return;
  }

  /* Populate the memory map with the allocated memory */
  result = doca_mmap_set_memrange(state_.src_mmap, src_buffer, final_agent_buf_size);
  if (result != DOCA_SUCCESS) {
    LOG_ERRORF("error setting memrange: {}", doca_error_get_name(result));
    dpf_host_destroy_core_objects(&state_);
    return;
  }

  result = doca_mmap_start(state_.src_mmap);
  if (result != DOCA_SUCCESS) {
    LOG_ERRORF("error starting mmap: {}", doca_error_get_name(result));
    dpf_host_destroy_core_objects(&state_);
    return;
  }

  result = doca_mmap_export_pci(state_.src_mmap, state_.dev, &export_desc, &export_desc_len);
  if (result != DOCA_SUCCESS) {
    LOG_ERRORF("error exporting mmap: {}", doca_error_get_name(result));
    dpf_host_destroy_core_objects(&state_);
    return;
  }

  save_config_info_to_files(export_desc, 
                            export_desc_len, 
                            src_buffer, final_agent_buf_size,
                            config.mmap_export_path.c_str(), 
                            config.buf_details_path.c_str());

  agent_buffer_ = new (src_buffer) dpf::buffer::AgentBuffer();
  agent_buffer_->Initialize();

  return;
  // char* export_desc;
  // size_t export_desc_size;
  // char* buf_details;
  // size_t buf_details_size;

  // struct doca_pci_bdf pcie_bdf = {0};
  // if (parse_pci_addr(config.pcie_address.c_str(), &pcie_bdf) != DOCA_SUCCESS) {
  //   LOG_ERRORF("Failed to parse PCIe address: {}", config.pcie_address);
  //   return;
  // }

  // auto ring_buffer_size = sizeof(dpf::buffer::AgentBuffer);
  // auto mem_region = reinterpret_cast<char*>(malloc(ring_buffer_size));
  // if (!mem_region) {
  //   LOG_ERRORF("Failed to allocate memory for ring buffer");
  //   return;
  // }
  
  // mem_region_ = mem_region;
  // agent_buffer_ = new (mem_region) dpf::buffer::AgentBuffer();
  // agent_buffer_->Initialize();
  
  // doca_error_t result = open_doca_device_with_pci(&pcie_bdf, nullptr, &state_.dev);
  // if (result != DOCA_SUCCESS) {
  //   LOG_ERRORF("Failed to open DOCA device: {}", doca_get_error_string(result));
  //   return;
  // }

  // result = host_init_core_objects(&state_);
  // if (result != DOCA_SUCCESS) {
  //   LOG_ERRORF("Failed to initialize DOCA core objects: {}", doca_get_error_string(result));
  //   return;
  // }
  
  // result = doca_mmap_populate(state_.mmap, mem_region, ring_buffer_size, sysconf(_SC_PAGESIZE), nullptr, nullptr);
  // if (result != DOCA_SUCCESS) {
  //   LOG_ERRORF("Failed to populate DOCA memory map: {}", doca_get_error_string(result));
  //   return;
  // }

  // result = doca_mmap_export(state_.mmap, state_.dev, (void**)&export_desc, &export_desc_size);
  // if (result != DOCA_SUCCESS) {
  //   LOG_ERRORF("Failed to export DOCA memory map: {}", doca_get_error_string(result));
  //   return;
  // }

  // std::cout << "Please copy " << config.mmap_export_path << " and " << config.buf_details_path << " to the DPU for the Prefetcher\n";
  // FILE* fp;
  // uint64_t req_buffer_addr = (uint64_t)mem_region_;
  // uint64_t req_buffer_len = ring_buffer_size;

  // // write the export descriptor file
  // fp = fopen(config.mmap_export_path.c_str(), "wb");
  // if (fp == NULL) {
  //   LOG_ERRORF("Failed to create the DMA copy file: {}", config.mmap_export_path);
  //   return;
  // }
  // if (fwrite(export_desc, 1, export_desc_size, fp) != export_desc_size) {
  //   LOG_ERRORF("Failed to write all data into the file: {}", config.mmap_export_path);
  //   fclose(fp);
  //   return;
  // }
  // fclose(fp);

  // // write the buffer details file
  // fp = fopen(config.buf_details_path.c_str(), "w");
  // if (fp == NULL) {
  //   LOG_ERRORF("Failed to create the DMA copy file: {}", config.buf_details_path);
  //   return;
  // }
  // fprintf(fp, "%" PRIu64 "\n", req_buffer_addr);
  // fprintf(fp, "%" PRIu64 "", req_buffer_len);
  // fclose(fp);
}

bool Agent::FindInCache(const uint64_t& remote_addr, char*& data) {
  // Note: this is version 1 where we do not store a local index
  // with a local index, we would just consume from the end of the queue and place in the local index
  // once hit, we'd remove it
  auto consumer_idx = agent_buffer_->consumer_index();
  auto producer_idx = agent_buffer_->producer_index();

  while (consumer_idx < producer_idx) {
    auto record = (reinterpret_cast<char*>(malloc(1024))); // TODO: fix this
    auto record_header = reinterpret_cast<dpf::buffer::RecordHeader*>(record);
    auto size = record_header->size;
    if (record_header->consumer_state.load(std::memory_order_acquire) == dpf::buffer::CONSUMER_STATE_WRITTEN) {
      // this has not been consumed yet
      auto client_key = reinterpret_cast<uint64_t*>(record + sizeof(buffer::RecordHeader));
      if (*client_key == remote_addr) {
        // we need to try to claim this
        auto expected_state = dpf::buffer::CONSUMER_STATE_WRITTEN;
        if (record_header->consumer_state.compare_exchange_weak(expected_state, 
                                                                dpf::buffer::CONSUMER_STATE_COMPLETED, 
                                                                std::memory_order_acquire,
                                                                std::memory_order_relaxed)) {
          // we have claimed this record
          data = record + sizeof(dpf::buffer::RecordHeader);
          return true;
        }
        // someone else claimed this record, we continue
      }
    }
    consumer_idx += record_header->size;
  }
  return false;
}

// TODO: evaluate the case whether we want to support "freeing" the cache memory 

} // namespace agent
} // namespace dpf