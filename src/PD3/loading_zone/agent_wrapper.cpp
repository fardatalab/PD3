#include "agent.hpp"

#include "PD3/buffer/types.hpp"
#include "PD3/system/logger.hpp"

#include <atomic>
#include <string>
#include <thread>

#if defined(_WIN32) || defined(_WIN64)  
#define EXPORTED_SYMBOL __declspec(dllexport)  
#else  
#define EXPORTED_SYMBOL __attribute__((visibility("default")))  
#endif 

extern "C" {

  EXPORTED_SYMBOL void Agent_Init(const char* agent_id, const char* agent_key) {
    dpf::LOG_INFOF("Agent_Init: agent_id: {}, agent_key: {}", agent_id, agent_key);
    dpf::agent::AgentConfig config;
    config.pcie_address = "21:00.0";
    config.mmap_export_path = "/data/nsdi/agent_export_desc";
    config.buf_details_path = "/data/nsdi/agent_buf_details.txt";
    dpf::agent::Initialize(config);
  }

  EXPORTED_SYMBOL bool Agent_CheckAndReturn(const char* desired_key, uint32_t length) {
    static thread_local uint64_t local_consumer_idx = 0;
    static thread_local uint64_t wrap_counter = 1;
    // std::cout << "local_consumer_idx: " << local_consumer_idx << std::endl;
    uint64_t key_value = *reinterpret_cast<const uint64_t*>(desired_key);
    uint64_t val = 0; // this is currently discarded

    bool found = false;
    char* buffer = dpf::agent::Agent::Instance().GetAgentBuffer()->buffer();
    const auto agent_buf = dpf::agent::Agent::Instance().GetAgentBuffer();

    uint64_t internal_counter = 0;

    while (true) {
      auto producer_idx = agent_buf->producer_index();
      if (producer_idx <= local_consumer_idx) {
        std::this_thread::yield();
        continue;
      }

      while (local_consumer_idx < producer_idx) {
        auto internal_idx = local_consumer_idx;
        auto consumer_pos = dpf::buffer::AgentBuffer::GetBufferIdx(local_consumer_idx);
        auto record_header = reinterpret_cast<dpf::buffer::RecordHeader*>(buffer + consumer_pos);
        auto curr_consumer_state = record_header->consumer_state.load(std::memory_order_acquire);
        auto record_header_size = record_header->size;
        internal_idx += sizeof(dpf::buffer::RecordHeader);
        auto key_ptr = reinterpret_cast<uint64_t*>(buffer + dpf::buffer::AgentBuffer::GetBufferIdx(internal_idx));
        auto key = __atomic_load_n(key_ptr, __ATOMIC_ACQUIRE);
        internal_idx += sizeof(uint64_t);
        if (key == key_value) {
          // update the state
          auto expected_state = dpf::buffer::CONSUMER_STATE_WRITTEN;
          if (record_header->consumer_state.compare_exchange_strong(expected_state, dpf::buffer::CONSUMER_STATE_PROCESSING, std::memory_order_release, std::memory_order_relaxed)) {
            record_header->consumer_state.store(dpf::buffer::CONSUMER_STATE_COMPLETED, std::memory_order_release);
            found = true;
            val = *reinterpret_cast<uint64_t*>(buffer + dpf::buffer::AgentBuffer::GetBufferIdx(internal_idx));
          } else {
            found = false;
          }
        }
        local_consumer_idx += record_header_size;
        if (found) {
          goto out;
        }
      }
    }

out:
    return found;
  }

}
