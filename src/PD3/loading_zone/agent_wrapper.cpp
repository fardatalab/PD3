#include "agent.hpp"

#include "PD3/system/logger.hpp"

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
    config.pcie_address = "e1:00.0";
    config.mmap_export_path = "/data/ssankhe/agent_export_desc";
    config.buf_details_path = "/data/ssankhe/agent_buf_details.txt";
    dpf::agent::Initialize(config);
  }

  EXPORTED_SYMBOL bool Agent_CheckAndReturn(const char* key, uint32_t length) {
    static thread_local uint64_t local_consumer_idx = 0;
    // std::cout << "local_consumer_idx: " << local_consumer_idx << std::endl;
    uint64_t key_value = *reinterpret_cast<const uint64_t*>(key);

    bool found = false;
    char* buffer = dpf::agent::Agent::Instance().GetAgentBuffer()->buffer();

    uint64_t internal_counter = 0;

    while (!found) {
      internal_counter++;
      auto internal_idx = local_consumer_idx;
      auto record_header = reinterpret_cast<dpf::buffer::RecordHeader*>(buffer + dpf::buffer::AgentBuffer::GetBufferIdx(internal_idx));
      auto curr_consumer_state = record_header->consumer_state.load(std::memory_order_acquire);
      if (curr_consumer_state != dpf::buffer::CONSUMER_STATE_WRITTEN) {
        std::this_thread::yield();
        continue;
      }
      auto record_header_size = record_header->size;
      if (record_header_size != 32) {
        std::this_thread::yield();
        continue;
      }
      internal_idx += sizeof(dpf::buffer::RecordHeader);
      auto key = *reinterpret_cast<uint64_t*>(buffer + dpf::buffer::AgentBuffer::GetBufferIdx(internal_idx));
      internal_idx += sizeof(uint64_t);
      auto value = *reinterpret_cast<uint64_t*>(buffer + dpf::buffer::AgentBuffer::GetBufferIdx(internal_idx));
      if (value != 1) {
        continue;
      }
      if (key == key_value) {
        found = true;
      }
      local_consumer_idx += record_header_size;
    }

    return true;
  }

}
