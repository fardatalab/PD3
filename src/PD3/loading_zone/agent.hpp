#pragma once

/**
 * The agent is a library that is embedded into the application. It serves cache miss requests from the 
 * agent memory buffer if found
 * 
 */

#include "PD3/buffer/agent_buffer.hpp"
#include "PD3/system/latency_writer.hpp"

#include "book_keeper/types.hpp"
#include "common/doca_common.hpp"
#include "common/app_utils.h"

namespace dpf {
namespace agent {

struct AgentConfig {
  std::string pcie_address;
  std::string mmap_export_path;
  std::string buf_details_path;
};

class Agent {

public:

  static Agent& Instance() {
    static Agent instance;
    return instance;
  }

  /// @brief Initialize the agent. This creates the spmc buffer and maps it to the NIC
  void Initialize(const AgentConfig& config);

  /// @brief Find the remote address in the cache
  /// @param remote_addr the remote address to find
  /// @return true if found, false otherwise
  bool FindInCache(const RemoteAddressT2& remote_addr, char*& data);

  /// @brief Get the agent buffer
  /// @return the agent buffer
  dpf::buffer::AgentBuffer* GetAgentBuffer() const noexcept {
    return agent_buffer_;
  }

  dpf::LatencyWriter& GetLatencyWriter() noexcept {
    return latency_writer_;
  }

private:

  Agent() = default;
  ~Agent() = default;

  Agent(const Agent&) = delete;
  Agent& operator=(const Agent&) = delete;

  AppState state_;

  dpf::buffer::AgentBuffer* agent_buffer_;
  char* mem_region_;
  LatencyWriter latency_writer_{"/data/ssankhe/fig21/agent_latency.jsonl"};

  // do we want an index?
};

inline Agent& GetInstance() {
  return Agent::Instance();
}

inline void Initialize(const AgentConfig& config) {
  GetInstance().Initialize(config);
}

inline bool FindInCache(const RemoteAddressT2& remote_addr, char*& data) {
  return GetInstance().FindInCache(remote_addr, data);
}

} // namespace agent
} // namespace dpf