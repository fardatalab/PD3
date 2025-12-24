/**
 * This file contains the main function for the PD3 memory backend.
 */
#include "json.h"
#include "PD3/rdma_backend/pd3_server.hpp"

#include <atomic>
#include <iostream>

std::atomic_bool stop_flag(false);

void SignalHandler(int signum) {
  if (signum == SIGINT || signum == SIGTERM) {
    std::cout << "Stopping server..." << std::endl;
    stop_flag.store(true, std::memory_order_relaxed);
  }
}

int main(int argc, char** argv) {
  dpf::PD3MemoryBackend backend;

  JSON config = {
    {"server_addr", "10.10.1.100"},
    {"server_port", "51216"},
    {"server_name", "pd3_memory_backend"},
    {"max_wr", 1024},
    {"init_regions", 1},
  };

  backend.Configure(config);

  backend.RunAync();

  while (!stop_flag.load(std::memory_order_relaxed)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  backend.Stop();

  return 0;
}