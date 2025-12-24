/**
 * This file contains the main function for the batched memory backend.
 */

#include "json.h"
#include "rdma_backend/batched_server.hpp"

#include <atomic>
#include <iostream>

#include <signal.h>

std::atomic_bool stop_flag(false);

void SignalHandler(int signum) {
  if (signum == SIGINT || signum == SIGTERM) {
    std::cout << "Stopping server..." << std::endl;
    stop_flag.store(true, std::memory_order_relaxed);
  }
}

int main(int argc, char** argv) {
  // TODO: take in arguments from the command line here
  dpf::BatchedMemoryBackend backend;

  JSON config = {
    {"server_addr", "10.10.1.100"},
    {"server_port", "51216"},
    {"server_name", "batched_memory_backend"},
    {"max_wr", 1024},
    {"init_regions", 20},
  };

  backend.Configure(config);

  backend.RunAync();

  while (!stop_flag.load(std::memory_order_relaxed)) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  backend.Stop();

  return 0;
}
