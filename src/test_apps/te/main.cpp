#include "PD3/transfer_engine/transfer_engine.hpp"
#include "PD3/transfer_engine/types.hpp"
#include "PD3/prefetcher/refresher.hpp"

#include <iostream>
#include <atomic>
#include <chrono>
#include <signal.h>
#include <unistd.h>

using namespace dpf;
using namespace dpf::offload;

std::atomic_bool running{false};

void SignalHandler(int signum)
{
  if (signum == SIGINT || signum == SIGTSTP) {
    running = false;
  }
  return;
}

int main()
{
  std::cout << "Hello from prefetcher app\n";

  signal(SIGINT, SignalHandler);

  TransferEngine engine;
  TransferEngineConfig config;
  config.buf_agent_file_path = "";
  config.buf_client_file_path = "/home/ubuntu/tc_buf_details.txt";
  config.dpu_pcie_addr = "03:00.0";
  config.enable_agent = false;
  config.enable_transfer = true;
  config.export_desc_agent_file_path = "";
  config.export_desc_client_file_path = "/home/ubuntu/tc_export_desc";
  config.max_wr = 2048;
  config.num_prefetch_slots = 8;
  config.num_slots = 8;
  config.server_addr = "10.10.2.100";
  config.local_addr = "10.10.2.201";
  config.server_port = "51216";
  config.slot_size = 16384*2;
  config.use_rdma = true;

  HostViewRefresher refresher;
  constexpr int QUEUE_DEPTH = 10000;
  constexpr int BATCH_SIZE = 500;
  refresher.Initialize(1, QUEUE_DEPTH, 500);
  std::atomic refresher_running{true};

  std::thread t([&]() {
    int core_id = 8; 
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);
    int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
    if (rc != 0) {
      std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
    }
    auto q = refresher.GetRefreshQueue(0);
    uint64_t count = 0;
    while (refresher_running.load(std::memory_order_relaxed)) {
      auto batch_ptr = q->front();
      if (!batch_ptr) {
        std::this_thread::yield();
      } else {
        auto batch = *batch_ptr;
        if (batch->is_read) {
          count += batch->size;
        }
        q->pop();
      }
    }
    std::cout << "Count: " << count << '\n';
  });


  PrefetchRequestBatchQueue prefetch_req_batch_q{1};

  engine.Initialize(config);
  engine.SetPrefetchRequestQueue(&prefetch_req_batch_q);
  engine.SetRefresher(&refresher);

  engine.Run();
  running = true;

  while (running) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  std::cout << "Stopping prefetcher\n";
  engine.Stop();
  refresher_running = false;
  t.join();
  std::cout << "Stopped prefetcher\n";
}