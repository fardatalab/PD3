#include "PD3/prefetcher/prefetcher.hpp"
#include "PD3/prefetcher/types.hpp"
#include "PD3/prefetcher/utils.hpp"

#include "catch2/catch_test_macros.hpp"

#include <atomic>
#include <chrono>
#include <iostream>
#include <thread>
#include <unistd.h>

namespace dpf {

uint64_t GetTimestamp()
{
  using namespace std::chrono;
  return duration_cast<microseconds>(std::chrono::high_resolution_clock::now().time_since_epoch()).count();
}


TEST_CASE("Prefetcher Construction", "[prefetcher]")
{
  Prefetcher prefetcher;
  JSON config;
  config["prefetcher"] = {};
  prefetcher.InitializeBare(config);
}

TEST_CASE("Prefetcher Correctness 2 Shards", "[pre2][corr]")
{
  Prefetcher prefetcher;
  JSON config;
  config["prefetcher"] = {
    {"num_shards", 2},
  };
  int MAX_KEY = 100;
  prefetcher.InitializeBare(config);
  prefetcher.BootstrapHostView(MAX_KEY);
  prefetcher.RunBare();

  prefetcher.PassClientRequest(2); // no prefetch on shard 0
  prefetcher.PassClientRequest(200); // yes prefetch on shard 0
  prefetcher.PassClientRequest(101); // yes prefetch on shard 1
  prefetcher.PassClientRequest(1); // no prefetch on shard 1
  prefetcher.Flush();

  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  auto output_queues = prefetcher.GetPrefetchQueues();

  // check queue 0
  CHECK(output_queues[0]->front() != nullptr);
  auto prebatch = *output_queues[0]->front();
  CHECK(prebatch->size == 1);
  CHECK(prebatch->keys[0] == 200);
  CHECK(output_queues[1]->front() != nullptr);
  prebatch = *output_queues[1]->front();
  CHECK(prebatch->size == 1);
  CHECK(prebatch->keys[0] == 101);

  prefetcher.StopBare();
}

TEST_CASE("Prefetcher Throughput 1 Shards", "[pre1][tput]")
{
  Prefetcher prefetcher;
  JSON config;
  config["prefetcher"] = {
    {"num_shards", 2},
  };
  prefetcher.InitializeBare(config);

  // generate requests
  int NUM_OVERALL_REQUESTS = 4096000;
  uint64_t MAX_KEY = 67108864;
  uint64_t start_ts = 0;
  uint64_t end_ts = 0;
  std::vector<uint64_t> requests;
  uint64_t local_requests = 0;
  for (auto i = 0; i < NUM_OVERALL_REQUESTS; ++i) {
    auto req = rand() % 1073741824;
    if (req < MAX_KEY) {
      local_requests++;
    }
    requests.push_back(req);
  }
  int NUM_PREFETCH_REQUESTS = NUM_OVERALL_REQUESTS - local_requests;
  std::cout << NUM_PREFETCH_REQUESTS << '\n';

  prefetcher.BootstrapHostView(MAX_KEY);
  prefetcher.RunBare();

  std::thread sim_te([&prefetcher, &end_ts, NUM_PREFETCH_REQUESTS]() {
    int core_id = 10;
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);
    int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
    if (rc != 0) {
      std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
    }

    uint64_t count = 0;
    auto prefetch_queues = prefetcher.GetPrefetchQueues();
    size_t iter = 0;

    while (count < NUM_PREFETCH_REQUESTS) {
      bool cpu_pause = true;
      for (int i = 0; i < prefetch_queues.size(); i++) {
        auto output_pipe = prefetch_queues[i];
        PrefetchRequestBatch** batch = output_pipe->front();
        if (batch) {
          auto batch_ptr = *batch;
          output_pipe->pop();
          count += batch_ptr->size;
          cpu_pause = false;
        }
      }
      if (cpu_pause) {
        CpuPause();
      }
    }
    end_ts = GetTimestamp();
  });

  // pin this thread
  int core_id = 7;
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(core_id, &cpuset);
  int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
  if (rc != 0) {
    std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(1000));

  start_ts = GetTimestamp();
  for (int i = 0; i < requests.size(); ++i) {
    prefetcher.PassClientRequest(requests[i]);
  }
  prefetcher.Flush();

  sim_te.join();

  auto duration = end_ts - start_ts;
  std::cout << "Duration: " << duration << " microseconds" << std::endl;
  std::cout << "Throughput: " << (double)requests.size() / duration << " million requests per second" << std::endl;

  prefetcher.StopBare();

}

TEST_CASE("Prefetcher Throughput 2 Shards", "[pre2][tput]")
{
  Prefetcher prefetcher;
  JSON config;
  config["prefetcher"] = {
    {"num_shards", 2},
  };
  prefetcher.InitializeBare(config);

  // generate requests
  int NUM_OVERALL_REQUESTS = 4096000;
  uint64_t MAX_KEY = 67108864;
  uint64_t start_ts = 0;
  uint64_t end_ts = 0;
  std::vector<uint64_t> requests;
  uint64_t local_requests = 0;
  for (auto i = 0; i < NUM_OVERALL_REQUESTS; ++i) {
    auto req = rand() % 1073741824;
    if (req < MAX_KEY) {
      local_requests++;
    }
    requests.push_back(req);
  }
  int NUM_PREFETCH_REQUESTS = NUM_OVERALL_REQUESTS - local_requests;
  std::cout << NUM_PREFETCH_REQUESTS << '\n';

  prefetcher.BootstrapHostView(MAX_KEY);
  prefetcher.RunBare();

  std::thread sim_te([&prefetcher, &end_ts, NUM_PREFETCH_REQUESTS]() {
    int core_id = 10;
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);
    int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
    if (rc != 0) {
      std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
    }

    uint64_t count = 0;
    auto prefetch_queues = prefetcher.GetPrefetchQueues();
    size_t iter = 0;

    while (count < NUM_PREFETCH_REQUESTS) {
      bool cpu_pause = true;
      for (int i = 0; i < prefetch_queues.size(); i++) {
        auto output_pipe = prefetch_queues[i];
        PrefetchRequestBatch** batch = output_pipe->front();
        if (batch) {
          auto batch_ptr = *batch;
          output_pipe->pop();
          count += batch_ptr->size;
          cpu_pause = false;
        }
      }
      if (cpu_pause) {
        CpuPause();
      }
    }
    end_ts = GetTimestamp();
  });

  // pin this thread
  int core_id = 7;
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(core_id, &cpuset);
  int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
  if (rc != 0) {
    std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(1000));

  start_ts = GetTimestamp();
  for (int i = 0; i < requests.size(); ++i) {
    prefetcher.PassClientRequest(requests[i]);
  }
  prefetcher.Flush();

  sim_te.join();

  auto duration = end_ts - start_ts;
  std::cout << "Duration: " << duration << " microseconds" << std::endl;
  std::cout << "Throughput: " << (double)requests.size() / duration << " million requests per second" << std::endl;

  prefetcher.StopBare();

}

TEST_CASE("Prefetcher Throughput 3 Shards", "[pre3][tput]")
{
  Prefetcher prefetcher;
  JSON config;
  config["prefetcher"] = {
    {"num_shards", 3},
    {"queue_depth", 16384}
  };
  prefetcher.InitializeBare(config);

  // generate requests
  int NUM_OVERALL_REQUESTS = 4096000 * 2;
  uint64_t MAX_KEY = 67108864;
  uint64_t start_ts = 0;
  uint64_t end_ts = 0;
  std::vector<uint64_t> requests;
  uint64_t local_requests = 0;
  for (auto i = 0; i < NUM_OVERALL_REQUESTS; ++i) {
    auto req = rand() % 1073741824;
    if (req < MAX_KEY) {
      local_requests++;
    }
    requests.push_back(req);
  }
  int NUM_PREFETCH_REQUESTS = NUM_OVERALL_REQUESTS - local_requests;
  std::cout << NUM_PREFETCH_REQUESTS << '\n';

  prefetcher.BootstrapHostView(MAX_KEY);
  prefetcher.RunBare();

  std::thread sim_te([&prefetcher, &end_ts, NUM_PREFETCH_REQUESTS]() {
    int core_id = 11;
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);
    int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
    if (rc != 0) {
      std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
    }

    uint64_t count = 0;
    auto prefetch_queues = prefetcher.GetPrefetchQueues();
    size_t iter = 0;

    while (count < NUM_PREFETCH_REQUESTS) {
      bool cpu_pause = true;
      for (int i = 0; i < prefetch_queues.size(); i++) {
        auto output_pipe = prefetch_queues[i];
        PrefetchRequestBatch** batch = output_pipe->front();
        if (batch) {
          auto batch_ptr = *batch;
          output_pipe->pop();
          count += batch_ptr->size;
          cpu_pause = false;
        }
      }
      if (cpu_pause) {
        CpuPause();
      }
    }
    end_ts = GetTimestamp();
  });

  // pin this thread
  int core_id = 7;
  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(core_id, &cpuset);
  int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
  if (rc != 0) {
    std::cerr << "Error calling pthread_setaffinity_np: " << rc << "\n";
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(1000));

  start_ts = GetTimestamp();
  for (int i = 0; i < requests.size(); ++i) {
    prefetcher.PassClientRequest(requests[i]);
  }
  prefetcher.Flush();

  sim_te.join();

  auto duration = end_ts - start_ts;
  std::cout << "Duration: " << duration << " microseconds" << std::endl;
  std::cout << "Throughput: " << (double)requests.size() / duration << " million requests per second" << std::endl;

  prefetcher.StopBare();

}

TEST_CASE("Prefetcher Correctness 2 Shards Refresh Write", "[pre2][corr][r]")
{
  // check whether the entries are correctly removed from the host view on refresh
  Prefetcher prefetcher;
  JSON config;
  config["prefetcher"] = {
    {"num_shards", 2},
  };
  prefetcher.InitializeBare(config);
  auto refresher = prefetcher.GetRefresher();

  int MAX_KEY = 10'000'000;

  prefetcher.BootstrapHostView(MAX_KEY);
  prefetcher.RunBare();

  // produce a few write batches into the refresher
  refresher->SetScopedMode(false);
  std::vector<int> requests;
  for (int i = 0; i < 1000; ++i) {
    auto req = rand() % 10'000;
    requests.push_back(req);
    refresher->PassRefreshRequest(req);
  }
  refresher->FlushRefreshRequest();

  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  prefetcher.StopBare();

  // check whether all the keys have been removed
  for (int i = 0; i < requests.size(); ++i) {
    CHECK_FALSE(prefetcher.HostViewContains(requests[i]));
  }
}

TEST_CASE("Prefetcher Correctness 1 Shards Refresh Write", "[pre1][corr][r]")
{
  // check whether the entries are correctly removed from the host view on refresh
  Prefetcher prefetcher;
  JSON config;
  config["prefetcher"] = {
  };
  prefetcher.InitializeBare(config);
  auto refresher = prefetcher.GetRefresher();

  int MAX_KEY = 10'000'000;

  prefetcher.BootstrapHostView(MAX_KEY);
  prefetcher.RunBare();

  // produce a few write batches into the refresher
  refresher->SetScopedMode(false);
  std::vector<int> requests;
  for (int i = 0; i < 1000; ++i) {
    auto req = rand() % 10'000;
    requests.push_back(req);
    refresher->PassRefreshRequest(req);
  }
  refresher->FlushRefreshRequest();

  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  prefetcher.StopBare();

  // check whether all the keys have been removed
  for (int i = 0; i < requests.size(); ++i) {
    CHECK_FALSE(prefetcher.HostViewContains(requests[i]));
  }
}


TEST_CASE("Prefetcher Correctness 1 Shards Refresh Read", "[pre1][corr][r]")
{
  /// IMPORTANT: PD3_HV_ADD_ON_REMOTE_READ must be set to 1 for this to work 
  // check whether the entries are correctly removed from the host view on refresh
  Prefetcher prefetcher;
  JSON config;
  config["prefetcher"] = {
  };
  prefetcher.InitializeBare(config);
  auto refresher = prefetcher.GetRefresher();

  int MAX_KEY = 10'000'000;

  prefetcher.BootstrapHostView(MAX_KEY);
  prefetcher.RunBare();

  // produce a few write batches into the refresher
  refresher->SetScopedMode(true);
  std::vector<int> requests;
  for (int i = 0; i < 1000; ++i) {
    auto req = rand() % 10'000'000 + 10'000'001;
    requests.push_back(req);
    refresher->PassRefreshRequest(req);
  }
  refresher->FlushRefreshRequest();

  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  prefetcher.StopBare();

  // first check whether all initial 10M keys are there
  for (int i = 1; i < 10'000'000; ++i) {
    CHECK(prefetcher.HostViewContains(i));
  }
  // check whether all the keys have been added
  for (int i = 0; i < requests.size(); ++i) {
    CHECK(prefetcher.HostViewContains(requests[i]));
  } 
}

TEST_CASE("Prefetcher Correctness 2 Shards Refresh Read", "[pre2][corr][r]")
{
  /// IMPORTANT: PD3_HV_ADD_ON_REMOTE_READ must be set to 1 for this to work
  // check whether the entries are correctly removed from the host view on refresh
  Prefetcher prefetcher;
  JSON config;
  config["prefetcher"] = {
    {"num_shards", 2},
  };
  prefetcher.InitializeBare(config);
  auto refresher = prefetcher.GetRefresher();

  int MAX_KEY = 10'000'000;

  prefetcher.BootstrapHostView(MAX_KEY);
  prefetcher.RunBare();

  // produce a few write batches into the refresher
  refresher->SetScopedMode(true);
  std::vector<int> requests;
  for (int i = 0; i < 1000; ++i) {
    auto req = rand() % 10'000'000 + 10'000'001;
    requests.push_back(req);
    refresher->PassRefreshRequest(req);
  }
  refresher->FlushRefreshRequest();

  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  prefetcher.StopBare();

  // first check whether all initial 10M keys are there
  for (int i = 1; i < 10'000'000; ++i) {
    CHECK(prefetcher.HostViewContains(i));
  }
  // check whether all the keys have been added
  for (int i = 0; i < requests.size(); ++i) {
    CHECK(prefetcher.HostViewContains(requests[i]));
  } 
}

}