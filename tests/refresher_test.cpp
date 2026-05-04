#include "PD3/prefetcher/refresher.hpp"
#include "PD3/prefetcher/types.hpp"

#include "catch2/catch_test_macros.hpp"

#include <atomic>
#include <chrono>
#include <iostream>
#include <thread>
#include <unistd.h>


namespace dpf {

TEST_CASE("Refresher Construction", "[refresher]")
{
  HostViewRefresher refresher;
  refresher.Initialize(2, 1024, 500);
}

TEST_CASE("Refresher Send Reads 1 Shard Flush", "[refresher]")
{
  HostViewRefresher refresher;
  refresher.Initialize(1, 4, 16);
  refresher.SetScopedMode(true); 

  for (int i = 0; i < 8; ++i) {
    refresher.PassRefreshRequest(i);
  }
  refresher.FlushRefreshRequest();

  auto refresh_q = refresher.GetRefreshQueue(0);
  auto refresh_batch_ptr = refresh_q->front();
  CHECK(refresh_batch_ptr != nullptr);
  auto refresh_batch = *refresh_batch_ptr;
  CHECK(refresh_batch->size == 8);
  CHECK(refresh_batch->is_read);
  for (int i = 0; i < 8; ++i) {
    CHECK(refresh_batch->requests[i] == i);
  }
}

TEST_CASE("Refresher Send Reads 1 Shard No Flush", "[refresher]")
{
  HostViewRefresher refresher;
  refresher.Initialize(1, 4, 16);
  refresher.SetScopedMode(true); 

  for (int i = 0; i < 16; ++i) {
    refresher.PassRefreshRequest(i);
  }

  auto refresh_q = refresher.GetRefreshQueue(0);
  auto refresh_batch_ptr = refresh_q->front();
  CHECK(refresh_batch_ptr != nullptr);
  auto refresh_batch = *refresh_batch_ptr;
  CHECK(refresh_batch->size == 16);
  CHECK(refresh_batch->is_read);
  for (int i = 0; i < 16; ++i) {
    CHECK(refresh_batch->requests[i] == i);
  }
}

TEST_CASE("Refresher Send Writes 1 Shard Flush", "[refresher]")
{
  HostViewRefresher refresher;
  refresher.Initialize(1, 4, 16);
  refresher.SetScopedMode(false); 

  for (int i = 0; i < 8; ++i) {
    refresher.PassRefreshRequest(i);
  }
  refresher.FlushRefreshRequest();

  auto refresh_q = refresher.GetRefreshQueue(0);
  auto refresh_batch_ptr = refresh_q->front();
  CHECK(refresh_batch_ptr != nullptr);
  auto refresh_batch = *refresh_batch_ptr;
  CHECK(refresh_batch->size == 8);
  CHECK(!refresh_batch->is_read);
  for (int i = 0; i < 8; ++i) {
    CHECK(refresh_batch->requests[i] == i);
  }
}

TEST_CASE("Refresher Send Writes 1 Shard No Flush", "[refresher]")
{
  HostViewRefresher refresher;
  refresher.Initialize(1, 4, 16);
  refresher.SetScopedMode(false); 

  for (int i = 0; i < 16; ++i) {
    refresher.PassRefreshRequest(i);
  }

  auto refresh_q = refresher.GetRefreshQueue(0);
  auto refresh_batch_ptr = refresh_q->front();
  CHECK(refresh_batch_ptr != nullptr);
  auto refresh_batch = *refresh_batch_ptr;
  CHECK(refresh_batch->size == 16);
  CHECK(!refresh_batch->is_read);
  for (int i = 0; i < 16; ++i) {
    CHECK(refresh_batch->requests[i] == i);
  }
}

TEST_CASE("Refresher Send Reads 2 Shards Flush", "[refresher]")
{
  HostViewRefresher refresher;
  refresher.Initialize(2, 4, 16);
  refresher.SetScopedMode(true);

  for (int i = 0; i < 8; ++i) {
    refresher.PassRefreshRequest(i);
  }
  refresher.FlushRefreshRequest();

  auto refresh_q0 = refresher.GetRefreshQueue(0);
  auto refresh_batch_ptr0 = refresh_q0->front();
  CHECK(refresh_batch_ptr0 != nullptr);
  auto refresh_batch0 = *refresh_batch_ptr0;
  CHECK(refresh_batch0->size == 4);
  CHECK(refresh_batch0->is_read);
  for (int i = 0; i < 4; ++i) {
    CHECK(refresh_batch0->requests[i] == i * 2);
  }

  auto refresh_q1 = refresher.GetRefreshQueue(1);
  auto refresh_batch_ptr1 = refresh_q1->front();
  CHECK(refresh_batch_ptr1 != nullptr);
  auto refresh_batch1 = *refresh_batch_ptr1;
  CHECK(refresh_batch1->size == 4);
  CHECK(refresh_batch1->is_read);
  for (int i = 0; i < 4; ++i) {
    CHECK(refresh_batch1->requests[i] == (i * 2) + 1);
  }
}

TEST_CASE("Refresher Send Reads 2 Shards No Flush", "[refresher]")
{
  HostViewRefresher refresher;
  refresher.Initialize(2, 4, 8);
  refresher.SetScopedMode(true);

  for (int i = 0; i < 16; ++i) {
    refresher.PassRefreshRequest(i);
  }

  auto refresh_q0 = refresher.GetRefreshQueue(0);
  auto refresh_batch_ptr0 = refresh_q0->front();
  CHECK(refresh_batch_ptr0 != nullptr);
  auto refresh_batch0 = *refresh_batch_ptr0;
  CHECK(refresh_batch0->size == 8);
  CHECK(refresh_batch0->is_read);
  for (int i = 0; i < 8; ++i) {
    CHECK(refresh_batch0->requests[i] == i * 2);
  }

  auto refresh_q1 = refresher.GetRefreshQueue(1);
  auto refresh_batch_ptr1 = refresh_q1->front();
  CHECK(refresh_batch_ptr1 != nullptr);
  auto refresh_batch1 = *refresh_batch_ptr1;
  CHECK(refresh_batch1->size == 8);
  CHECK(refresh_batch1->is_read);
  for (int i = 0; i < 8; ++i) {
    CHECK(refresh_batch1->requests[i] == (i * 2) + 1);
  }
}

TEST_CASE("Refresher Send Writes 2 Shards Flush", "[refresher]")
{
  HostViewRefresher refresher;
  refresher.Initialize(2, 4, 16);
  refresher.SetScopedMode(false);

  for (int i = 0; i < 8; ++i) {
    refresher.PassRefreshRequest(i);
  }
  refresher.FlushRefreshRequest();

  auto refresh_q0 = refresher.GetRefreshQueue(0);
  auto refresh_batch_ptr0 = refresh_q0->front();
  CHECK(refresh_batch_ptr0 != nullptr);
  auto refresh_batch0 = *refresh_batch_ptr0;
  CHECK(refresh_batch0->size == 4);
  CHECK(!refresh_batch0->is_read);
  for (int i = 0; i < 4; ++i) {
    CHECK(refresh_batch0->requests[i] == i * 2);
  }

  auto refresh_q1 = refresher.GetRefreshQueue(1);
  auto refresh_batch_ptr1 = refresh_q1->front();
  CHECK(refresh_batch_ptr1 != nullptr);
  auto refresh_batch1 = *refresh_batch_ptr1;
  CHECK(refresh_batch1->size == 4);
  CHECK(!refresh_batch1->is_read);
  for (int i = 0; i < 4; ++i) {
    CHECK(refresh_batch1->requests[i] == (i * 2) + 1);
  }
}

TEST_CASE("Refresher Send Writes 2 Shards No Flush", "[refresher]")
{
  HostViewRefresher refresher;
  refresher.Initialize(2, 4, 8);
  refresher.SetScopedMode(false);

  for (int i = 0; i < 16; ++i) {
    refresher.PassRefreshRequest(i);
  }

  auto refresh_q0 = refresher.GetRefreshQueue(0);
  auto refresh_batch_ptr0 = refresh_q0->front();
  CHECK(refresh_batch_ptr0 != nullptr);
  auto refresh_batch0 = *refresh_batch_ptr0;
  CHECK(refresh_batch0->size == 8);
  CHECK(!refresh_batch0->is_read);
  for (int i = 0; i < 8; ++i) {
    CHECK(refresh_batch0->requests[i] == i * 2);
  }

  auto refresh_q1 = refresher.GetRefreshQueue(1);
  auto refresh_batch_ptr1 = refresh_q1->front();
  CHECK(refresh_batch_ptr1 != nullptr);
  auto refresh_batch1 = *refresh_batch_ptr1;
  CHECK(refresh_batch1->size == 8);
  CHECK(!refresh_batch1->is_read);
  for (int i = 0; i < 8; ++i) {
    CHECK(refresh_batch1->requests[i] == (i * 2) + 1);
  }
}

TEST_CASE("Refresher Throughput 1 Shard Reads", "[refresher][tput]")
{
  constexpr uint64_t TOTAL_REQUESTS = 4096000;
  constexpr int QUEUE_DEPTH = 8192;
  constexpr int BATCH_SIZE = 500;

  HostViewRefresher refresher;
  refresher.Initialize(1, QUEUE_DEPTH, BATCH_SIZE);
  refresher.SetScopedMode(true);

  auto refresh_q = refresher.GetRefreshQueue(0);
  REQUIRE(refresh_q != nullptr);

  std::atomic<bool> start_benchmark{false};
  std::atomic<uint64_t> consumed_requests{0};
  std::atomic<uint64_t> consumed_batches{0};
  std::atomic<bool> saw_non_read_batch{false};
  std::atomic<bool> saw_partial_batch{false};

  std::thread producer([&]() {
    while (!start_benchmark.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }

    for (uint64_t i = 0; i < TOTAL_REQUESTS; ++i) {
      refresher.PassRefreshRequest(i);
      if ((i + 1) % BATCH_SIZE == 0) {
        std::this_thread::yield();
      }
    }
  });

  std::thread consumer([&]() {
    while (!start_benchmark.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }

    uint64_t local_consumed = 0;
    uint64_t local_batches = 0;
    bool local_saw_non_read_batch = false;
    bool local_saw_partial_batch = false;

    while (local_consumed < TOTAL_REQUESTS) {
      auto refresh_batch_ptr = refresh_q->front();
      if (refresh_batch_ptr == nullptr) {
        std::this_thread::yield();
        continue;
      }

      auto refresh_batch = *refresh_batch_ptr;
      refresh_q->pop();

      local_consumed += refresh_batch->size;
      local_batches++;
      local_saw_non_read_batch = local_saw_non_read_batch || !refresh_batch->is_read;
      local_saw_partial_batch = local_saw_partial_batch || (refresh_batch->size != BATCH_SIZE);
    }

    consumed_requests.store(local_consumed, std::memory_order_release);
    consumed_batches.store(local_batches, std::memory_order_release);
    saw_non_read_batch.store(local_saw_non_read_batch, std::memory_order_release);
    saw_partial_batch.store(local_saw_partial_batch, std::memory_order_release);
  });

  auto start = std::chrono::high_resolution_clock::now();
  start_benchmark.store(true, std::memory_order_release);

  producer.join();
  consumer.join();

  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
  auto throughput = duration == 0 ? 0.0 : static_cast<double>(TOTAL_REQUESTS) / duration;

  std::cout << "Refresher throughput: " << throughput << " MOps\n";

  CHECK(consumed_requests.load(std::memory_order_acquire) == TOTAL_REQUESTS);
  CHECK(consumed_batches.load(std::memory_order_acquire) == TOTAL_REQUESTS / BATCH_SIZE);
  CHECK(!saw_non_read_batch.load(std::memory_order_acquire));
  CHECK(!saw_partial_batch.load(std::memory_order_acquire));
}


}