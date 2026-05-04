#include "PD3/buffer/types.hpp"
#include "PD3/transfer_engine/transfer_client.hpp"
#include "PD3/transfer_engine/types.hpp"
#include "PD3/prefetcher/types.hpp"

#include "catch2/catch_test_macros.hpp"

#include <strings.h>
#include <thread>
#include <chrono>
#include <atomic>
#include <unistd.h>
#include <iostream>
#include <vector>
#include <algorithm>

namespace dpf {
namespace offload {

/**
 * IMPORTANT: please make sure the PD3 server is running on remote memory + PD3 is running on the DPU before running 
 * these tests. They will fail if either process is not up
 */

TEST_CASE("TransferClient Construction", "[transfer_client]")
{
  TransferClient client;
}

TEST_CASE("TransferClient One Read/Write Request", "[transfer_client][one]")
{
  TransferClient client;
  TransferClientConfig config;
  config.pcie_address = "21:00.0";
  config.mmap_export_path = "/data/ssankhe/tc_export_desc";
  config.buf_details_path = "/data/ssankhe/tc_buf_details.txt";

  const char* data = "This is a test string of data";
  size_t data_size = strlen(data);

  client.Initialize(config);
  TransferRequest request;
  request.address = 0;
  request.bytes = data_size;
  request.req_id = 1;
  request.is_read = false;
  client.SubmitRequest(request, data, data_size);
  CompletionOutput output;
  while (true) {
    output = client.PollCompletions();
    if (output.response == nullptr) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    } else {
      break;
    }
  }
  std::cout << "Received completion for write\n";
  std::cout << "ReqID: " << output.response->req_id << '\n';

  request.address = 0;
  request.bytes = data_size;
  request.req_id = 2;
  request.is_read = true;
  client.SubmitRequest(request, nullptr, 0);
  while (true) {
    output = client.PollCompletions();
    if (output.response == nullptr) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    } else {
      break;
    }
  }
  std::cout << "Received completion for read\n";
  std::cout << "ReqID: " << output.response->req_id << '\n';
  CHECK(output.response->req_id == 2);
  CHECK(strncasecmp(output.data, data, data_size));
  CHECK(output.response->bytes == data_size);
}

TEST_CASE("TransferClient Stress Test", "[transfer_client][stress][s]")
{
  TransferClient client;
  TransferClientConfig config;
  config.pcie_address = "21:00.0";
  config.mmap_export_path = "/data/ssankhe/tc_export_desc";
  config.buf_details_path = "/data/ssankhe/tc_buf_details.txt";

  const char* data = "This is a test string of data";
  size_t data_size = strlen(data);

  client.Initialize(config);
  TransferRequest request;
  request.address = 0;
  request.bytes = data_size;
  request.req_id = 1;
  request.is_read = false;
  client.SubmitRequest(request, data, data_size);
  CompletionOutput output;
  while (true) {
    output = client.PollCompletions();
    if (output.response == nullptr) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    } else {
      break;
    }
  }
  std::cout << "Received completion for write\n";
  std::cout << "ReqID: " << output.response->req_id << '\n';

  std::vector<uint64_t> latencies;
  std::vector<uint16_t> req_ids;
  latencies.resize(10000);

  uint64_t max_req_id = 0;
  uint64_t num_errors = 0;
  for (int i = 2; i < 10000; ++i) {
    request.address = 0;
    request.bytes = data_size;
    request.req_id = i;
    request.is_read = true;
    auto start = std::chrono::high_resolution_clock::now();
    client.SubmitRequest(request, nullptr, 0);
    while (true) {
      output = client.PollCompletions();
      if (output.response == nullptr) {
        std::this_thread::yield();
        // std::this_thread::sleep_for(std::chrono::milliseconds(500));
      } else {
        break;
      }
    }
    auto end = std::chrono::high_resolution_clock::now();
    auto latency_in_nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
    latencies[i] = latency_in_nanos;
    if (output.response->req_id == i) {
      max_req_id = i;
    } else {
      // std::cout << "output.response->bytes: " << output.response->bytes << '\n';
      // std::cout << "output.response->req_id: " << output.response->req_id << '\n';
      // std::cout << "output.response->is_read: " << output.response->is_read << '\n';
      // std::cout << "i: " << i << '\n';
      num_errors++;
    }
    if (output.response->req_id != 0) {
      req_ids.push_back(output.response->req_id);
    }
    // if (output.response->req_id != 0)
    //   std::cout << "ReqID: " << output.response->req_id << '\n';

  }
  
  uint64_t total_latency = 0;
  uint64_t count = 0;
  for (int i = 2000; i < 8000; ++i) {
    total_latency += latencies[i];
    count++;
  }
  double avg_latency = (double)total_latency / count;
  std::cout << "Avg Latency: " << avg_latency << '\n';
  std::cout << "Num errors: " << num_errors << '\n';
  std::cout << "Max Request ID: " << max_req_id << '\n';

  for (int i = 0; i < req_ids.size()-1; ++i) {
    if (req_ids[i] != req_ids[i+1]-1) {
      std::cout << "out of order: " << req_ids[i] << " " << req_ids[i+1] << '\n';
    }
  }
}

TEST_CASE("TransferClient Stress Test Many Requests", "[transfer_client][stress][xl][ll]")
{
  TransferClient client;
  TransferClientConfig config;
  config.pcie_address = "21:00.0";
  config.mmap_export_path = "/data/ssankhe/tc_export_desc";
  config.buf_details_path = "/data/ssankhe/tc_buf_details.txt";

  // const char* data = "This is a test string of data";
  // size_t data_size = strlen(data);

  char data[16];
  uint64_t key = 1;
  uint64_t val = 200;
  *reinterpret_cast<uint64_t*>(data) = key;
  *reinterpret_cast<uint64_t*>(data + sizeof(uint64_t)) = val;
  size_t data_size = 16;

  client.Initialize(config);
  TransferRequest request;
  request.address = 0;
  request.bytes = data_size;
  request.req_id = 1;
  request.is_read = false;
  client.SubmitRequest(request, data, data_size);
  CompletionOutput output;
  while (true) {
    output = client.PollCompletions();
    if (output.response == nullptr) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    } else {
      break;
    }
  }
  std::cout << "Received completion for write\n";
  std::cout << "ReqID: " << output.response->req_id << '\n';

  uint16_t req_id = 1;

  // const uint64_t NUM_REQUESTS = 500 * 4096;
  const uint64_t NUM_REQUESTS = 1000 * 4096;
  const uint32_t RECORD_SIZE = 16;
  const uint32_t BATCH_SIZE = 2 * 4096;

  std::vector<uint16_t> received_req_ids;
  received_req_ids.resize(NUM_REQUESTS);
  std::vector<uint16_t> sent_req_ids;
  sent_req_ids.resize(NUM_REQUESTS);
  auto received_req_ids_idx = 0;
  std::vector<uint64_t> addresses;
  addresses.resize(NUM_REQUESTS);
  for (int i = 0; i < NUM_REQUESTS; ++i) {
    auto key = rand() % 1'000'000;
    addresses[i] = key * 8;
  }

  auto start = std::chrono::high_resolution_clock::now();
  // uint16_t expected_req_id = 2;
  uint64_t sent = 0;
  for (int i = 0; i < NUM_REQUESTS; ++i) {
    request.address = 0;
    request.bytes = RECORD_SIZE;
    request.req_id = req_id++;
    request.is_read = true;
    auto out = client.SubmitRequest(request, nullptr, 0);
    if (!out) {
      std::cout << "failed to submit request\n";
    }
    sent_req_ids[i] = request.req_id;
    sent++;
    // if (sent == 8) {
    //   std::this_thread::sleep_for(std::chrono::milliseconds(30));
    // }
    // std::cout << "sent: " << sent << '\n';
    if (sent % BATCH_SIZE == 0) {
      uint16_t expected_req_id = 1;
      // poll completions
      uint32_t num_completions = 0;
      uint64_t count = 0;
      while (num_completions < BATCH_SIZE) {
        count++;
        if (count % 1'000'000 == 0) {
          std::cout << sent << " " << num_completions << '\n';
          client.PrintStats();
        }
        output = client.PollCompletions();
        if (output.response == nullptr) {
          std::this_thread::yield();
        } else {
          received_req_ids[received_req_ids_idx++] = output.response->req_id;
          if (expected_req_id != output.response->req_id) {
            std::cout << expected_req_id << " " << output.response->req_id << " " << sent << '\n';
          }
          expected_req_id++;
          num_completions++;
        }
      }
      req_id = 1;
    }
  }

  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
  std::cout << "Time taken: " << duration << " us\n";
  
  std::cout << "Completed " << NUM_REQUESTS << " requests\n";
  std::cout << "Throughput: " << (double)NUM_REQUESTS / duration << " Mops\n";

  // std::sort(sent_req_ids.begin(), sent_req_ids.end());
  // std::sort(received_req_ids.begin(), received_req_ids.end());
  for (int i = 0; i < NUM_REQUESTS; ++i) {
    if (sent_req_ids[i] != received_req_ids[i]) {
      std::cout << "Sent reqID: " << sent_req_ids[i] << " but got: " << received_req_ids[i] << '\n';
      break;
    }
  }
}

TEST_CASE("TransferClient Stress Test Many Requests Multi Threaded", "[transfer_client][stress][mt]")
{
  TransferClient client;
  TransferClientConfig config;
  config.pcie_address = "21:00.0";
  config.mmap_export_path = "/data/ssankhe/tc_export_desc";
  config.buf_details_path = "/data/ssankhe/tc_buf_details.txt";

  const char* data = "This is a test string of data";
  size_t data_size = strlen(data);

  client.Initialize(config);
  TransferRequest request;
  request.address = 0;
  request.bytes = data_size;
  request.req_id = 1;
  request.is_read = false;
  client.SubmitRequest(request, data, data_size);
  CompletionOutput output;
  while (true) {
    output = client.PollCompletions();
    if (output.response == nullptr) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    } else {
      break;
    }
  }
  std::cout << "Received completion for write\n";
  std::cout << "ReqID: " << output.response->req_id << '\n';

  static constexpr uint32_t NUM_THREADS = 8;
  static constexpr uint32_t REQUESTS_PER_THREAD = 4096;
  static constexpr uint32_t TOTAL_REQUESTS = NUM_THREADS * REQUESTS_PER_THREAD;
  static constexpr uint32_t RECORD_SIZE = 64;

  std::vector<uint16_t> sent_req_ids(TOTAL_REQUESTS);
  std::vector<uint16_t> received_req_ids(TOTAL_REQUESTS);
  std::atomic<bool> start_benchmark = false;
  std::atomic<uint64_t> submission_retries = 0;

  std::vector<std::thread> submitters;
  submitters.reserve(NUM_THREADS);
  for (uint32_t thread_idx = 0; thread_idx < NUM_THREADS; ++thread_idx) {
    submitters.emplace_back([&, thread_idx]() {
      while (!start_benchmark.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }

      TransferRequest thread_request;
      for (uint32_t request_idx = 0; request_idx < REQUESTS_PER_THREAD; ++request_idx) {
        const uint32_t global_idx = thread_idx * REQUESTS_PER_THREAD + request_idx;
        thread_request.address = static_cast<uint64_t>(global_idx) * RECORD_SIZE;
        thread_request.bytes = RECORD_SIZE;
        thread_request.req_id = static_cast<uint16_t>(global_idx + 1);
        thread_request.is_read = true;

        while (!client.SubmitRequest(thread_request, nullptr, 0)) {
          submission_retries.fetch_add(1, std::memory_order_relaxed);
          std::this_thread::yield();
        }

        sent_req_ids[global_idx] = thread_request.req_id;
      }
    });
  }

  std::thread completion_reader([&]() {
    while (!start_benchmark.load(std::memory_order_acquire)) {
      std::this_thread::yield();
    }

    uint32_t received = 0;
    while (received < TOTAL_REQUESTS) {
      auto completion = client.PollCompletions();
      if (completion.response == nullptr) {
        std::this_thread::yield();
        continue;
      }

      received_req_ids[received] = completion.response->req_id;
      received++;
    }
  });

  auto start = std::chrono::high_resolution_clock::now();
  start_benchmark.store(true, std::memory_order_release);

  for (auto& submitter : submitters) {
    submitter.join();
  }
  completion_reader.join();

  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
  auto throughput = duration == 0 ? 0.0 : static_cast<double>(TOTAL_REQUESTS) / duration;

  std::cout << "Time taken: " << duration << " us\n";
  std::cout << "Completed " << TOTAL_REQUESTS << " requests with " << NUM_THREADS
            << " submitter threads and 1 completion thread\n";
  std::cout << "Submission retries: " << submission_retries.load(std::memory_order_relaxed) << '\n';
  std::cout << "Throughput: " << throughput << " Mops\n";

  std::sort(sent_req_ids.begin(), sent_req_ids.end());
  std::sort(received_req_ids.begin(), received_req_ids.end());

  uint32_t mismatches = 0;
  for (uint32_t i = 0; i < TOTAL_REQUESTS; ++i) {
    if (sent_req_ids[i] != received_req_ids[i]) {
      std::cout << "Sent reqID: " << sent_req_ids[i] << " but got: " << received_req_ids[i] << '\n';
      mismatches++;
      break;
    }
  }

  CHECK(mismatches == 0);
}



// TODO: add the following test cases
// 1. Multiple threads sending transfer requests and polling completions on multiple threads (needs code changes)

} // namespace offload
} // namespace dpf