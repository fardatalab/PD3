#include "PD3/buffer/types.hpp"
#include "catch2/catch_test_macros.hpp"

#include "PD3/transfer_engine/transfer_client.hpp"
#include "PD3/transfer_engine/types.hpp"
#include "PD3/prefetcher/types.hpp"

#include <thread>
#include <chrono>
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
  config.pcie_address = "e1:00.0";
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

  // poll completions
  // TODO: get the data pointer
  // TODO: make sure the data pointer is what we want
}

TEST_CASE("TransferClient Stress Test", "[transfer_client][stress][s]")
{
  TransferClient client;
  TransferClientConfig config;
  config.pcie_address = "e1:00.0";
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
}

TEST_CASE("TransferClient Stress Test Many Requests", "[transfer_client][stress][xl]")
{
  TransferClient client;
  TransferClientConfig config;
  config.pcie_address = "e1:00.0";
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

  uint16_t req_id = 2;

  // const uint64_t NUM_REQUESTS = 500 * 4096;
  const uint64_t NUM_REQUESTS = 500 * 4096;
  const uint32_t RECORD_SIZE = 16;
  const uint32_t BATCH_SIZE = 2 * 4096;

  std::vector<uint16_t> received_req_ids;
  received_req_ids.resize(NUM_REQUESTS);
  std::vector<uint16_t> sent_req_ids;
  sent_req_ids.resize(NUM_REQUESTS);
  auto received_req_ids_idx = 0;

  auto start = std::chrono::high_resolution_clock::now();
  uint16_t expected_req_id = 2;
  uint64_t sent = 0;
  for (int i = 0; i < NUM_REQUESTS; ++i) {
    request.address = 0;
    request.bytes = RECORD_SIZE;
    request.req_id = req_id++;
    request.is_read = true;
    client.SubmitRequest(request, nullptr, 0);
    sent_req_ids[i] = request.req_id;
    sent++;
    if (sent % BATCH_SIZE == 0) {
      // poll completions
      uint32_t num_completions = 0;
      while (num_completions < BATCH_SIZE) {
        output = client.PollCompletions();
        if (output.response == nullptr) {
          std::this_thread::yield();
        } else {
          received_req_ids[received_req_ids_idx++] = output.response->req_id;
          num_completions++;
        }
      }
    }
  }

  auto end = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
  std::cout << "Time taken: " << duration << " us\n";
  
  std::cout << "Completed " << NUM_REQUESTS << " requests\n";
  std::cout << "Throughput: " << (double)NUM_REQUESTS / duration << " Mops\n";

  std::sort(sent_req_ids.begin(), sent_req_ids.end());
  std::sort(received_req_ids.begin(), received_req_ids.end());
  for (int i = 0; i < NUM_REQUESTS; ++i) {
    if (sent_req_ids[i] != received_req_ids[i]) {
      std::cout << "Sent reqID: " << sent_req_ids[i] << " but got: " << received_req_ids[i] << '\n';
    }
  }
}

} // namespace offload
} // namespace dpf