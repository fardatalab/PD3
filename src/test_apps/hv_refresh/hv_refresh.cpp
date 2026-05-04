#include "PD3/buffer/types.hpp"
#include "PD3/transfer_engine/transfer_client.hpp"
#include "PD3/transfer_engine/types.hpp"
#include "PD3/prefetcher/types.hpp"

#include <iostream>
#include <strings.h>
#include <thread>
#include <chrono>
#include <atomic>
#include <unistd.h>
#include <vector>
#include <algorithm>

using namespace dpf;
using namespace dpf::offload;

void TestOne(TransferClient& client)
{
  // 10 write requests
  const int NUM_ITEMS = 10;
  uint64_t address = 0;
  size_t data_size = 16;
  uint16_t req_id = 0;
  char data[16];
  uint64_t key = 1;
  uint64_t val = 200;
  for (int i = 0; i < NUM_ITEMS; ++i) {
    TransferRequest request;
    request.address = address;
    address += 16; // next one
    request.bytes = data_size;
    request.req_id = req_id++;
    request.is_read = false;
    *reinterpret_cast<uint64_t*>(data) = key++;
    *reinterpret_cast<uint64_t*>(data + sizeof(uint64_t)) = val++;
    client.SubmitRequest(request, data, data_size);
  }
  CompletionOutput output;
  for (int i = 0; i < NUM_ITEMS; ++i) {
    while (true) {
      output = client.PollCompletions();
      if (output.response == nullptr) {
        std::this_thread::yield();
      } else {
        break;
      }
    }
    std::cout << "Received completion: " << i << '\n';
  }
  std::cout << "Received all completions\n";
}

void TestTwo(TransferClient& client)
{
  // 10 read requests after 10 write requests
  TestOne(client);
  const int NUM_ITEMS = 10;
  uint64_t address = 0;
  size_t data_size = 16;
  uint16_t req_id = 0;
  char data[16];
  *reinterpret_cast<uint64_t*>(data) = 10;
  uint64_t key = 1;
  uint64_t val = 200;
  for (int i = 0; i < NUM_ITEMS; ++i) {
    TransferRequest request;
    request.address = address;
    address += 16; // next one
    request.bytes = data_size;
    request.req_id = req_id++;
    request.is_read = true;
    client.SubmitRequest(request, nullptr, 0);
    // request.is_read = false;
    // client.SubmitRequest(request, data, data_size);
  }
  CompletionOutput output;
  for (int i = 0; i < NUM_ITEMS; ++i) {
    while (true) {
      output = client.PollCompletions();
      if (output.response == nullptr) {
        std::this_thread::yield();
      } else {
        break;
      }
    }
    std::cout << "Received completion: " << i << '\n';
  }
  std::cout << "Received all completions\n";
}

void TestThree(TransferClient& client)
{
  // 10 write requests
  const int NUM_ITEMS = 10;
  uint64_t address = 0;
  size_t data_size = 16;
  uint16_t req_id = 0;
  char data[16];
  uint64_t key = 1;
  uint64_t val = 200;
  for (int i = 0; i < NUM_ITEMS; ++i) {
    TransferRequest request;
    request.address = address;
    address += 16; // next one
    request.bytes = data_size;
    request.req_id = req_id++;
    request.is_read = false;
    *reinterpret_cast<uint64_t*>(data) = key++;
    *reinterpret_cast<uint64_t*>(data + sizeof(uint64_t)) = val++;
    client.SubmitRequest(request, data, data_size);
  }
  // 10 read requests
  address = 0;
  req_id = 1;
  for (int i = 0; i < NUM_ITEMS; ++i) {
    TransferRequest request;
    request.address = address;
    address += 16; // next one
    request.bytes = data_size;
    request.req_id = req_id++;
    request.is_read = true;
    client.SubmitRequest(request, nullptr, 0);
    // request.is_read = false;
    // client.SubmitRequest(request, data, data_size);
  }
  CompletionOutput output;
  for (int i = 0; i < NUM_ITEMS*2; ++i) {
    while (true) {
      output = client.PollCompletions();
      if (output.response == nullptr) {
        std::this_thread::yield();
      } else {
        break;
      }
    }
    std::cout << "Received completion: " << i << '\n';
  }
  std::cout << "Received all read and write completions\n";
}

int main(int argc, char** argv)
{
  std::cout << "Hello from hv_refresh\n";

  if (argc != 2) {
    std::cout << "Usage: ./hv_refresh [test_num]\n";
    return -1;
  }

  int test_num = std::atoi(argv[1]);

  TransferClient client;
  TransferClientConfig config;
  config.pcie_address = "21:00.0";
  config.mmap_export_path = "/data/ssankhe/tc_export_desc";
  config.buf_details_path = "/data/ssankhe/tc_buf_details.txt";
  client.Initialize(config);

  switch (test_num) {
    case 1: {
      TestOne(client);
      break;
    }
    case 2: {
      TestTwo(client);
      break;
    }
    case 3: {
      TestThree(client);
      break;
    }
    default: {
      std::cout << "Test: " << test_num << " is not supported\n";
    }
  }
  
  return 0;
}