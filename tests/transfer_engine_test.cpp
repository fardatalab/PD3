#include "PD3/buffer/types.hpp"
#include "PD3/transfer_engine/transfer_engine.hpp"
#include "PD3/transfer_engine/types.hpp"
#include "PD3/prefetcher/refresher.hpp"
#include "PD3/prefetcher/types.hpp"

#include "catch2/catch_test_macros.hpp"

#include "types/rdma_protocol.hpp"

#include <thread>
#include <chrono>
#include <unistd.h>

namespace dpf {
namespace offload {

/**
 IMPORTANT: please make sure the PD3 server is running on remote memory before running these tests.
 They will fail if the PD3 server is not running.
*/

TEST_CASE("TransferEngine Construction", "[transfer_engine]") {
  TransferEngine transfer_engine;
}

TEST_CASE("TransferEngine Prefetching no DMA", "[transfer_engine]") {
  TransferEngine transfer_engine;
  TransferEngineConfig config;
  config.use_rdma = true;
  config.local_addr = "10.10.1.203"; // this is on the DPU
  config.server_addr = "10.10.1.101";
  config.server_port = "51216";
  config.num_slots = 8;
  config.num_prefetch_slots = 8;
  config.slot_size = 8192 + 8;
  config.testing_mode = true;
  transfer_engine.InitializeNoDma(config);

  const size_t QUEUE_DEPTH = 1024;
  const size_t BATCH_SIZE = 256;
  size_t VALUE_SIZE = 8;

  PrefetchRequestBatchQueue prefetch_req_batch_q(1024);
  transfer_engine.SetPrefetchRequestQueue(&prefetch_req_batch_q);
  std::vector<PrefetchRequestBatch*> prefetch_req_batches(QUEUE_DEPTH);

  for (size_t i = 0; i < QUEUE_DEPTH; i++) {
    prefetch_req_batches[i] = new PrefetchRequestBatch();
    prefetch_req_batches[i]->size = 0;
    prefetch_req_batches[i]->keys = new uint64_t[BATCH_SIZE];
  }

  transfer_engine.RunNoDma();

  std::this_thread::sleep_for(std::chrono::seconds(1));

  const size_t NUM_PREFETCH_BATCHES = 7;
  for (size_t i = 0; i < NUM_PREFETCH_BATCHES; i++) {
    for (size_t j = 0; j < BATCH_SIZE; j++) {
      prefetch_req_batches[i]->keys[j] = i * BATCH_SIZE + j;
    }
    prefetch_req_batches[i]->size = BATCH_SIZE;
    prefetch_req_batch_q.push(prefetch_req_batches[i]);
  }

  std::this_thread::sleep_for(std::chrono::seconds(2));
  size_t record_size = VALUE_SIZE + offload::kPrefetchResponseSize;

  std::cout << "Checking prefetch results" << std::endl;
  for (size_t i = 0; i < NUM_PREFETCH_BATCHES; i++) {
    auto slot = transfer_engine.GetSlot(i, true);
    CHECK(slot->status == SlotStatus::COMPLETED);
    auto msg_size = *reinterpret_cast<uint32_t*>(slot->resp);
    CHECK(msg_size == BATCH_SIZE * record_size);
    size_t offset = sizeof(uint32_t);
    for (size_t j = 0; j < BATCH_SIZE; j++) {
      auto response = reinterpret_cast<PrefetchResponse*>(slot->resp + offset);
      CHECK(response->key == i * BATCH_SIZE + j);
      CHECK(response->consumer_state == dpf::buffer::CONSUMER_STATE_WRITTEN);
      CHECK(response->size == record_size);
      offset += record_size;
    }
  }

  transfer_engine.Stop();

  for (size_t i = 0; i < QUEUE_DEPTH; i++) {
    delete[] prefetch_req_batches[i]->keys;
    delete prefetch_req_batches[i];
  }

  return;
}

TEST_CASE("TransferEngine Prefetching Throughput", "[transfer_engine_tput]")
{
  TransferEngine transfer_engine;
  TransferEngineConfig config;
  config.use_rdma = true;
  config.local_addr = "10.10.1.203"; // this is on the DPU
  config.server_addr = "10.10.1.101";
  config.server_port = "51216";
  config.num_slots = 8;
  config.num_prefetch_slots = 8;
  config.slot_size = 8192 * 2;
  transfer_engine.InitializeNoDma(config);

  const size_t QUEUE_DEPTH = 2048;
  const size_t BATCH_SIZE = 500;
  size_t VALUE_SIZE = 8;

  PrefetchRequestBatchQueue prefetch_req_batch_q(QUEUE_DEPTH);
  transfer_engine.SetPrefetchRequestQueue(&prefetch_req_batch_q);
  std::vector<PrefetchRequestBatch*> prefetch_req_batches(QUEUE_DEPTH);

  for (size_t i = 0; i < QUEUE_DEPTH; i++) {
    prefetch_req_batches[i] = new PrefetchRequestBatch();
    prefetch_req_batches[i]->size = 0;
    prefetch_req_batches[i]->keys = new uint64_t[BATCH_SIZE];
  }

  const size_t NUM_PREFETCH_BATCHES = QUEUE_DEPTH;
  for (size_t i = 0; i < NUM_PREFETCH_BATCHES; i++) {
    for (size_t j = 0; j < BATCH_SIZE; j++) {
      prefetch_req_batches[i]->keys[j] = i * BATCH_SIZE + j;
    }
    prefetch_req_batches[i]->size = BATCH_SIZE;
    prefetch_req_batch_q.push(prefetch_req_batches[i]);
  }

  std::this_thread::sleep_for(std::chrono::seconds(1));

  transfer_engine.RunNoDma();

  std::this_thread::sleep_for(std::chrono::seconds(10));

  transfer_engine.Stop();

  for (size_t i = 0; i < QUEUE_DEPTH; i++) {
    delete[] prefetch_req_batches[i]->keys;
    delete prefetch_req_batches[i];
  }

  return;
}

TEST_CASE("TransferEngine Prefetching with DMA Correctness", "[transfer_engine_dma]") {
  // IMPORTANT: this must be run on the DPU
  TransferEngine transfer_engine;
  TransferEngineConfig config;

  // rdma server config params
  config.local_addr = "10.10.1.203"; // this is on the DPU
  config.server_addr = "10.10.1.101";
  config.server_port = "51216";
  config.max_wr = 1024;
  config.use_rdma = true;

  // dpu config params
  config.dpu_pcie_addr = "03:00.0";

  // dma config params
  config.export_desc_client_file_path = "/home/ubuntu/export_desc_client";
  config.export_desc_agent_file_path = "/home/ubuntu/agent_export_desc";
  config.buf_client_file_path = "/home/ubuntu/buf_client.txt";
  config.buf_agent_file_path = "/home/ubuntu/agent_buf_details.txt";
  config.enable_transfer = false;
  config.enable_agent = true;

  // buffer config params
  config.num_slots = 8;
  config.num_prefetch_slots = 8;
  config.slot_size = 8192 * 2;

  transfer_engine.Initialize(config);

  std::this_thread::sleep_for(std::chrono::seconds(10));

  const size_t QUEUE_DEPTH = 256;
  const size_t BATCH_SIZE = 16;
  size_t VALUE_SIZE = 8;

  PrefetchRequestBatchQueue prefetch_req_batch_q(QUEUE_DEPTH);
  transfer_engine.SetPrefetchRequestQueue(&prefetch_req_batch_q);
  std::vector<PrefetchRequestBatch*> prefetch_req_batches(QUEUE_DEPTH);

  for (size_t i = 0; i < QUEUE_DEPTH; i++) {
    prefetch_req_batches[i] = new PrefetchRequestBatch();
    prefetch_req_batches[i]->size = 0;
    prefetch_req_batches[i]->keys = new uint64_t[BATCH_SIZE];
  }

  const size_t NUM_PREFETCH_BATCHES = QUEUE_DEPTH;
  for (size_t i = 0; i < NUM_PREFETCH_BATCHES; i++) {
    for (size_t j = 0; j < BATCH_SIZE; j++) {
      prefetch_req_batches[i]->keys[j] = i * BATCH_SIZE + j + 1;
    }
    prefetch_req_batches[i]->size = BATCH_SIZE;
    prefetch_req_batch_q.push(prefetch_req_batches[i]);
  }

  transfer_engine.Run();

  std::this_thread::sleep_for(std::chrono::seconds(30));

  transfer_engine.Stop();
}

TEST_CASE("TransferEngine DMA Prefetching Throughput", "[transfer_engine_dma_tput]")
{
  /**
  IMPORTANT: For this test to work, uncomment lines 646, 648-650, 672-676 in transfer_engine.cpp
             Make sure `./bin/mini_agent` is running on the host, and that the `pd3_memory_backend`
             is running on the remote memory node
  */
  TransferEngine transfer_engine;
  TransferEngineConfig config;

  // rdma server config params
  config.local_addr = "10.10.2.201"; // this is on the DPU
  config.server_addr = "10.10.2.100";
  config.server_port = "51216";
  config.max_wr = 1024;
  config.use_rdma = true;

  // dpu config params
  config.dpu_pcie_addr = "03:00.0";

  // dma config params
  config.export_desc_client_file_path = "/home/ubuntu/export_desc_client";
  config.export_desc_agent_file_path = "/home/ubuntu/agent_export_desc";
  config.buf_client_file_path = "/home/ubuntu/buf_client.txt";
  config.buf_agent_file_path = "/home/ubuntu/agent_buf_details.txt";
  config.enable_transfer = false;
  config.enable_agent = true;

  // buffer config params
  config.num_slots = 8;
  config.num_prefetch_slots = 8;
  config.slot_size = 8192 * 2;

  transfer_engine.Initialize(config);

  const size_t QUEUE_DEPTH = 2048;
  const size_t BATCH_SIZE = 500;
  size_t VALUE_SIZE = 8;

  PrefetchRequestBatchQueue prefetch_req_batch_q(QUEUE_DEPTH);
  transfer_engine.SetPrefetchRequestQueue(&prefetch_req_batch_q);
  std::vector<PrefetchRequestBatch*> prefetch_req_batches(QUEUE_DEPTH);

  for (size_t i = 0; i < QUEUE_DEPTH; i++) {
    prefetch_req_batches[i] = new PrefetchRequestBatch();
    prefetch_req_batches[i]->size = 0;
    prefetch_req_batches[i]->keys = new uint64_t[BATCH_SIZE];
  }

  const size_t NUM_PREFETCH_BATCHES = QUEUE_DEPTH;
  for (size_t i = 0; i < NUM_PREFETCH_BATCHES; i++) {
    for (size_t j = 0; j < BATCH_SIZE; j++) {
      prefetch_req_batches[i]->keys[j] = i * BATCH_SIZE + j + 1;
      // prefetch_req_batches[i]->keys[j] = rand() % 10'000'000; // 10M keys
    }
    prefetch_req_batches[i]->size = BATCH_SIZE;
    prefetch_req_batch_q.push(prefetch_req_batches[i]);
  }

  std::this_thread::sleep_for(std::chrono::seconds(1));

  transfer_engine.RunWithFixedBatches(QUEUE_DEPTH);

  std::this_thread::sleep_for(std::chrono::seconds(5));

  transfer_engine.Stop();

  for (size_t i = 0; i < QUEUE_DEPTH; i++) {
    delete[] prefetch_req_batches[i]->keys;
    delete prefetch_req_batches[i];
  }

  return;
}

TEST_CASE("TransferEngine DMA Prefetching and TE Reads Throughput", "[transfer_engine_reads][tput]")
{
  /**
  IMPORTANT: This test must be run on the DPU. For this to work, run ./bin/test_agent_and_transfer on the host and copy
             the descriptor files to the DPU
  */
   /**
  IMPORTANT: For this test to work, uncomment lines 646, 648-650, 672-676 in transfer_engine.cpp
             Make sure `./bin/mini_agent` is running on the host, and that the `pd3_memory_backend`
             is running on the remote memory node
  */
  TransferEngine transfer_engine;
  TransferEngineConfig config;

  // rdma server config params
  config.local_addr = "10.10.2.201"; // this is on the DPU
  config.server_addr = "10.10.2.100";
  config.server_port = "51216";
  config.max_wr = 1024;
  config.use_rdma = true;

  // dpu config params
  config.dpu_pcie_addr = "03:00.0";

  // dma config params
  config.export_desc_client_file_path = "/home/ubuntu/tc_export_desc";
  config.export_desc_agent_file_path = "/home/ubuntu/agent_export_desc";
  config.buf_client_file_path = "/home/ubuntu/tc_buf_details.txt";
  config.buf_agent_file_path = "/home/ubuntu/agent_buf_details.txt";
  config.enable_transfer = true;
  config.enable_agent = true;

  // buffer config params
  config.num_slots = 8;
  config.num_prefetch_slots = 8;
  config.slot_size = 8192 * 2;

  transfer_engine.Initialize(config);

  const size_t QUEUE_DEPTH = 8192;
  const size_t BATCH_SIZE = 500;
  size_t VALUE_SIZE = 8;

  HostViewRefresher refresher;
  refresher.Initialize(1, 8192, 500);

  PrefetchRequestBatchQueue prefetch_req_batch_q(QUEUE_DEPTH);
  transfer_engine.SetPrefetchRequestQueue(&prefetch_req_batch_q);
  transfer_engine.SetRefresher(&refresher);
  std::vector<PrefetchRequestBatch*> prefetch_req_batches(QUEUE_DEPTH);

  for (size_t i = 0; i < QUEUE_DEPTH; i++) {
    prefetch_req_batches[i] = new PrefetchRequestBatch();
    prefetch_req_batches[i]->size = 0;
    prefetch_req_batches[i]->keys = new uint64_t[BATCH_SIZE];
  }

  const size_t NUM_PREFETCH_BATCHES = QUEUE_DEPTH;
  for (size_t i = 0; i < NUM_PREFETCH_BATCHES; i++) {
    for (size_t j = 0; j < BATCH_SIZE; j++) {
      prefetch_req_batches[i]->keys[j] = i * BATCH_SIZE + j + 1;
      // prefetch_req_batches[i]->keys[j] = rand() % 10'000'000; // 10M keys
    }
    prefetch_req_batches[i]->size = BATCH_SIZE;
    prefetch_req_batch_q.push(prefetch_req_batches[i]);
  }

  std::this_thread::sleep_for(std::chrono::seconds(1));

  transfer_engine.RunWithFixedBatches(QUEUE_DEPTH);

  std::this_thread::sleep_for(std::chrono::seconds(5));

  transfer_engine.Stop();

  for (size_t i = 0; i < QUEUE_DEPTH; i++) {
    delete[] prefetch_req_batches[i]->keys;
    delete prefetch_req_batches[i];
  }

  // check the refresher
  return;
}

TEST_CASE("TransferEngine Refresh Correctness Write Unit Test", "[transfer_engine_ref_write][u]")
{
  char* buf = new char[64];
  // set up buffer
  uint64_t loff = 0;
  *reinterpret_cast<uint32_t*>(buf + loff) = 48;
  loff += sizeof(uint32_t) + sizeof(ProtocolHeader);
  *reinterpret_cast<uint32_t*>(buf + loff) = 40;
  loff += sizeof(size_t); // this is the size of the request, written by the transfer client (maybe not necessary)
  TransferRequest* req = reinterpret_cast<TransferRequest*>(buf + loff);
  req->address = 0;
  req->bytes = 16;
  req->req_id = 1;
  req->is_read = false;
  *reinterpret_cast<uint64_t*>(buf + loff + sizeof(TransferRequest)) = 1234;

  auto msg_size = *reinterpret_cast<uint32_t*>(buf);
  uint64_t offset = sizeof(uint32_t) + sizeof(ProtocolHeader);
  while (offset < msg_size) {
    auto req_size = *reinterpret_cast<size_t*>(buf + offset);
    auto curr_request = reinterpret_cast<TransferRequest*>(buf + offset + sizeof(size_t));
    if (!curr_request->is_read) {
      auto reqbuf = reinterpret_cast<char*>(curr_request + 1);
      CHECK(*reinterpret_cast<uint64_t*>(reqbuf) == 1234);
    }
    offset += req_size;
  }

  delete[] buf;
}

TEST_CASE("TransferEngine Refresh Test 1", "[transfer_engine_ref][r1]")
{
  // single sharded refresh, 10 requests from the host
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
  config.server_port = "51216";
  config.slot_size = 16384*2;
  config.use_rdma = true;

  HostViewRefresher refresher;
  refresher.Initialize(1, 4, 16);

  engine.SetRefresher(&refresher);

  PrefetchRequestBatchQueue prefetch_req_batch_q{1};
  engine.Initialize(config);
  engine.SetPrefetchRequestQueue(&prefetch_req_batch_q);

  engine.Run();

  auto ref_q = refresher.GetRefreshQueue(0);
  while (true) {
    auto batch_ptr = ref_q->front();
    if (!batch_ptr) {
      // yield
      std::this_thread::yield();
      continue;
    }
    auto batch = *batch_ptr;
    CHECK(!batch->is_read); 
    CHECK(batch->size == 10);
    for (int i = 0; i < 10; ++i) {
      CHECK(batch->requests[i] == i + 1);
    }
    ref_q->pop();
    break; 
  }
  std::this_thread::sleep_for(std::chrono::seconds(2)); // finish all the DMA writes
  engine.Stop();
}

TEST_CASE("TransferEngine Refresh Test 2", "[transfer_engine_ref][r2]")
{
  // single sharded refresh, 10 write requests from the host, 10 read requests in separate batches
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
  config.server_port = "51216";
  config.slot_size = 16384*2;
  config.use_rdma = true;

  HostViewRefresher refresher;
  refresher.Initialize(1, 4, 16);

  engine.SetRefresher(&refresher);

  PrefetchRequestBatchQueue prefetch_req_batch_q{1};
  engine.Initialize(config);
  engine.SetPrefetchRequestQueue(&prefetch_req_batch_q);

  engine.Run();

  auto ref_q = refresher.GetRefreshQueue(0);
  int cnt = 0;
  while (cnt < 2) {
    auto batch_ptr = ref_q->front();
    if (!batch_ptr) {
      // yield
      std::this_thread::yield();
      continue;
    }
    auto batch = *batch_ptr;
    if (cnt == 0) {
      CHECK(!batch->is_read);
    } else if (cnt == 1) {
      CHECK(batch->is_read);
    }
    CHECK(batch->size == 10);
    for (int i = 0; i < 10; ++i) {
      CHECK(batch->requests[i] == i + 1);
    }
    ref_q->pop();
    cnt++;
  }
  std::this_thread::sleep_for(std::chrono::seconds(2)); // finish all the DMA writes
  CHECK(ref_q->front() == nullptr);
  engine.Stop();
}

TEST_CASE("TransferEngine Refresh Test 3", "[transfer_engine_ref][r3]")
{
  // single sharded refresh, 10 write requests, 10 read requests in a single batch
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
  config.server_port = "51216";
  config.slot_size = 16384*2;
  config.use_rdma = true;

  HostViewRefresher refresher;
  refresher.Initialize(1, 4, 16);

  engine.SetRefresher(&refresher);

  PrefetchRequestBatchQueue prefetch_req_batch_q{1};
  engine.Initialize(config);
  engine.SetPrefetchRequestQueue(&prefetch_req_batch_q);

  engine.Run();

  auto ref_q = refresher.GetRefreshQueue(0);
  int cnt = 0;
  while (cnt < 2) {
    auto batch_ptr = ref_q->front();
    if (!batch_ptr) {
      // yield
      std::this_thread::yield();
      continue;
    }
    auto batch = *batch_ptr;
    if (cnt == 0) {
      CHECK(!batch->is_read);
    } else if (cnt == 1) {
      CHECK(batch->is_read);
    }
    CHECK(batch->size == 10);
    for (int i = 0; i < 10; ++i) {
      CHECK(batch->requests[i] == i + 1);
    }
    ref_q->pop();
    cnt++;
  }
  std::this_thread::sleep_for(std::chrono::seconds(2)); // finish all the DMA writes
  CHECK(ref_q->front() == nullptr);
  engine.Stop();
}

} // namespace offload
} // namespace dpf