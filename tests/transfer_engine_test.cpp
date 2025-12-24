#include "PD3/buffer/types.hpp"
#include "catch2/catch_test_macros.hpp"

#include "PD3/transfer_engine/transfer_engine.hpp"
#include "PD3/transfer_engine/types.hpp"
#include "PD3/prefetcher/types.hpp"

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
  config.server_addr = "10.10.1.100";
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
  transfer_engine.AddPrefetchRequestQueue(&prefetch_req_batch_q);
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
  config.server_addr = "10.10.1.100";
  config.server_port = "51216";
  config.num_slots = 8;
  config.num_prefetch_slots = 8;
  config.slot_size = 8192 * 2;
  transfer_engine.InitializeNoDma(config);

  const size_t QUEUE_DEPTH = 2048;
  const size_t BATCH_SIZE = 500;
  size_t VALUE_SIZE = 8;

  PrefetchRequestBatchQueue prefetch_req_batch_q(QUEUE_DEPTH);
  transfer_engine.AddPrefetchRequestQueue(&prefetch_req_batch_q);
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

  std::this_thread::sleep_for(std::chrono::seconds(5));

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
  config.server_addr = "10.10.1.100";
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
  transfer_engine.AddPrefetchRequestQueue(&prefetch_req_batch_q);
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
  config.server_addr = "10.10.1.100";
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
  transfer_engine.AddPrefetchRequestQueue(&prefetch_req_batch_q);
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

  std::this_thread::sleep_for(std::chrono::seconds(1));

  transfer_engine.Run();

  std::this_thread::sleep_for(std::chrono::seconds(5));

  transfer_engine.Stop();

  for (size_t i = 0; i < QUEUE_DEPTH; i++) {
    delete[] prefetch_req_batches[i]->keys;
    delete prefetch_req_batches[i];
  }

  return;
}

TEST_CASE("TransferEngine Prefetching Backpressure Correctness", "[backpressure]")
{
  TransferEngine transfer_engine;
  TransferEngineConfig config;

  // rdma server config params
  config.server_addr = "10.10.1.100";
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

  const size_t QUEUE_DEPTH = 1000;
  const size_t BATCH_SIZE = 500;
  size_t VALUE_SIZE = 8;

  PrefetchRequestBatchQueue prefetch_req_batch_q(QUEUE_DEPTH);
  transfer_engine.AddPrefetchRequestQueue(&prefetch_req_batch_q);
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

  std::this_thread::sleep_for(std::chrono::seconds(60));

  transfer_engine.Stop();
}

} // namespace offload
} // namespace dpf