#include "catch2/catch_test_macros.hpp"

#include "common/spmc_buffer.hpp"

#include <thread>
#include <vector>
#include <map>
#include <iostream>

namespace dpf {

struct CacheAlignedVector {
  std::vector<std::string> data;
  char buf[64 - sizeof(std::vector<std::string>)];
};

static_assert(sizeof(CacheAlignedVector) == 64, "CacheAlignedVector should be 64 bytes");

TEST_CASE("SpmcBuffer Construction", "[spmc_buffer]") {
  // common::SpmcBuffer spmc_buffer;
  auto spmc_buffer = std::make_unique<common::SpmcBuffer>();
  spmc_buffer->Initialize();
}

TEST_CASE("SpmcBuffer Reserve", "[spmc_buffer][1]") {
  // common::SpmcBuffer spmc_buffer;
  auto spmc_buffer = std::make_unique<common::SpmcBuffer>();
  spmc_buffer->Initialize();

  auto res = spmc_buffer->Reserve(10);
  REQUIRE(res.buffer_1 != nullptr);
  REQUIRE(res.size_1 == 10);
  REQUIRE(res.buffer_2 == nullptr);
  REQUIRE(res.size_2 == 0);

  // check the buffer is correct
  auto buffer = spmc_buffer->buffer();
  auto header = reinterpret_cast<const common::RecordHeader*>(buffer);
  REQUIRE(header->consumer_state.load(std::memory_order_relaxed) == common::CONSUMER_STATE_RESERVED);
  REQUIRE(header->size == 32); // record header size + msg_size aligned up

  // check the internal state
  REQUIRE(spmc_buffer->producer_index() == 0); // not committed yet
  REQUIRE(spmc_buffer->consumer_index() == 0);
  REQUIRE(spmc_buffer->reservation_index() == 16 + sizeof(common::RecordHeader));
}

TEST_CASE("SpmcBuffer Reserve and Commit", "[spmc_buffer][1]") {
  // common::SpmcBuffer spmc_buffer;
  auto spmc_buffer = std::make_unique<common::SpmcBuffer>();
  spmc_buffer->Initialize();

  // reserve 10 bytes
  auto res = spmc_buffer->Reserve(10);
  REQUIRE(res.buffer_1 != nullptr);
  REQUIRE(res.size_1 == 10);
  REQUIRE(res.buffer_2 == nullptr);
  REQUIRE(res.size_2 == 0);
  CHECK(res.commit_idx == 0);

  // reserve another 20 bytes
  auto res2 = spmc_buffer->Reserve(20);
  REQUIRE(res2.buffer_1 != nullptr);
  REQUIRE(res2.size_1 == 20);
  REQUIRE(res2.buffer_2 == nullptr);
  REQUIRE(res2.size_2 == 0);
  CHECK(res2.commit_idx == 16 + sizeof(common::RecordHeader));
  
  // check the buffer
  auto buffer = spmc_buffer->buffer();
  auto header = reinterpret_cast<const common::RecordHeader*>(buffer);
  REQUIRE(header->consumer_state.load(std::memory_order_relaxed) == common::CONSUMER_STATE_RESERVED);
  REQUIRE(header->size == 32);

  auto header2 = reinterpret_cast<const common::RecordHeader*>(buffer + 16 + sizeof(common::RecordHeader));
  REQUIRE(header2->consumer_state.load(std::memory_order_relaxed) == common::CONSUMER_STATE_RESERVED);
  REQUIRE(header2->size == 40);

  // check the internal state
  REQUIRE(spmc_buffer->producer_index() == 0);
  REQUIRE(spmc_buffer->consumer_index() == 0);
  REQUIRE(spmc_buffer->reservation_index() == 72);

  // commit the second 20 bytes
  spmc_buffer->Commit(res2.commit_idx);

  // check the buffer
  header2 = reinterpret_cast<const common::RecordHeader*>(buffer + 16 + sizeof(common::RecordHeader));
  REQUIRE(header2->consumer_state.load(std::memory_order_relaxed) == common::CONSUMER_STATE_WRITTEN);
  REQUIRE(header2->size == 40);
  // check the internal state
  REQUIRE(spmc_buffer->producer_index() == 0);
  REQUIRE(spmc_buffer->consumer_index() == 0);
  REQUIRE(spmc_buffer->reservation_index() == 40 + 2 * sizeof(common::RecordHeader));

  // commit the first 10 bytes
  spmc_buffer->Commit(res.commit_idx);

  // check the buffer
  header = reinterpret_cast<const common::RecordHeader*>(buffer);
  REQUIRE(header->consumer_state.load(std::memory_order_relaxed) == common::CONSUMER_STATE_WRITTEN);
  REQUIRE(header->size == 32);
  header2 = reinterpret_cast<const common::RecordHeader*>(buffer + 16 + sizeof(common::RecordHeader));
  REQUIRE(header2->consumer_state.load(std::memory_order_relaxed) == common::CONSUMER_STATE_WRITTEN);
  REQUIRE(header2->size == 40);

  // check the internal state
  REQUIRE(spmc_buffer->producer_index() == 32 + 40);
  REQUIRE(spmc_buffer->consumer_index() == 0);
  REQUIRE(spmc_buffer->reservation_index() == 32 + 40);
}

TEST_CASE("SpmcBuffer Consume", "[spmc_buffer][1]") {
  auto spmc_buffer = std::make_unique<common::SpmcBuffer>();
  spmc_buffer->Initialize();

  std::string data = "hello world";
  // reserve 10 bytes
  auto res = spmc_buffer->Reserve(data.size());
  REQUIRE(res.buffer_1 != nullptr);
  REQUIRE(res.size_1 == data.size());
  REQUIRE(res.buffer_2 == nullptr);
  REQUIRE(res.size_2 == 0);
  // write the data
  memcpy(res.buffer_1, data.data(), data.size());
  spmc_buffer->Commit(res.commit_idx);

  // consume the data
  auto result = spmc_buffer->Front();
  REQUIRE(result.buffer_1 != nullptr);
  // REQUIRE(result.size_1 == data.size());
  REQUIRE(result.size_1 == 16 /*11 aligned up*/);
  REQUIRE(result.buffer_2 == nullptr);
  REQUIRE(result.size_2 == 0);
  REQUIRE(result.pop_idx == 0);
  REQUIRE(std::string(result.buffer_1) == data);

  // produce another message
  auto res2 = spmc_buffer->Reserve(data.size());
  REQUIRE(res2.buffer_1 != nullptr);
  REQUIRE(res2.size_1 == data.size());
  REQUIRE(res2.buffer_2 == nullptr);
  REQUIRE(res2.size_2 == 0);
  memcpy(res2.buffer_1, data.data(), data.size());
  spmc_buffer->Commit(res2.commit_idx);

  // consume the second message
  auto result2 = spmc_buffer->Front();
  REQUIRE(result2.buffer_1 != nullptr);
  REQUIRE(result2.size_1 == 16);
  REQUIRE(result2.buffer_2 == nullptr);
  REQUIRE(result2.size_2 == 0);
  REQUIRE(result2.pop_idx == 16 + sizeof(common::RecordHeader));
  REQUIRE(std::string(result2.buffer_1) == data);

  // pop the second message
  spmc_buffer->Pop(result2.pop_idx);
  REQUIRE(spmc_buffer->consumer_index() == 0);

  spmc_buffer->Pop(result.pop_idx);
  REQUIRE(spmc_buffer->consumer_index() == 64);
}

TEST_CASE("SpmcBuffer Wrap Around", "[spmc_buffer][2]") {
  auto spmc_buffer = std::make_unique<common::SpmcBuffer>();
  spmc_buffer->Initialize();

  std::string data = "hello world";
  // reserve 11 bytes
  auto res = spmc_buffer->Reserve(data.size());
  REQUIRE(res.buffer_1 != nullptr);
  REQUIRE(res.size_1 == data.size());
  REQUIRE(res.buffer_2 == nullptr);
  REQUIRE(res.size_2 == 0);
  memcpy(res.buffer_1, data.data(), data.size());
  spmc_buffer->Commit(res.commit_idx);

  // consume the data
  auto result = spmc_buffer->Front();
  REQUIRE(result.buffer_1 != nullptr);
  REQUIRE(result.size_1 == 16);
  REQUIRE(result.buffer_2 == nullptr);
  REQUIRE(result.size_2 == 0);
  REQUIRE(result.pop_idx == 0);
  REQUIRE(std::string(result.buffer_1, data.size()) == data);
  spmc_buffer->Pop(result.pop_idx);

  // producer idx and consumer idx should now be at 27

  // produce another message that wraps around
  size_t size = PD3_RING_BUFFER_SIZE - 15 - sizeof(common::RecordHeader);
  auto aligned_size = size + (8 - size % 8);
  auto res2 = spmc_buffer->Reserve(size);
  REQUIRE(res2.buffer_1 != nullptr);
  REQUIRE(res2.size_1 == PD3_RING_BUFFER_SIZE - 48);
  REQUIRE(res2.buffer_2 != nullptr);
  REQUIRE(res2.size_2 == aligned_size - res2.size_1);
  memcpy(res2.buffer_1, data.data(), data.size());
  spmc_buffer->Commit(res2.commit_idx);

  // consume the data
  auto result2 = spmc_buffer->Front();
  REQUIRE(result2.buffer_1 != nullptr);
  REQUIRE(result2.size_1 == res2.size_1);
  REQUIRE(result2.buffer_2 != nullptr);
  REQUIRE(result2.size_2 == res2.size_2);
  REQUIRE(result2.pop_idx == 32);
  spmc_buffer->Pop(result2.pop_idx);
}

// TEST_CASE("SpmcBuffer Stress Test", "[spmc_buffer]") {
//   common::SpmcBuffer spmc_buffer;
//   spmc_buffer.Initialize();
  
//   uint64_t num_consumers = 10;
//   std::vector<std::thread> consumers;
//   std::vector<CacheAlignedVector> consumer_buffers;

//   std::atomic<bool> done(false);
//   // consumer threads
//   for (uint64_t i = 0; i < num_consumers; ++i) {
//     consumers.emplace_back([&](int id) {
//       auto& consumer_buffer = consumer_buffers[id];
//       while (!done.load(std::memory_order_relaxed)) {
//         auto [buffer, size] = spmc_buffer.Front();
//         if (buffer == nullptr) {
//           continue;
//         }
//         auto header = reinterpret_cast<common::RecordHeader*>(buffer);
//       }
//     }, i);
//   }

//   std::map<std::string, bool> consumed_strings;
//   std::map<std::string, bool> produced_strings;
//   // producer thread
//   std::thread producer([&]() {
//     while (!done.load(std::memory_order_relaxed)) {
//       // Generate random string of 10-100 bytes
//       size_t size = 10 + (rand() % 91);
//       std::string data;
//       data.reserve(size);
//       for (size_t i = 0; i < size; i++) {
//         data += 'a' + (rand() % 26);
//       }

//       // Try to reserve space and write the data
//       auto res = spmc_buffer.Reserve(size + sizeof(common::RecordHeader));
//       if (res.buffer_1 != nullptr) {
//         auto header = reinterpret_cast<common::RecordHeader*>(res.buffer_1);
//         header->size = size;
//         header->consumer_state.store(common::CONSUMER_STATE_WRITTEN, std::memory_order_release);
//         memcpy(res.buffer_1 + sizeof(common::RecordHeader), data.data(), size);
//         spmc_buffer.Commit();
//       }
//     }
//   });

//   std::this_thread::sleep_for(std::chrono::seconds(5));
//   done.store(true, std::memory_order_relaxed);

//   for (auto& consumer : consumers) {
//     consumer.join();
//   }
//   producer.join();

//   // make some assertions
// }

TEST_CASE("AgentBuffer Produce/Consume", "[agent_buffer]") {
  auto agent_buffer = std::make_unique<common::AgentBuffer>();
  agent_buffer->Initialize();

  std::string data = "hello world";

  agent_buffer->Produce(data.data(), data.size());

  auto buffer = agent_buffer->buffer();
  auto header = reinterpret_cast<const common::RecordHeader*>(buffer);
  REQUIRE(header->consumer_state.load(std::memory_order_relaxed) == common::CONSUMER_STATE_WRITTEN);
  REQUIRE(header->size == 32);
  REQUIRE(std::string(buffer + sizeof(common::RecordHeader), data.size()) == data);
}

TEST_CASE("AgentBuffer Produce/Consumer Wrap Around", "[agent_buffer]") {
  auto agent_buffer = std::make_unique<common::AgentBuffer>();
  agent_buffer->Initialize();

  char* data = new char[PD3_RING_BUFFER_SIZE - 32];
  CHECK(agent_buffer->Produce(data, PD3_RING_BUFFER_SIZE - 32));
  CHECK(!agent_buffer->Produce(data, PD3_RING_BUFFER_SIZE - 32));
  
  auto buffer = agent_buffer->buffer();
  uint64_t consumer_idx = 0;
  auto header = reinterpret_cast<common::RecordHeader*>(buffer + consumer_idx);
  CHECK(header->consumer_state.load(std::memory_order_relaxed) == common::CONSUMER_STATE_WRITTEN);
  CHECK(header->size == PD3_RING_BUFFER_SIZE - 16);
  uint64_t expected_state = common::CONSUMER_STATE_WRITTEN;
  header->consumer_state.store(common::CONSUMER_STATE_COMPLETED, std::memory_order_relaxed);
  consumer_idx += header->size;

  CHECK(agent_buffer->Produce(data, PD3_RING_BUFFER_SIZE - 32));

  auto remaining_space = PD3_RING_BUFFER_SIZE - common::AgentBuffer::GetBufferIdx(consumer_idx);
  if (remaining_space < sizeof(common::RecordHeader)) {
    consumer_idx += remaining_space;
  }
  header = reinterpret_cast<common::RecordHeader*>(buffer + consumer_idx);
  CHECK(header->consumer_state.load(std::memory_order_relaxed) == common::CONSUMER_STATE_WRITTEN);
  CHECK(header->size == PD3_RING_BUFFER_SIZE - 16);
}

TEST_CASE("AgentBuffer Produce/Consumer Stress Test", "[agent_buffer][1]") {
  auto agent_buffer = std::make_unique<common::AgentBuffer>();
  agent_buffer->Initialize();

  static constexpr uint64_t TOTAL_RECORDS = 10'000'000;
  // static constexpr uint64_t TOTAL_RECORDS = 100;
  std::thread producer([&]() {
    uint64_t to_produce = 0;
    for (int i = 0; i < TOTAL_RECORDS; i++) {
      while (!agent_buffer->Produce(reinterpret_cast<const char*>(&to_produce), sizeof(to_produce))) {
        std::this_thread::yield();
      }
      to_produce++;
    }
  });

  std::thread consumer([&]() {
    uint64_t consumer_idx = 0;
    uint64_t expected_to_consume = 0;
    auto buffer = agent_buffer->buffer();
    for (int i = 0; i < TOTAL_RECORDS; i++) {
      size_t remaining_space = PD3_RING_BUFFER_SIZE - common::AgentBuffer::GetBufferIdx(consumer_idx);
      if (remaining_space < sizeof(common::RecordHeader)) {
        consumer_idx += remaining_space;
      }
      auto header = reinterpret_cast<common::RecordHeader*>(buffer + common::AgentBuffer::GetBufferIdx(consumer_idx));
      while (header->consumer_state.load(std::memory_order_acquire) != common::CONSUMER_STATE_WRITTEN) {
        std::this_thread::yield();
      }
      if (header->size != 24) {
        std::cerr << "header->size: " << header->size << std::endl;
        return;
      }
      header->consumer_state.store(common::CONSUMER_STATE_COMPLETED, std::memory_order_relaxed);
      auto actual_to_consume = *reinterpret_cast<uint64_t*>(buffer + common::AgentBuffer::GetBufferIdx(consumer_idx + sizeof(common::RecordHeader)));
      if (actual_to_consume != expected_to_consume) {
        std::cerr << "expected_to_consume: " << expected_to_consume << " actual_to_consume: " << actual_to_consume << std::endl;
        return;
      }
      // CHECK(actual_to_consume == expected_to_consume);
      expected_to_consume++;
      consumer_idx += header->size;      
    }
  });

  producer.join();
  std::cout << "producer done" << std::endl;
  consumer.join();
}

}