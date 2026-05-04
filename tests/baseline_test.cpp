#include "catch2/catch_template_test_macros.hpp"
#include "catch2/catch_test_macros.hpp"

#include "PD3/buffer/baselines.hpp"

#include <thread>
#include <vector>
#include <map>
#include <iostream>
#include <random>
#include <atomic>

using namespace dpf::buffer;
using namespace dpf;

namespace {
struct TestItem {
    uint64_t key;
    uint64_t val;
    char data[48];
};
static_assert(sizeof(TestItem) == 64);
}

TEMPLATE_TEST_CASE("AgentBuffer Init", "[baseline][1]", LocklessAgentBuffer, SpinlockAgentBuffer) {
    auto agent_buffer = std::make_unique<TestType>();
    agent_buffer->Initialize();
    REQUIRE(agent_buffer->producer_index() == 0);
    REQUIRE(agent_buffer->consumer_index() == 0);
}

TEMPLATE_TEST_CASE("AgentBuffer Simple Produce/Consume", "[baseline][1]", LocklessAgentBuffer, SpinlockAgentBuffer) {
    auto agent_buffer = std::make_unique<TestType>();
    agent_buffer->Initialize();

    TestItem item = {123, 456};
    char* data_list[] = {reinterpret_cast<char*>(&item)};
    size_t size_list[] = {sizeof(item)};
    int num_inserted = 0;
    
    REQUIRE(agent_buffer->ProduceBatch(data_list, size_list, 1, &num_inserted, sizeof(item)));
    REQUIRE(num_inserted == 1);

    REQUIRE(agent_buffer->CheckAndReturn(123));
    REQUIRE(agent_buffer->consumer_index() > 0);
    
    // Searching for the same key should fail as it's consumed.
    REQUIRE_FALSE(agent_buffer->CheckAndReturn(123));
}

TEMPLATE_TEST_CASE("AgentBuffer Find Second Item", "[baseline][1]", LocklessAgentBuffer, SpinlockAgentBuffer) {
    auto agent_buffer = std::make_unique<TestType>();
    agent_buffer->Initialize();
    
    TestItem item1 = {1, 1};
    TestItem item2 = {2, 2};
    
    char* data_list1[] = {reinterpret_cast<char*>(&item1)};
    size_t size_list1[] = {sizeof(item1)};
    int num_inserted1 = 0;
    REQUIRE(agent_buffer->ProduceBatch(data_list1, size_list1, 1, &num_inserted1, sizeof(item1)));
    REQUIRE(num_inserted1 == 1);
    
    char* data_list2[] = {reinterpret_cast<char*>(&item2)};
    size_t size_list2[] = {sizeof(item2)};
    int num_inserted2 = 0;
    REQUIRE(agent_buffer->ProduceBatch(data_list2, size_list2, 1, &num_inserted2, sizeof(item2)));
    REQUIRE(num_inserted2 == 1);
    
    REQUIRE_FALSE(agent_buffer->CheckAndReturn(3)); // Should not hang, should return false.
    REQUIRE(agent_buffer->CheckAndReturn(2));
    REQUIRE(agent_buffer->consumer_index() == 0); // Item 1 not consumed yet.
    REQUIRE(agent_buffer->CheckAndReturn(1));
    REQUIRE(agent_buffer->consumer_index() > 0);
}

// TEMPLATE_TEST_CASE("AgentBuffer Wrap Around", "[baseline]", LocklessAgentBuffer, SpinlockAgentBuffer) {
//     auto agent_buffer = std::make_unique<TestType>();
//     agent_buffer->Initialize();

//     const size_t item_size = 128;
//     struct BigItem {
//         uint64_t key;
//         char data[item_size - sizeof(uint64_t)];
//     };
//     static_assert(sizeof(BigItem) == item_size, "Incorrect size");
    
//     size_t num_items_to_fill = PD3_RING_BUFFER_SIZE / (item_size + 64) -1;

//     std::vector<BigItem> items(num_items_to_fill);
//     for(size_t i = 0; i < num_items_to_fill; ++i) {
//         items[i].key = i;
//         char* data_list[] = {reinterpret_cast<char*>(&items[i])};
//         size_t size_list[] = {sizeof(BigItem)};
//         int num_inserted = 0;
//         REQUIRE(agent_buffer->ProduceBatch(data_list, size_list, 1, &num_inserted, sizeof(BigItem)));
//         REQUIRE(num_inserted == 1);
//     }
    
//     // Now consume half of them
//     for(size_t i = 0; i < num_items_to_fill / 2; ++i) {
//         REQUIRE(agent_buffer->CheckAndReturn(i));
//     }
    
//     // Now produce more to wrap around
//     std::vector<BigItem> items2(num_items_to_fill);
//     for(size_t i = 0; i < num_items_to_fill / 2; ++i) {
//         items2[i].key = i + num_items_to_fill;
//         char* data_list[] = {reinterpret_cast<char*>(&items2[i])};
//         size_t size_list[] = {sizeof(BigItem)};
//         int num_inserted = 0;
//         REQUIRE(agent_buffer->ProduceBatch(data_list, size_list, 1, &num_inserted, sizeof(BigItem)));
//         REQUIRE(num_inserted == 1);
//     }

//     // Consume the rest
//     for(size_t i = num_items_to_fill / 2; i < num_items_to_fill; ++i) {
//         REQUIRE(agent_buffer->CheckAndReturn(i));
//     }
//     for(size_t i = 0; i < num_items_to_fill / 2; ++i) {
//         REQUIRE(agent_buffer->CheckAndReturn(i + num_items_to_fill));
//     }
// }

TEMPLATE_TEST_CASE("AgentBuffer Multi-threaded Stress Test", "[baseline][stress]", LocklessAgentBuffer) {
    auto agent_buffer = std::make_unique<TestType>();
    agent_buffer->Initialize();

    const int num_producers = 4;
    const int num_consumers = 4;
    const int items_per_producer = 10000;
    const int total_items = num_producers * items_per_producer;

    std::vector<std::thread> producers;
    for (int i = 0; i < num_producers; ++i) {
        producers.emplace_back([&, i]() {
            for (int j = 0; j < items_per_producer; ++j) {
                uint64_t key = i * items_per_producer + j;
                TestItem item = {key, (uint64_t)i};
                
                char* data_list[] = {reinterpret_cast<char*>(&item)};
                size_t size_list[] = {sizeof(item)};
                int num_inserted = 0;
                
                while (num_inserted == 0) {
                    agent_buffer->ProduceBatch(data_list, size_list, 1, &num_inserted, sizeof(item));
                    if (num_inserted == 0) std::this_thread::yield();
                }
            }
            std::cout << "Producer " << i << " finished" << std::endl;
        });
    }

    std::this_thread::sleep_for(std::chrono::seconds(1));
    std::cout << "Producer finished" << std::endl;

    std::atomic<int> consumed_count = 0;
    std::vector<std::thread> consumers;
    std::vector<bool> consumed_keys(total_items, false);

    for (int i = 0; i < num_consumers; ++i) {
        consumers.emplace_back([&]() {
            while (consumed_count.load(std::memory_order_acquire) < total_items) {
                 for (uint64_t k = 0; k < total_items; ++k) {
                    if (!consumed_keys[k] && agent_buffer->CheckAndReturn(k)) {
                        consumed_keys[k] = true;
                        consumed_count++;
                    }
                 }
                 std::this_thread::yield();
            }
        });
    }

    for (auto& p : producers) {
        p.join();
    }

    for (auto& c : consumers) {
        c.join();
    }

    REQUIRE(consumed_count == total_items);
    int total_consumed_final = 0;
    for(int i = 0; i < total_items; ++i) {
        if(consumed_keys[i]) {
            total_consumed_final++;
        }
    }
    REQUIRE(total_consumed_final == total_items);
}


