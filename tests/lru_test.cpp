#include "catch2/catch_test_macros.hpp"

#include "benchmarking/lru.hpp"

namespace dpf {
namespace benchmark {

TEST_CASE("LRU Construction", "[lru]") {
  LRU lru;
  lru.Initialize(1000);
  REQUIRE(lru.Get(1) == false);
}

TEST_CASE("LRU Operations", "[lru]") {
  LRU lru;
  lru.Initialize(3); // Small size to test eviction

  // Test basic put/get
  lru.Put(1);
  REQUIRE(lru.Get(1) == true);
  REQUIRE(lru.Get(2) == false);

  // Test filling the cache
  lru.Put(2);
  lru.Put(3);
  REQUIRE(lru.Get(1) == true);
  REQUIRE(lru.Get(2) == true); 
  REQUIRE(lru.Get(3) == true);

  // Test eviction - adding 4 should evict 1
  lru.Put(4);
  REQUIRE(lru.Get(1) == false); // 1 was evicted
  REQUIRE(lru.Get(2) == true);
  REQUIRE(lru.Get(3) == true);
  REQUIRE(lru.Get(4) == true);

  // Test accessing elements updates their position
  lru.Get(2); // Brings 2 to front
  lru.Put(5); // Should evict 3, not 2
  REQUIRE(lru.Get(2) == true);
  REQUIRE(lru.Get(3) == false); // 3 was evicted
  REQUIRE(lru.Get(4) == true);
  REQUIRE(lru.Get(5) == true);
}


} // namespace benchmark
} // namespace dpf
