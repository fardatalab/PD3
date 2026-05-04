#include "catch2/catch_test_macros.hpp"

#include "benchmarking/leap.hpp"

namespace dpf {
namespace benchmark {

TEST_CASE("Leap Construction", "[leap]") {
  Leap leap;
}

TEST_CASE("Leap ProcessAccess", "[leap]") {
  Leap leap;
  LeapConfig config;
  config.access_history_size = 5;
  config.num_splits = 2;
  config.max_prefetch_window_size = 10;
  config.page_cache_size = 10;
  leap.Initialize(config);

  // test with strided accesses
  uint64_t accesses[] = {100, 110, 120, 130, 140, 150, 160, 170};
  size_t num_accesses = sizeof(accesses) / sizeof(accesses[0]);
  
  leap.ProcessAccess(accesses[0]);
  CHECK(leap.GetPrefetchedPagesSize() == 0);

  leap.ProcessAccess(accesses[1]);
  CHECK(leap.GetPrefetchedPagesSize() == 0);

  leap.ProcessAccess(accesses[2]);
  CHECK(leap.GetPrefetchedPagesSize() == 0);

  leap.ProcessAccess(accesses[3]);
  CHECK(leap.GetPrefetchedPagesSize() == 0);

  leap.ProcessAccess(accesses[4]);
  leap.PrintAccessHistory();
  // the current page is in the trend, but no cache hits so the prefetch window size is 1
  CHECK(leap.GetPrefetchedPagesSize() == 1);
  CHECK(leap.IsPrefetchedCandidate(accesses[5]));

  // simulate a bunch more cache hits
  for (auto i = 0; i < 10; ++i) {
    leap.IsPrefetchedCandidate(accesses[5]);
  }

  leap.ProcessAccess(accesses[6]);
  CHECK(leap.GetPrefetchedPagesSize() == 10);

  CHECK(leap.IsPrefetchedCandidate(180));
  CHECK(leap.IsPrefetchedCandidate(190));
  CHECK(leap.IsPrefetchedCandidate(200));
  CHECK(leap.IsPrefetchedCandidate(210));
  CHECK(leap.IsPrefetchedCandidate(220));
  CHECK(leap.IsPrefetchedCandidate(230));
  CHECK(leap.IsPrefetchedCandidate(240));
  
}


} // namespace benchmark 
} // namespace dpf