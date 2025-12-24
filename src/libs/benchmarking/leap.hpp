#pragma once

#include "lru.hpp"

#include <cstdint>
#include <cstddef>
#include <vector>
#include <unordered_set>
#include <deque>
#include <iostream>

namespace dpf {
namespace benchmark {


struct LeapConfig {
  size_t num_splits = 0;
  size_t access_history_size = 0;
  size_t max_prefetch_window_size = 0;
  size_t page_cache_size = 0;
};
  

/// @brief This class implements the Leap prefetching algorithm from https://www.usenix.org/system/files/atc20-maruf.pdf
class Leap {

public:

  Leap() = default;
  ~Leap() = default;

  void Initialize(const LeapConfig& config);
  
  /// @brief Process an access to a page that has page faulted
  /// @param page_id the page ID that the process accessed
  void ProcessAccess(uint64_t page_id);

  /// @brief Check if a page is a candidate for prefetching
  /// @param page_id the page ID to check
  /// @return true if the page is a candidate for prefetching, false otherwise
  bool IsPrefetchedCandidate(uint64_t page_id);

  /// @brief Populate the prefetched set of pages
  void PrefetchRequest(uint64_t page_id);

  // Getters 
  size_t GetAccessHistorySize() const { return access_history_.size(); }
  size_t GetPrefetchedPagesSize() const { return lru_cache_.Size(); }

  void PrintAccessHistory() const {
    for (auto& page_id : access_history_) {
      std::cout << page_id << " ";
    }
    std::cout << std::endl;
  }


private:

  int64_t FindTrend(size_t num_splits);
  int64_t BoyerMooreMajority(const std::deque<int64_t>::iterator& start, const std::deque<int64_t>::iterator& end);

  uint64_t GetPrefetchWindowSize(uint64_t page_id);

  void DoPrefetch(uint64_t page_id);

private:

  LeapConfig config_;
  std::unordered_set<uint64_t> prefetched_pages_;
  std::deque<int64_t> access_history_;
  uint64_t pw_curr_ = 0;
  uint64_t pw_prev_ = 0;
  uint64_t cache_hits_after_last_prefetch_ = 0;
  int64_t curr_trend_ = 0;
  int64_t latest_trend_ = 0;
  uint64_t last_page_id_ = 0;

  // for maintaining pages to get evicted
  LRU lru_cache_;
};

} // namespace benchmark
} // namespace dpf
