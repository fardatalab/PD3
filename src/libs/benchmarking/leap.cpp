#include "leap.hpp"


namespace dpf {
namespace benchmark {


static uint64_t round_to_power_of_two(uint64_t x) {
  if (x == 0) return 1;
  x--;
  x |= x >> 1;
  x |= x >> 2;
  x |= x >> 4;
  x |= x >> 8;
  x |= x >> 16;
  x |= x >> 32;
  x++;
  return x;
}

void Leap::Initialize(const LeapConfig& config) {
  config_ = config;
  access_history_.clear();
  lru_cache_.Initialize(config_.page_cache_size);
}

void Leap::ProcessAccess(uint64_t page_id) {
}

bool Leap::IsPrefetchedCandidate(uint64_t page_id) {
  return false;
}

void Leap::PrefetchRequest(uint64_t page_id) {
  // not implemented currently
  throw std::runtime_error("Not implemented");
}

//
// Private methods
//

int64_t Leap::FindTrend(size_t num_splits) {
  return 0;
}

int64_t Leap::BoyerMooreMajority(const std::deque<int64_t>::iterator& start, const std::deque<int64_t>::iterator& end) {
  return 0;
}

uint64_t Leap::GetPrefetchWindowSize(uint64_t page_id) {
  return 0;
}

void Leap::DoPrefetch(uint64_t page_id) {
  return;
}

} // namespace benchmark
} // namespace dpf