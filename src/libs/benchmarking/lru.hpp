#pragma once

#include <cstdint>
#include <cstddef>
#include <unordered_map>

namespace dpf {
namespace benchmark {

// Implements a simple LRU cache

struct LRUNode {
  uint64_t key;
  LRUNode* prev;
  LRUNode* next;
};

class LRU {

public:

  LRU() = default;
  ~LRU();

  LRU(size_t capacity);

  void Initialize(size_t capacity);

  void Put(uint64_t key);
  bool Get(uint64_t key);
  bool Peek(uint64_t key);
  void Evict(uint64_t key);

  size_t Size() const { return cache_.size(); }

private:

  void MoveToFront(LRUNode* node);
  void AddToFront(LRUNode* node);
  
private:

  size_t capacity_ = 0;

  LRUNode* head_ = nullptr;
  LRUNode* tail_ = nullptr;
  std::unordered_map<uint64_t, LRUNode*> cache_;

};


} // namespace benchmark
} // namespace dpf