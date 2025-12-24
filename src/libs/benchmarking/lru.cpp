#include "lru.hpp"

namespace dpf {
namespace benchmark { 

LRU::LRU(size_t capacity) {
  Initialize(capacity);
}

LRU::~LRU() {
  LRUNode* current = head_;
  while (current != nullptr) {
    LRUNode* next = current->next;
    delete current;
    current = next;
  }
}

void LRU::Initialize(size_t capacity) {
  capacity_ = capacity;
  head_ = new LRUNode();
  tail_ = new LRUNode();
  head_->next = tail_;
  head_->prev = nullptr;
  tail_->next = nullptr;
  tail_->prev = head_;
}

void LRU::Put(uint64_t key) {
  if (cache_.find(key) != cache_.end()) {
    // this is already in the cache
    LRUNode* node = cache_[key];
    MoveToFront(node);
  } else {
    // not in cache
    LRUNode* node = new LRUNode();
    node->key = key;
    cache_[key] = node;
    AddToFront(node);

    if (cache_.size() > capacity_) {
      LRUNode* node = tail_->prev;
      node->prev->next = tail_;
      tail_->prev = node->prev;
      cache_.erase(node->key);
      delete node;
    }
  }
}

bool LRU::Get(uint64_t key) {
  if (cache_.find(key) != cache_.end()) {
    // this is already in the cache
    LRUNode* node = cache_[key];
    MoveToFront(node);
    return true;
  }
  return false;
}

bool LRU::Peek(uint64_t key) {
  return cache_.find(key) != cache_.end();
}

void LRU::Evict(uint64_t key) {
  if (cache_.find(key) != cache_.end()) {
    LRUNode* node = cache_[key];
    node->prev->next = node->next;
    node->next->prev = node->prev;
    cache_.erase(key);
    delete node;
  }
}

void LRU::MoveToFront(LRUNode* node) {
  // remove from current position
  node->prev->next = node->next;
  node->next->prev = node->prev; 
  AddToFront(node);
}

void LRU::AddToFront(LRUNode* node) {
  // add to the front
  node->next = head_->next;
  node->prev = head_;
  head_->next->prev = node;
  head_->next = node;
}

} // namespace benchmark  
} // namespace dpf