#pragma once

#include <list>
#include <unordered_map>
#include <cstddef>
#include <cstdint>

class LRUCache {
 public:
  explicit LRUCache(std::size_t capacity) : cap_(capacity) {}

  /** @return true  ⇢ cache hit
   *          false ⇢ cache miss (key inserted) */
  bool access(uint64_t key) {
    auto it = map_.find(key);
    if (it == map_.end()) {                      // miss
      if (list_.size() == cap_) {                // full → evict LRU (back)
        map_.erase(list_.back());
        list_.pop_back();
      }
      list_.push_front(key);                     // new becomes MRU
      map_[key] = list_.begin();
      return false;
    }
    /* hit: move entry to MRU position */
    list_.splice(list_.begin(), list_, it->second);
    return true;
  }

 private:
  std::size_t cap_;
  std::list<uint64_t>                                        list_; // MRU front, LRU back
  std::unordered_map<uint64_t, std::list<uint64_t>::iterator> map_;
};