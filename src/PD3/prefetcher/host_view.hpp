#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>

#include <arm_neon.h>

namespace dpf {
namespace cuckoo_set {

// assume cache line size is 64B
constexpr std::size_t hardware_constructive_interference_size = 64;

// We fix the key type to control the bucket size carefully.
using KeyT = uint64_t;

constexpr KeyT NULL_KEY = -1;

constexpr size_t NULL_SLOT_IDX = -1;
constexpr size_t SLOTS_PER_BUCKET = 4;
static_assert((SLOTS_PER_BUCKET & (SLOTS_PER_BUCKET - 1)) == 0);

constexpr size_t MAX_LOOKUP_BATCH_SZ =
    hardware_constructive_interference_size / sizeof(KeyT);

// each bucket is half-a-cache-line sized, and aligned to that as well
// i.e. we get 2 buckets per cache line
struct alignas(hardware_constructive_interference_size / 2) Bucket {
  struct iterator {
    iterator()
        : slot_(nullptr) {}

    iterator(KeyT* slot)
        : slot_(slot) {}

    bool is_null() { return !slot_; }

    const KeyT& key() {
      return *slot_;
    }

    KeyT* slot_;
  };

  std::array<KeyT, SLOTS_PER_BUCKET> key_slots;

  iterator find(KeyT key) {
    for (size_t i = 0; i < SLOTS_PER_BUCKET; ++i) {
      if (key_slots[i] == key) {
        return {&key_slots[i]};
      }
    }
    return {};
  }

  iterator find_simd(KeyT key) {
    static_assert(SLOTS_PER_BUCKET == 4, "Only 4 slots supported");

    uint64x2_t key_vec = vdupq_n_u64(key);

    uint64x2_t keys01 = vld1q_u64(&key_slots[0]);
    uint64x2_t keys23 = vld1q_u64(&key_slots[2]);

    uint64x2_t cmp01 = vceqq_u64(keys01, key_vec);
    uint64x2_t cmp23 = vceqq_u64(keys23, key_vec);
    uint32x4_t cmp_all = vcombine_u32(vmovn_u64(cmp01), vmovn_u64(cmp23));

    // emulate a movemask
    uint32x4_t m_all = vshrq_n_u32(cmp_all, 31);
    const int32x4_t shift_weights = {0, 1, 2, 3};
    uint32x4_t m_all_weighted = vshlq_u32(m_all, shift_weights);
    uint32_t mask = vaddvq_u32(m_all_weighted);

    return mask
      ? iterator{&key_slots[__builtin_ctz(mask)]}
      : iterator{};
  }

  bool insert(KeyT key) {
    for (size_t i = 0; i < SLOTS_PER_BUCKET; ++i) {
      if (is_empty(key_slots[i])) {
        update(i, key);
        return true;
      } else if (key_slots[i] == key) {
        throw std::runtime_error{"tried to insert existing key"};
      }
    }
    return false;
  }

  KeyT displace_insert(KeyT key) {
    size_t disp_idx = get_random_displace_idx();
    KeyT to_displace = key_slots[disp_idx];
    update(disp_idx, key);
    return to_displace;
  }

  void update(size_t i, KeyT key) {
    key_slots[i] = key;
  }

  void erase(size_t i) {
    key_slots[i] = NULL_KEY;
  }

 private:
  static size_t get_random_displace_idx() {
    static size_t curr_idx{0};
    curr_idx++;
    return curr_idx & (SLOTS_PER_BUCKET - 1);
  }

  static bool is_empty(const KeyT& key) { return key == NULL_KEY; }
};
static_assert(alignof(Bucket) == hardware_constructive_interference_size / 2);
static_assert(sizeof(Bucket) == hardware_constructive_interference_size / 2);

template <class Hash = std::hash<KeyT>,
          class Allocator = std::allocator<Bucket>>
class cuckoo_set {
 public:
  using iterator = Bucket::iterator;

  cuckoo_set(size_t capacity)
      : hash_fn_(),
        allocator_(),
        num_buckets_(next_pow2(capacity) / SLOTS_PER_BUCKET),
        bucket_bitmask_(num_buckets_ - 1),
        buckets_() {
    if ((num_buckets_ & (num_buckets_ - 1)) != 0) {
      throw std::invalid_argument("num_buckets must be a power of 2");
    }

    buckets_ = allocator_.allocate(num_buckets_);
    if ((uint64_t)(buckets_) % hardware_constructive_interference_size != 0) {
      throw std::runtime_error("buckets_ is not cache-aligned");
    }

    // initialize slots
    for (size_t i = 0; i < num_buckets_ * SLOTS_PER_BUCKET; ++i) {
      buckets_[i / SLOTS_PER_BUCKET].erase(i % SLOTS_PER_BUCKET);
    }
  }

  ~cuckoo_set() {
    if (buckets_) {
      allocator_.deallocate(buckets_, num_buckets_);
    }
  }

  size_t size() { return sz_; }

  double load_factor() {
    return static_cast<double>(sz_) / (num_buckets_ * SLOTS_PER_BUCKET);
  }

  iterator find(KeyT key) {
    size_t hash = hash_key(key);
    size_t bucket_id1 = get_bucket_id(hash);

    auto it = buckets_[bucket_id1].find_simd(key);
    if (!it.is_null()) {
      return it;
    }

    size_t bucket_id2 = get_other_bucket_id(hash, key);
    return buckets_[bucket_id2].find_simd(key);
  }

  void find_batched(const KeyT* keys, size_t num_keys, iterator* results) {
    std::array<size_t, MAX_LOOKUP_BATCH_SZ> bucket_id1s;
    std::array<size_t, MAX_LOOKUP_BATCH_SZ> bucket_id2s;

    // compute hashes and prefetch buckets
    for (size_t i = 0; i < num_keys; ++i) {
      size_t hash = hash_key(keys[i]);
      bucket_id1s[i] = get_bucket_id(hash);
      bucket_id2s[i] = get_other_bucket_id(hash, keys[i]);
      __builtin_prefetch(&buckets_[bucket_id1s[i]], 0, 3);
      __builtin_prefetch(&buckets_[bucket_id2s[i]], 0, 3);
    }

    // search buckets via SIMD
    for (size_t i = 0; i < num_keys; ++i) {
      results[i] = buckets_[bucket_id1s[i]].find_simd(keys[i]);
      if (results[i].is_null()) {
        results[i] = buckets_[bucket_id2s[i]].find_simd(keys[i]);
      }
    }
  }

  void erase(const iterator& it) {
    sz_--;
    *it.slot_ = NULL_KEY;
  }

  void insert(KeyT key) {
    sz_++;

    size_t hash = hash_key(key);
    size_t bucket_id1 = get_bucket_id(hash);
    size_t bucket_id2 = get_other_bucket_id(hash, key);

    Bucket& bucket1 = buckets_[bucket_id1];
    if (bucket1.insert(key)) {
      return;
    }

    Bucket& bucket2 = buckets_[bucket_id2];
    if (bucket2.insert(key)) {
      return;
    }

    return displace_insert(bucket_id1, key, 0);
  }

 private:
  static constexpr size_t MAX_INSERT_DEPTH = 256;

  void displace_insert(size_t bucket_id, KeyT key, size_t curr_depth) {
    if (curr_depth >= MAX_INSERT_DEPTH) {
      throw std::runtime_error{"cannot find insertion slot."};
    }

    KeyT displaced_slot = buckets_[bucket_id].displace_insert(key);

    size_t hash = hash_key(displaced_slot);
    size_t bucket_id1 = get_bucket_id(hash);
    size_t bucket_id2 = get_other_bucket_id(hash, displaced_slot);

    size_t nxt_bucket_id = bucket_id1 == bucket_id ? bucket_id2 : bucket_id1;
    if (buckets_[nxt_bucket_id].insert(displaced_slot)) {
      return;
    }

    return displace_insert(nxt_bucket_id, displaced_slot, curr_depth + 1);
  }

  static constexpr uint64_t next_pow2(uint64_t x) {
    x--;
    x |= (x >> 1);
    x |= (x >> 2);
    x |= (x >> 4);
    x |= (x >> 8);
    x |= (x >> 16);
    x |= (x >> 32);
    x++;
    return x;
  }

  size_t hash_key(KeyT key) { return hash_fn_(key); }
  size_t get_bucket_id(size_t h) { return h & bucket_bitmask_; }
  size_t get_other_bucket_id(size_t h, KeyT k) {
    return hash_fn_(h ^ k) & bucket_bitmask_;
  }

  Hash hash_fn_;
  Allocator allocator_;

  size_t num_buckets_;
  size_t bucket_bitmask_;
  Bucket* buckets_;

  size_t sz_{0};
};
}

}  // namespace cuckoo_set