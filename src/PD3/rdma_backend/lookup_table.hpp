#pragma once

#include <unordered_map>
#include <vector>
#include <cstdint>
#include <cstddef>

namespace dpf {

class LookupTable {

public:

  LookupTable() = default;
  ~LookupTable() = default;

  void Initialize(size_t num_keys);

  bool Lookup(uint64_t key);

private:

  std::unordered_map<uint64_t, uint64_t> table_;
  size_t num_keys_;

};

class LookupTableFast {

public:

  LookupTableFast() = default;
  ~LookupTableFast() = default;

  void Initialize(size_t num_keys);

  bool Lookup(uint64_t key);

private:

  std::vector<uint64_t> table_;
  size_t num_keys_;

};

}