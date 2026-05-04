// Reference: https://rigtorp.se/hugepages/

#pragma once

#include <sys/mman.h>

#include <limits>

template <typename T>
struct huge_page_allocator {
  constexpr static std::size_t huge_page_size = 1 << 21;  // 2 MiB
  using value_type = T;

  huge_page_allocator() = default;
  template <class U>
  constexpr huge_page_allocator(const huge_page_allocator<U> &) noexcept {}

  size_t round_to_huge_page_size(size_t n) {
    return (((n - 1) / huge_page_size) + 1) * huge_page_size;
  }

  T *allocate(std::size_t n) {
    if (n > std::numeric_limits<std::size_t>::max() / sizeof(T)) {
      throw std::bad_alloc();
    }
    auto p = static_cast<T *>(mmap(
        nullptr, round_to_huge_page_size(n * sizeof(T)), PROT_READ | PROT_WRITE,
        MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB, -1, 0));
    if (p == MAP_FAILED) {
      throw std::bad_alloc();
    }
    return p;
  }

  void deallocate(T *p, std::size_t n) {
    munmap(p, round_to_huge_page_size(n * sizeof(T)));
  }

  bool operator==(const huge_page_allocator<T> &) const noexcept {
    return true;
  }
};