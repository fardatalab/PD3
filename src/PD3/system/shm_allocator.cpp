#include "shm_allocator.hpp"

#include "utils.hpp"

#include <cerrno>
#include <cstdlib>
#include <fcntl.h>
#include <sys/mman.h>
#include <system_error>
#include <unistd.h>

namespace dpf {

static constexpr int POSIX_FAIL = -1;


ShmAllocator::~ShmAllocator() {
  if (capacity_) {
    munlock(addr(), capacity());
    msync(addr(), capacity(), MS_SYNC);
    munmap(addr(), capacity());

    shm_unlink(name_.c_str());
    addr_ = nullptr;
    capacity_ = 0;
  }
}

ShmAllocator::ShmAllocator(const ShmAllocatorConfigOptions& options)
{
  capacity_ = detail::PageRoundUp(options.capacity);
  if (capacity_ == 0) {
    throw std::invalid_argument("capacity must be greater than 0");
  }
  perms_ = options.perms;
  mirrored_ = options.mirrored;
  name_ = options.name;
  if (name_.empty()) {
    throw std::invalid_argument("name must be non-empty");
  }
  OpenSegment();

  if (options.lock) {
    Lock();
  }
  if (options.sequential) {
    Sequential();
  }
  if (options.random) {
    Random();
  }
}

ShmAllocator::ShmAllocator(ShmAllocator&& other) {
  std::swap(addr_, other.addr_);
  std::swap(capacity_, other.capacity_);
  std::swap(perms_, other.perms_);
  std::swap(mirrored_, other.mirrored_);
  std::swap(name_, other.name_);

  other.capacity_ = 0;
}

ShmAllocator& ShmAllocator::operator=(ShmAllocator&& other) {
  if (this != &other) {
    std::swap(addr_, other.addr_);
    std::swap(capacity_, other.capacity_);
    std::swap(perms_, other.perms_);
    std::swap(mirrored_, other.mirrored_);
    std::swap(name_, other.name_);

    other.capacity_ = 0;
  }
  return *this;
}

void ShmAllocator::Sequential() {
  if (madvise(addr(), capacity(), MADV_SEQUENTIAL) == POSIX_FAIL) {
    throw std::system_error(errno, std::generic_category(), "madvise");
  }
}

void ShmAllocator::Random() {
  if (madvise(addr(), capacity(), MADV_RANDOM) == POSIX_FAIL) {
    throw std::system_error(errno, std::generic_category(), "madvise");
  }
}

void ShmAllocator::Lock() {
  if (mlock(addr(), capacity()) == POSIX_FAIL) {
    throw std::system_error(errno, std::generic_category(), "mlock");
  }
}

size_t ShmAllocator::MappedCapacity() const noexcept {
  if (mirrored_) {
    return capacity_ * 2;
  }
  return capacity_;
}

void ShmAllocator::OpenSegment() {
  auto fd = shm_open(name_.c_str(), O_RDWR | O_CREAT | O_EXCL, perms_);
  if (fd == POSIX_FAIL) {
    throw std::system_error(errno, std::generic_category(), "shm_open");
  }
  auto result = ftruncate(fd, static_cast<int64_t>(capacity()));
  if (result == POSIX_FAIL) {
    throw std::system_error(errno, std::generic_category(), "ftruncate");
  }
  addr_ = mmap(nullptr, MappedCapacity(), PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE, -1, 0);
  if (addr_ == MAP_FAILED) {
    throw std::system_error(errno, std::generic_category(), "mmap");
  }
  if (mirrored_) {
    if (mmap(addr(), capacity(), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_FIXED, fd, 0) == MAP_FAILED) {
      throw std::system_error(errno, std::generic_category(), "mmap");
    }

    void* topaddr = reinterpret_cast<uint8_t*>(addr()) + capacity();
    if (mmap(topaddr, capacity(), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_FIXED, fd, 0) == MAP_FAILED) {
      throw std::system_error(errno, std::generic_category(), "mmap");
    }
  } else {
    if (mmap(addr(), capacity(), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_FIXED, fd, 0) == MAP_FAILED) {
      throw std::system_error(errno, std::generic_category(), "mmap");
    }
  }
  close(fd);
}

}