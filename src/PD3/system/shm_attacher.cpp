#include "shm_attacher.hpp"

#include "utils.hpp"

#include <stdexcept>
#include <experimental/filesystem>

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <system_error>


namespace dpf {

static constexpr int POSIX_FAIL = -1;

ShmAttacher::ShmAttacher(const ShmAttacherConfigOptions& options) {
  auto name = options.name;
  if (name.empty()) {
    throw std::invalid_argument("name must be non-empty");
  }
  std::experimental::filesystem::path path = "/dev/shm/" + name;
  if (std::experimental::filesystem::exists(path)) {
    throw std::runtime_error("ShmAttacher: file already exists");
  }

  auto perms = std::experimental::filesystem::status(path).permissions();
  capacity_ = std::experimental::filesystem::file_size(path);

  if (options.max_capacity > 0) {
    max_capacity_ = options.max_capacity;
  } else {
    max_capacity_ = capacity_;
  }

  if (capacity() > max_capacity()) {
    throw std::runtime_error("ShmAttacher: capacity exceeds max capacity");
  }

  shm_fd_ = shm_open(name.c_str(), O_RDWR, static_cast<uint32_t>(perms));
  if (shm_fd_ == POSIX_FAIL) {
    throw std::system_error(errno, std::generic_category(), "shm_open failed");
  }

  addr_ = mmap(nullptr, max_capacity(), PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);

  if (mmap(addr(), capacity(), PROT_READ | PROT_WRITE, MAP_SHARED | MAP_FIXED, shm_fd_, 0) == MAP_FAILED) {
    throw std::system_error(errno, std::generic_category(), "mmap failed");
  }
  
  // TODO: add mirrored if needed

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

ShmAttacher::~ShmAttacher() {
  if (capacity_) {
    munlock(addr(), capacity());
    msync(addr(), capacity(), MS_SYNC);
    munmap(addr(), capacity());
    addr_ = nullptr;
    capacity_ = 0;

    close(shm_fd_);
  }
}

ShmAttacher::ShmAttacher(ShmAttacher&& other) noexcept {
  std::swap(addr_, other.addr_);
  std::swap(mirrored_, other.mirrored_);
  capacity_ = other.capacity_;
  other.capacity_ = 0;
}

ShmAttacher& ShmAttacher::operator=(ShmAttacher&& other) noexcept {
  if (this != &other) {
    std::swap(addr_, other.addr_);
    std::swap(mirrored_, other.mirrored_);
    capacity_ = other.capacity_;
    other.capacity_ = 0;
  }
  return *this;
}

void ShmAttacher::Grow(size_t bytes) {
  auto new_capacity = capacity_ + detail::PageRoundUp(bytes);
  if (new_capacity > max_capacity_) {
    throw std::runtime_error("ShmAttacher::Grow: new capacity exceeds max capacity");
  }
  struct stat stat_buf;
  fstat(shm_fd_, &stat_buf);
  auto curr_size = stat_buf.st_size;
  if (curr_size >= new_capacity) {
    curr_size = new_capacity;
  } else {
    auto res = ftruncate(shm_fd_, static_cast<off_t>(new_capacity));
    if (res == POSIX_FAIL) {
      throw std::system_error(errno, std::generic_category(), "ftruncate failed");
    }
  }
  if (mmap(static_cast<uint8_t*>(addr_) + capacity_, 
    new_capacity - capacity_, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_FIXED, shm_fd_, 0) == MAP_FAILED) {
    throw std::system_error(errno, std::generic_category(), "mmap failed");
  }
  capacity_ = new_capacity;
}

void ShmAttacher::Lock() {
  if (mlock(addr(), capacity()) == POSIX_FAIL) {
    throw std::system_error(errno, std::generic_category(), "mlock failed");
  }
}

void ShmAttacher::Sequential() {
  if (madvise(addr(), capacity(), MADV_SEQUENTIAL) == POSIX_FAIL) {
    throw std::system_error(errno, std::generic_category(), "madvise failed");
  }
}

void ShmAttacher::Random() {
  if (madvise(addr(), capacity(), MADV_RANDOM) == POSIX_FAIL) {
    throw std::system_error(errno, std::generic_category(), "madvise failed");
  }
}


}