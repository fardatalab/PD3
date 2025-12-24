#pragma once

#include <cstdint>
#include <iostream>
#include <thread>
#include <cstring>

namespace dpf {

class ReceiveBuffer {

public:

  ReceiveBuffer() noexcept = default;
  ~ReceiveBuffer() noexcept = default;

  uint32_t max_size() const noexcept {
    return max_size_;
  }

  void* buffer() const noexcept {
    return buffer_;
  }

  uint64_t remote_address() const noexcept {
    return remote_address_;
  }

  uint32_t remote_rkey() const noexcept {
    return remote_rkey_;
  }

  void max_size(uint32_t max_size) noexcept {
    max_size_ = max_size;
  }

  void buffer(void* buffer) noexcept {
    buffer_ = buffer;
  }

  void remote_address(uint64_t remote_address) noexcept {
    remote_address_ = remote_address;
  }

  void remote_rkey(uint32_t remote_rkey) noexcept {
    remote_rkey_ = remote_rkey;
  }

  bool PollMessage() {
    char* msg_buf = reinterpret_cast<char*>(buffer_);

    // check the first word
    auto msg_size = *reinterpret_cast<uint32_t*>(msg_buf);
    if (msg_size == 0) {
      return false;
    }
    // check the tail
    uint32_t* tail = (uint32_t*)(msg_buf + sizeof(uint32_t) + msg_size);
    while (*tail == 0) {
      std::this_thread::yield();
    }
    return true;
  }

  void RemoveMessage() {
    char* msg_buf = reinterpret_cast<char*>(buffer_);
    auto msg_size = *reinterpret_cast<uint32_t*>(msg_buf);
    std::memset(msg_buf, 0, msg_size + sizeof(uint32_t) * 2);
  }

private:
  // the maximum size of a message
  uint32_t max_size_ = 0;

  // pointer to the buffer
  void* buffer_ = nullptr;

  // remote receive buffer
  uint64_t remote_address_ = 0;

  // access token for the remote buffer
  uint32_t remote_rkey_ = 0;

};

}
