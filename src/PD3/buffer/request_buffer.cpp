#include "request_buffer.hpp"

#include <cstring>
#include <iostream>

namespace dpf {
namespace buffer {

void RequestBuffer::Initialize() {
  producer_index_.value.store(0);
  consumer_index_.value = 0;
  progress_.value.store(0);
}

bool RequestBuffer::Produce(const char* data, size_t size) {
  auto producer_idx = producer_index_.value.load(std::memory_order_relaxed);
  auto consumer_idx = consumer_index_.value;
  size_t distance = 0;

  if (producer_idx < consumer_idx) {
    distance = consumer_idx + PD3_RING_BUFFER_SIZE - producer_idx;
  } else {
    distance = producer_idx - consumer_idx;
  }

  size_t request_bytes = sizeof(size_t) + size;

  if (request_bytes % sizeof(size_t) != 0) {
    request_bytes += (sizeof(size_t) - (request_bytes % sizeof(size_t)));
  }

  if (distance + request_bytes >= PD3_MAX_PRODUCER_ADVANCEMENT) {
    return false;
  }
  if (request_bytes > PD3_RING_BUFFER_SIZE - distance) {
    return false;
  }

  // update the producer index
  while (!producer_index_.value.compare_exchange_weak(producer_idx, (producer_idx + request_bytes) % PD3_RING_BUFFER_SIZE)) {
    producer_idx = producer_index_.value.load(std::memory_order_relaxed);
    consumer_idx = consumer_index_.value;

    if (producer_idx <= consumer_idx) {
      distance = consumer_idx + PD3_RING_BUFFER_SIZE - producer_idx;
    } else {
      distance = producer_idx - consumer_idx;
    }
    if (distance + request_bytes >= PD3_MAX_PRODUCER_ADVANCEMENT) {
      return false;
    }
    if (request_bytes > PD3_RING_BUFFER_SIZE - distance) {
      return false;
    }
  }

  if (producer_idx + sizeof(size_t) + size <= PD3_RING_BUFFER_SIZE) {
    char* req_addr = &buffer_[producer_idx];
    reinterpret_cast<size_t*>(req_addr)[0] = request_bytes;
    std::memcpy(req_addr + sizeof(size_t), data, size);

    // update progress
    auto progress = progress_.value.load(std::memory_order_relaxed);
    while (!progress_.value.compare_exchange_weak(progress, (progress + request_bytes) % PD3_RING_BUFFER_SIZE)) {
      progress = progress_.value.load(std::memory_order_relaxed);
    }
  } else {
    // split the request into two parts
    size_t remaining_bytes = PD3_RING_BUFFER_SIZE - producer_idx - sizeof(size_t);
    char* req_addr = &buffer_[producer_idx];
    char* req_addr_2 = &buffer_[0];

    // write the number of bytes in
    reinterpret_cast<size_t*>(req_addr)[0] = size;
    if (remaining_bytes > 0) {
      std::memcpy(req_addr + sizeof(size_t), data, remaining_bytes);
    }
    std::memcpy(req_addr_2, data + remaining_bytes, size - remaining_bytes);

    // update progress
    auto progress = progress_.value.load(std::memory_order_relaxed);
    while (!progress_.value.compare_exchange_weak(progress, (progress + request_bytes) % PD3_RING_BUFFER_SIZE)) {
      progress = progress_.value.load(std::memory_order_relaxed);
    }
  }
  return true;
}

bool RequestBuffer::Produce(const char* data, const char* data2, size_t size1, size_t size2) {
  auto producer_idx = producer_index_.value.load(std::memory_order_relaxed);
  auto consumer_idx = consumer_index_.value;
  size_t distance = 0;

  auto combined_size = size1 + size2;

  if (producer_idx < consumer_idx) {
    distance = consumer_idx + PD3_RING_BUFFER_SIZE - producer_idx;
  } else {
    distance = producer_idx - consumer_idx;
  }

  size_t request_bytes = sizeof(size_t) + combined_size;

  if (request_bytes % sizeof(size_t) != 0) {
    request_bytes += (sizeof(size_t) - (request_bytes % sizeof(size_t)));
  }

  if (distance + request_bytes >= PD3_MAX_PRODUCER_ADVANCEMENT) {
    return false;
  }
  if (request_bytes > PD3_RING_BUFFER_SIZE - distance) {
    return false;
  }

  // update the producer index
  while (!producer_index_.value.compare_exchange_weak(producer_idx, (producer_idx + request_bytes) % PD3_RING_BUFFER_SIZE)) {
    producer_idx = producer_index_.value.load(std::memory_order_relaxed);
    consumer_idx = consumer_index_.value;

    if (producer_idx <= consumer_idx) {
      distance = consumer_idx + PD3_RING_BUFFER_SIZE - producer_idx;
    } else {
      distance = producer_idx - consumer_idx;
    }
    if (distance + request_bytes >= PD3_MAX_PRODUCER_ADVANCEMENT) {
      return false;
    }
    if (request_bytes > PD3_RING_BUFFER_SIZE - distance) {
      return false;
    }
  }

  if (producer_idx + sizeof(size_t) + combined_size <= PD3_RING_BUFFER_SIZE) {
    char* req_addr = &buffer_[producer_idx];
    reinterpret_cast<size_t*>(req_addr)[0] = request_bytes;
    std::memcpy(req_addr + sizeof(size_t), data, size1);
    std::memcpy(req_addr + sizeof(size_t) + size1, data2, size2);

    // update progress
    auto progress = progress_.value.load(std::memory_order_relaxed);
    while (!progress_.value.compare_exchange_weak(progress, (progress + request_bytes) % PD3_RING_BUFFER_SIZE)) {
      progress = progress_.value.load(std::memory_order_relaxed);
    }
  } else {
    // split the request into two parts
    size_t remaining_bytes = PD3_RING_BUFFER_SIZE - producer_idx - sizeof(size_t);
    char* req_addr = &buffer_[producer_idx];
    char* req_addr_2 = &buffer_[0];

    // write the number of bytes in
    reinterpret_cast<size_t*>(req_addr)[0] = combined_size;
    // write the buffers
    if (remaining_bytes > 0) {
      if (remaining_bytes >= size1) {
        std::memcpy(req_addr + sizeof(size_t), data, size1);
        if (remaining_bytes > size1) {
          std::memcpy(req_addr + sizeof(size_t) + size1, data2, remaining_bytes - size1);
        } else {
          std::memcpy(req_addr_2, data + size1, remaining_bytes - size1);
        }
      } else {
        std::memcpy(req_addr + sizeof(size_t), data, remaining_bytes);
        std::memcpy(req_addr_2, data + remaining_bytes, size1 - remaining_bytes);
        std::memcpy(req_addr_2 + size1 - remaining_bytes, data2, size2 - (size1 - remaining_bytes));
      }
    } else {
      std::memcpy(req_addr_2, data, size1);
      std::memcpy(req_addr_2 + size1, data2, size2);
    }

    // update progress
    auto progress = progress_.value.load(std::memory_order_relaxed);
    while (!progress_.value.compare_exchange_weak(progress, (progress + request_bytes) % PD3_RING_BUFFER_SIZE)) {
      progress = progress_.value.load(std::memory_order_relaxed);
    }
  }
  return true;
}

// DDS has a diff implementation on DPU
// TODO: change
bool RequestBuffer::Consume(char* data, size_t& size) {
  // This consume function was taken from DDS
  // The following invariant is maintained on production into this queue
  //   (1) tail is advanced
  //   (2) request is inserted
  //   (3) progress is incremented
  // The order of reading the progress pointer and the tail at the consumer matters. If the consumer reads the tail before progress,
  // its possible the producer performs all three steps, updating progress. The reader is thus left with an invalid progress pointer.

  auto progress = progress_.value.load(std::memory_order_relaxed);
  auto producer_index = producer_index_.value.load(std::memory_order_acquire);
  auto consumer_index = consumer_index_.value;

  if (consumer_index == producer_index) {
    // nothing to consume
    return false;
  }

  if (progress != producer_index) {
    // the tail has been advanced, but the progress has not been updated
    return false;
  }

  // now it is safe to copy requests
  size_t available_bytes = 0;
  size_t output_size = 0;
  char* source_buffer_1 = nullptr;
  char* source_buffer_2 = nullptr;

  if (progress > consumer_index) {
    available_bytes = progress - consumer_index;
    output_size = available_bytes;
    source_buffer_1 = &buffer_[consumer_index];
    size = output_size;
  } else {
    available_bytes = PD3_RING_BUFFER_SIZE - consumer_index;
    output_size = available_bytes + progress;
    source_buffer_1 = &buffer_[consumer_index];
    size = output_size;
    source_buffer_2 = &buffer_[0];
  }

  std::memcpy(data, source_buffer_1, available_bytes);
  memset(source_buffer_1, 0, available_bytes);

  if (source_buffer_2) {
    std::memcpy(data + available_bytes, source_buffer_2, size - available_bytes);
    memset(source_buffer_2, 0, size - available_bytes);
  }

  consumer_index_.value = progress;
  return true;
}


} // namespace buffer
} // namespace dpf