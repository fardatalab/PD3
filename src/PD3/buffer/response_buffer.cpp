#include "response_buffer.hpp"

#include <cstring>
#include <thread>

namespace dpf {
namespace buffer {

void ResponseBuffer::Initialize() {
  producer_index_.value = 0;
  progress_.value = 0;
  consumer_index_.value = 0;
}

bool ResponseBuffer::Produce(const char* data, size_t size, size_t contiguous_size) {
   //
    // In order to make this ring buffer safe, we must maintain the invariant below:
    // Each consumer moves the head before it increments the progress.
    // Every consumser maintains this invariant:
    // They (1) advance the head,
    //      (2) read the response, and
    //      (3) increment the progress.
    // However, the order of reading progress and head at the producer matters.
    // If the producer reads the head first, then it's possible that
    // before it reads the progress, a concurrent consumer performs all three steps above
    // and thus the progress is updated.
    //
    //
    auto progress = progress_.value.load(std::memory_order_relaxed);
    auto head = consumer_index_.value.load(std::memory_order_relaxed);
    auto tail = producer_index_.value;

    // Check if responses are safe to be inserted
    if (head != progress) {
        return false;
    }

    size_t distance = 0;
    
    if (tail >= head) {
        distance = head + PD3_RING_BUFFER_SIZE - tail;
    }
    else {
        distance = head - tail;
    }

    auto response_bytes = size + sizeof(size_t);
    if (response_bytes % sizeof(uint64_t) != 0) {
      response_bytes += sizeof(uint64_t) - (response_bytes % sizeof(uint64_t));
    }

    *reinterpret_cast<size_t*>(buffer_ + tail) = response_bytes;
    if (response_bytes > distance) {
      return false;
    }

    if (tail + response_bytes > PD3_RING_BUFFER_SIZE) {
      auto size_1 = PD3_RING_BUFFER_SIZE - tail - sizeof(size_t);
      auto size_2 = response_bytes - size_1;
      if (size_1 > 0) {
        std::memcpy(buffer_ + tail + sizeof(size_t), data, size_1);
      }
      if (size_2 > 0) {
        std::memcpy(buffer_, data + size_1, size_2);
      }
    } else {
      std::memcpy(buffer_ + tail + sizeof(size_t), data, size);
    }

    producer_index_.value = (tail + response_bytes) % PD3_RING_BUFFER_SIZE;
    return true;
}

bool ResponseBuffer::ProduceTwo(const char* data1, size_t size1, const char* data2, size_t size2) {
   //
    // In order to make this ring buffer safe, we must maintain the invariant below:
    // Each consumer moves the head before it increments the progress.
    // Every consumser maintains this invariant:
    // They (1) advance the head,
    //      (2) read the response, and
    //      (3) increment the progress.
    // However, the order of reading progress and head at the producer matters.
    // If the producer reads the head first, then it's possible that
    // before it reads the progress, a concurrent consumer performs all three steps above
    // and thus the progress is updated.
    //
    //
    auto progress = progress_.value.load(std::memory_order_relaxed);
    auto head = consumer_index_.value.load(std::memory_order_relaxed);
    auto tail = producer_index_.value;

    // Check if responses are safe to be inserted
    if (head != progress) {
        return false;
    }

    size_t distance = 0;
    
    if (tail >= head) {
        distance = head + PD3_RING_BUFFER_SIZE - tail;
    }
    else {
        distance = head - tail;
    }

    auto response_bytes = size1 + size2 + sizeof(size_t);
    if (response_bytes % sizeof(uint64_t) != 0) {
      response_bytes += sizeof(uint64_t) - (response_bytes % sizeof(uint64_t));
    }

    *reinterpret_cast<size_t*>(buffer_ + tail) = response_bytes;
    if (response_bytes > distance) {
      return false;
    }

    if (tail + response_bytes > PD3_RING_BUFFER_SIZE) {
      auto size_1 = PD3_RING_BUFFER_SIZE - tail - sizeof(size_t);
      auto size_2 = response_bytes - size_1;
      if (size_1 > 0) {
        if (size_1 >= size1) {
          // we can accomodate data item 1 fully
          std::memcpy(buffer_ + tail + sizeof(size_t), data1, size1);
          auto remaining_space = size_1 - size1;
          if (remaining_space > 0) {
            std::memcpy(buffer_ + tail + sizeof(size_t) + size1, data2, remaining_space);
          }
          std::memcpy(buffer_, data2 + remaining_space, size2 - remaining_space);
        } else {
          std::memcpy(buffer_ + tail + sizeof(size_t), data1, size_1);
          auto second_size = size1 - size_1;
          std::memcpy(buffer_, data1 + size_1, second_size);
          std::memcpy(buffer_ + second_size, data2, size2);
        }
      } else {
        std::memcpy(buffer_, data1, size_1);
        std::memcpy(buffer_ + size_1, data2, size2);
      }
    } else {
      std::memcpy(buffer_ + tail + sizeof(size_t), data1, size1);
      std::memcpy(buffer_ + tail + sizeof(size_t) + size1, data2, size2);
    }

    producer_index_.value = (tail + response_bytes) % PD3_RING_BUFFER_SIZE;
    return true;
}

ReservationResult ResponseBuffer::Reserve(size_t size, size_t contiguous_size) {
  auto progress = progress_.value.load(std::memory_order_relaxed);
  auto head = consumer_index_.value.load(std::memory_order_relaxed);
  auto res_idx = reservation_index_.value;

  // safety
  if (head != progress) {
    return ReservationResult();
  }

  size_t distance = 0;
  
  if (res_idx >= head) {
      distance = head + PD3_RING_BUFFER_SIZE - res_idx;
  }
  else {
      distance = head - res_idx;
  }

  auto response_bytes = size + sizeof(size_t);
  if (response_bytes % sizeof(uint64_t) != 0) {
    response_bytes += sizeof(uint64_t) - (response_bytes % sizeof(uint64_t));
  }

  if (response_bytes > distance) {
    return ReservationResult();
  }

  *reinterpret_cast<size_t*>(buffer_ + res_idx) = response_bytes;

  ReservationResult result;
  if (res_idx + response_bytes > PD3_RING_BUFFER_SIZE) {
    result.buffer_1 = buffer_ + res_idx + sizeof(size_t);
    auto size_1 = PD3_RING_BUFFER_SIZE - res_idx - sizeof(size_t);
    auto size_2 = response_bytes - size_1;
    result.size_1 = size_1;
    result.size_2 = size_2;
    return result;
  } else {
    result.buffer_1 = buffer_ + res_idx + sizeof(size_t);
    result.size_1 = response_bytes;
    result.buffer_2 = nullptr;
    result.size_2 = 0;
    return result;
  }

  return ReservationResult();
}

void ResponseBuffer::Commit(size_t size)
{
  auto new_tail = (producer_index_.value + size) % PD3_RING_BUFFER_SIZE;
  producer_index_.value = new_tail;
}

bool ResponseBuffer::Consume(char* data, size_t& size) {

  auto tail = producer_index_.value;
  auto head = consumer_index_.value.load(std::memory_order_relaxed);
  size = *reinterpret_cast<size_t*>(buffer_ + head);

  if (tail == head) {
    return false;
  }
  if (size == 0) {
    return false;
  }

  // Grab the current head
  while(consumer_index_.value.compare_exchange_weak(head, (head + size) % PD3_RING_BUFFER_SIZE) == false) {
    std::this_thread::yield();
    tail = producer_index_.value;
    head = consumer_index_.value.load(std::memory_order_relaxed);
    size = *reinterpret_cast<size_t*>(buffer_ + head);

    if (tail == head) {
      return false;
    }

    if (size == 0) {
      return false;
    }
  }

  // Now, it's safe to copy the response
  auto r_tail = (head + size) % PD3_RING_BUFFER_SIZE;
  size_t avail_bytes = 0;
  char* source_buffer_1 = nullptr;
  char* source_buffer_2 = nullptr;

  if (r_tail > head) {
    avail_bytes = size;
    source_buffer_1 = &buffer_[head];
  } else {
    avail_bytes = PD3_RING_BUFFER_SIZE - head;
    source_buffer_1 = &buffer_[head];
    source_buffer_2 = &buffer_[0];
  }

  std::memcpy(data, source_buffer_1, avail_bytes);
  std::memset(source_buffer_1, 0, avail_bytes);

  if (source_buffer_2) {
    std::memcpy(data + avail_bytes, source_buffer_2, r_tail);
    std::memset(source_buffer_2, 0, r_tail);
  }


  auto progress = progress_.value.load(std::memory_order_relaxed);
  while (progress_.value.compare_exchange_weak(progress, (progress + size) % PD3_RING_BUFFER_SIZE) == false) {
    progress = progress_.value;
  }

  return true;
}

bool ResponseBuffer::ConsumeNoCopy(ConsumeResult& result) {
  auto tail = producer_index_.value;
  auto head = consumer_index_.value.load(std::memory_order_relaxed);
  auto size = *reinterpret_cast<size_t*>(buffer_ + head);

  if (tail == head) {
    return false;
  }
  if (size == 0) {
    return false;
  }

  // Grab the current head
  while(consumer_index_.value.compare_exchange_weak(head, (head + size) % PD3_RING_BUFFER_SIZE) == false) {
    tail = producer_index_.value;
    head = consumer_index_.value.load(std::memory_order_relaxed);
    size = *reinterpret_cast<size_t*>(buffer_ + head);

    if (tail == head) {
      return false;
    }

    if (size == 0) {
      return false;
    }
  }

  // Now, it's safe to copy the response
  auto r_tail = (head + size) % PD3_RING_BUFFER_SIZE;
  size_t avail_bytes = 0;
  char* source_buffer_1 = nullptr;
  char* source_buffer_2 = nullptr;

  if (r_tail > head) {
    avail_bytes = size;
    source_buffer_1 = &buffer_[head];
  } else {
    avail_bytes = PD3_RING_BUFFER_SIZE - head;
    source_buffer_1 = &buffer_[head];
    source_buffer_2 = &buffer_[0];
  }

  result.buffer_1 = source_buffer_1;
  result.buffer_2 = source_buffer_2;
  result.size_1 = avail_bytes;
  result.size_2 = size - avail_bytes;

  return true;
}



} // namespace buffer
} // namespace dpf