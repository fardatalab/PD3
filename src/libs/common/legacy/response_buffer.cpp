#include "response_buffer.hpp"

#include <cstring>
#include <thread>

namespace dpf {
namespace common {

void ResponseBuffer::Initialize() {
  producer_index_.value = 0;
  progress_.value = 0;
  consumer_index_.value = 0;
  consumer_index_history_.clear();
}

void ResponseBuffer::Initialize(size_t num_consumers) {
  Initialize();
  consumer_index_history_.resize(num_consumers);
}

bool ResponseBuffer::Produce(const char* data, size_t size) {
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

// undo consume
void ResponseBuffer::UndoConsume(uint64_t size) {
  consumer_index_.value.fetch_sub(size, std::memory_order_acq_rel);
}

void ResponseBuffer::Commit(uint64_t size) {
  auto progress = progress_.value.load(std::memory_order_relaxed);

  while (progress_.value.compare_exchange_weak(progress, (progress + size) % PD3_RING_BUFFER_SIZE) == false) {
    progress = progress_.value;
  }
}

static uint64_t get_min_value(const std::vector<uint64_t>& values) {
  uint64_t min_value = std::numeric_limits<uint64_t>::max();
  for (auto&& value : values) {
    min_value = std::min(min_value, value);
  }
  return min_value;
}

void ResponseBuffer::StoreConsumerIndex(uint64_t index, uint32_t thread_id) {
  consumer_index_history_[thread_id] = index;
  auto curr_value = consumer_index_.value.load(std::memory_order_relaxed);
  auto curr_progress_value = progress_.value.load(std::memory_order_relaxed);
  auto set_value = get_min_value(consumer_index_history_);
  while (curr_value < set_value) {
    consumer_index_.value.compare_exchange_weak(curr_value, set_value);
    set_value = get_min_value(consumer_index_history_);
  }
  // update progress
  while (curr_progress_value < set_value) {
    progress_.value.compare_exchange_weak(curr_progress_value, set_value);
  }
}



} // namespace common
} // namespace dpf