#include "baselines.hpp"

#include <cstring>
#include <mutex>
#include <iostream>
#include <thread>

namespace dpf {
namespace buffer {

void LocklessAgentBuffer::Initialize() {
  producer_index_.value.store(0);
  consumer_index_.value.store(0);
  std::memset(buffer_, 0, PD3_RING_BUFFER_SIZE);
}

static uint64_t align_to_eight_bytes(uint64_t pos) {
  return (pos + 7) & ~7;
}

bool LocklessAgentBuffer::ProduceBatch(char** data_list, size_t* size_list, size_t num_items, int* num_inserted, size_t contiguous_size) {
  auto consumer_idx = consumer_index_.value.load(std::memory_order_relaxed);
  auto producer_idx = producer_index_.value.load(std::memory_order_relaxed);
  auto producer_pos = get_buffer_idx(producer_idx);

  uint64_t distance = PD3_RING_BUFFER_SIZE - (producer_idx - consumer_idx);
  const uint64_t buffer = 8;

  if (distance < 8192) [[unlikely]] {
    // the distance needs to be more than 8KB to write a batched response
    return false;
  }
  // now we have enough space to write the batch 
  for (size_t i = 0; i < num_items; i++) {
    producer_idx = producer_index_.value.load(std::memory_order_relaxed);
    producer_pos = get_buffer_idx(producer_idx);
    consumer_idx = consumer_index_.value.load(std::memory_order_relaxed);
    auto actual_size = size_list[i] + sizeof(RecordHeader);
    actual_size = align_to_eight_bytes(actual_size);
    distance = PD3_RING_BUFFER_SIZE - (producer_idx - consumer_idx);
    if (distance < actual_size + buffer) {
      break;
    }
    // we have enough place to produce
    auto remaining_space = PD3_RING_BUFFER_SIZE - producer_pos;
    if (remaining_space < sizeof(RecordHeader) + contiguous_size) {
      producer_index_.value.fetch_add(remaining_space, std::memory_order_relaxed);
      producer_pos = get_buffer_idx(producer_index_.value.load(std::memory_order_relaxed));
      remaining_space = PD3_RING_BUFFER_SIZE - producer_pos;
    }

    if (remaining_space >= actual_size) {
      // just one production
      auto* header = reinterpret_cast<RecordHeader*>(buffer_ + producer_pos);
      header->size = actual_size;
      std::memcpy(buffer_ + producer_pos + sizeof(RecordHeader), data_list[i], size_list[i]);
      header->consumer_state.store(CONSUMER_STATE_WRITTEN, std::memory_order_release);
      producer_index_.value.fetch_add(actual_size, std::memory_order_relaxed);
    } else {
      // two productions
      auto* header = reinterpret_cast<RecordHeader*>(buffer_ + producer_pos);
      header->size = actual_size;
      size_t size_1 = remaining_space - sizeof(RecordHeader);
      size_t size_2 = actual_size - size_1 - sizeof(RecordHeader);
      producer_pos += sizeof(RecordHeader);
      std::memcpy(buffer_ + producer_pos, data_list[i], size_1);
      std::memcpy(buffer_, data_list[i] + size_1, size_2);
      header->consumer_state.store(CONSUMER_STATE_WRITTEN, std::memory_order_release);
      producer_index_.value.fetch_add(actual_size, std::memory_order_relaxed);
    }
    *num_inserted += 1;
  }

  return true;
}

bool LocklessAgentBuffer::CheckAndReturn(uint64_t key) {
  auto consumer_idx = consumer_index_.value.load(std::memory_order_acquire);
  auto producer_idx = producer_index_.value.load(std::memory_order_acquire);

  while (consumer_idx < producer_idx) {
    auto consumer_pos = get_buffer_idx(consumer_idx);
    auto remaining_space = PD3_RING_BUFFER_SIZE - consumer_pos;
    if (remaining_space < sizeof(RecordHeader) + 8) {
        consumer_idx += remaining_space;
        continue;
    }
    auto* header = reinterpret_cast<RecordHeader*>(buffer_ + consumer_pos);
    
    if (header->consumer_state.load(std::memory_order_acquire) == CONSUMER_STATE_WRITTEN) {
      // this has not been consumed yet
      // The key is the first 8 bytes of the payload.
      auto memkey = *reinterpret_cast<uint64_t*>(buffer_ + consumer_pos + sizeof(RecordHeader));
      if (memkey == key) {
        // we need to try to claim this
        auto expected_state = CONSUMER_STATE_WRITTEN;
        if (header->consumer_state.compare_exchange_strong(expected_state, 
                                                                CONSUMER_STATE_COMPLETED, 
                                                                std::memory_order_acq_rel,
                                                                std::memory_order_relaxed)) {
          // we have claimed this record
          ConsumeRun();
          return true;
        }
        // someone else claimed this record or it is already completed, continue search
      }
    }
    
    auto size = header->size;
    if (size == 0) {
        // This indicates a problem, likely a race with a producer wrapping around.
        // Re-reading the producer index and retrying is the safest option.
        producer_idx = producer_index_.value.load(std::memory_order_acquire);
        continue;
    }
    consumer_idx += size;
  }
  return false;
}

void LocklessAgentBuffer::ConsumeRun() {
  while (true) {
    uint64_t current_consumer_idx = consumer_index_.value.load(std::memory_order_acquire);
    uint64_t producer_idx = producer_index_.value.load(std::memory_order_acquire);

    if (current_consumer_idx >= producer_idx) {
      return;
    }
    
    auto consumer_pos = get_buffer_idx(current_consumer_idx);
    auto remaining_space = PD3_RING_BUFFER_SIZE - consumer_pos;

    if (remaining_space < sizeof(RecordHeader)) {
        uint64_t next_idx = current_consumer_idx + remaining_space;
        // Atomically skip the gap. If it fails, another thread did it, so we restart.
        consumer_index_.value.compare_exchange_weak(current_consumer_idx, next_idx, std::memory_order_acq_rel, std::memory_order_relaxed);
        continue;
    }

    auto* header = reinterpret_cast<RecordHeader*>(buffer_ + consumer_pos);
    
    // If the head of the queue is not completed, we cannot advance.
    // We also need to handle the case where the producer hasn't finished writing the header yet.
    auto state = header->consumer_state.load(std::memory_order_acquire);
    if (state != CONSUMER_STATE_COMPLETED) {
        return;
    }

    // Now we know state is CONSUMER_STATE_COMPLETED
    auto size = header->size;
    if (size == 0) {
        // This shouldn't happen for a COMPLETED record unless it's a badly formed sentinel.
        // We can't advance, so we stop.
        return;
    }
    
    uint64_t next_consumer_idx = current_consumer_idx + size;
    // Try to advance the consumer index. If this fails, another thread has already advanced it.
    // We just loop again.
    consumer_index_.value.compare_exchange_weak(current_consumer_idx, next_consumer_idx, std::memory_order_acq_rel, std::memory_order_relaxed);
  }
}

void SpinlockAgentBuffer::Initialize() {
  producer_index_.value = 0;
  consumer_index_.value = 0;
  std::memset(buffer_, 0, PD3_RING_BUFFER_SIZE);
}

bool SpinlockAgentBuffer::ProduceBatch(char** data_list, size_t* size_list, size_t num_items, int* num_inserted, size_t contiguous_size) {
  std::lock_guard<LockType> locker(spin_lock_);

  auto consumer_idx = consumer_index_.value;
  auto consumer_pos = get_buffer_idx(consumer_idx);
  auto producer_idx = producer_index_.value;
  auto producer_pos = get_buffer_idx(producer_idx);

  uint64_t distance = PD3_RING_BUFFER_SIZE - producer_idx + consumer_idx;
  const uint64_t buffer = 8;

  if (distance < 8192) [[unlikely]] {
    // the distance needs to be more than 8KB to write a batched response
    return false;
  }

  for (size_t i = 0; i < num_items; i++) {
    producer_idx = producer_index_.value;
    producer_pos = get_buffer_idx(producer_idx);
    consumer_idx = consumer_index_.value;
    consumer_pos = get_buffer_idx(consumer_idx);
    auto actual_size = size_list[i] + sizeof(RecordHeader);
    actual_size = align_to_eight_bytes(actual_size);
    distance = PD3_RING_BUFFER_SIZE - producer_idx + consumer_idx;
    if (distance < actual_size + buffer) {
      break;
    }
    // we have enough place to produce
    auto remaining_space = PD3_RING_BUFFER_SIZE - producer_pos;
    if (remaining_space < sizeof(RecordHeader) + contiguous_size) {
      producer_index_.value += remaining_space;
      producer_pos = get_buffer_idx(producer_index_.value);
      remaining_space = PD3_RING_BUFFER_SIZE - producer_pos;
    }

    if (remaining_space >= actual_size) {
      // just one production
      auto* header = reinterpret_cast<RecordHeader*>(buffer_ + producer_pos);
      header->size = actual_size;
      std::memcpy(buffer_ + producer_pos + sizeof(RecordHeader), data_list[i], size_list[i]);
      header->consumer_state.store(CONSUMER_STATE_WRITTEN, std::memory_order_release);
      producer_index_.value += actual_size;
    } else {
      // two productions
      auto* header = reinterpret_cast<RecordHeader*>(buffer_ + producer_pos);
      header->size = actual_size;
      size_t size_1 = remaining_space - sizeof(RecordHeader);
      size_t size_2 = actual_size - size_1 - sizeof(RecordHeader);
      producer_pos += sizeof(RecordHeader);
      std::memcpy(buffer_ + producer_pos, data_list[i], size_1);
      std::memcpy(buffer_, data_list[i] + size_1, size_2);
      header->consumer_state.store(CONSUMER_STATE_WRITTEN, std::memory_order_release);
      producer_index_.value += actual_size;
    }
    *num_inserted += 1;
  }

  return true;
}

bool SpinlockAgentBuffer::CheckAndReturn(uint64_t key) {
  // get the producer and consumer indices
  spin_lock_.lock();
  auto consumer_idx = consumer_index_.value;
  auto producer_idx = producer_index_.value;
  spin_lock_.unlock();
  
  // check the buffer
  while (consumer_idx < producer_idx) {
    auto consumer_pos = get_buffer_idx(consumer_idx);
    auto remaining_space = PD3_RING_BUFFER_SIZE - consumer_pos;
    if (remaining_space < sizeof(RecordHeader) + 8) {
      consumer_idx += remaining_space;
      continue;
    }
    auto* header = reinterpret_cast<RecordHeader*>(buffer_ + consumer_pos);
    auto memkey = *reinterpret_cast<uint64_t*>(buffer_ + consumer_pos + sizeof(RecordHeader));
    if (memkey == key) {
      std::lock_guard<LockType> locker(spin_lock_);
      if (header->consumer_state.load(std::memory_order_relaxed) == CONSUMER_STATE_WRITTEN) {
        header->consumer_state.store(CONSUMER_STATE_COMPLETED, std::memory_order_relaxed);
        ConsumeRun();
        return true;
      } 
    }
    consumer_idx += header->size;
  }
  return false;
}

void SpinlockAgentBuffer::ConsumeRun() {
  // this is always called when the lock is held
  auto consumer_idx = consumer_index_.value;
  auto producer_idx = producer_index_.value;
  int count = 0;
  while (consumer_idx < producer_idx) {
    auto consumer_pos = get_buffer_idx(consumer_idx);
    auto remaining_space = PD3_RING_BUFFER_SIZE - consumer_pos;
    if (remaining_space < sizeof(RecordHeader) + 8) {
      consumer_idx += remaining_space;
    }
    consumer_pos = get_buffer_idx(consumer_idx);
    auto* header = reinterpret_cast<RecordHeader*>(buffer_ + consumer_pos);
    if (header->consumer_state.load(std::memory_order_relaxed) == CONSUMER_STATE_COMPLETED) {
      consumer_idx += header->size;
      count++;
    } else {
      break;
    }
  }
  consumer_index_.value = consumer_idx;
}

} // namespace buffer
} // namespace dpf