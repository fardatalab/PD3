#include "agent_buffer.hpp"

#include <cstring>
#include <stdexcept>
namespace dpf {
namespace buffer {

static uint64_t align_to_eight_bytes(uint64_t pos) {
  return (pos + 7) & ~7;
}

void AgentBuffer::Initialize() {
  producer_index_.value = 0;
  consumer_index_.value = 0;
  std::memset(buffer_, 0, PD3_RING_BUFFER_SIZE);
}

ReservationResult AgentBuffer::Reserve(size_t size, size_t contiguous_size) {
  auto consumer_idx = consumer_index_.value;
  auto reservation_idx = reservation_index_.value;
  auto reservation_pos = GetBufferIdx(reservation_idx);
  auto consumer_pos = GetBufferIdx(consumer_idx);

  auto actual_size = size + sizeof(RecordHeader);
  actual_size = align_to_eight_bytes(actual_size);
  uint64_t distance = PD3_RING_BUFFER_SIZE - reservation_idx + consumer_idx;
  uint64_t buffer = 8; // in case we have to wrap around and leave some space
  if (distance < actual_size + buffer) [[unlikely]] {
    FreeConsumedEntries();
    distance = PD3_RING_BUFFER_SIZE - reservation_idx + consumer_index_.value;
    if (distance < actual_size + buffer) { // no space to reserve
      return ReservationResult();
    }
  }

  auto remaining_space = PD3_RING_BUFFER_SIZE - reservation_pos;
  if (remaining_space < sizeof(RecordHeader) + contiguous_size) {
    // need to wrap around
    std::memset(buffer_ + reservation_pos, 0, remaining_space);
    reservation_index_.value += remaining_space;
    reservation_pos = GetBufferIdx(reservation_index_.value);
    remaining_space = PD3_RING_BUFFER_SIZE - reservation_pos;
  }

  if (remaining_space >= actual_size) {
    auto* header = reinterpret_cast<RecordHeader*>(buffer_ + reservation_pos);
    header->size = actual_size;
    reservation_index_.value += actual_size;
    header->consumer_state.store(CONSUMER_STATE_RESERVED, std::memory_order_relaxed);
    ReservationResult output;
    output.commit_idx = reservation_index_.value;
    output.buffer_1 = buffer_ + reservation_pos + sizeof(RecordHeader);
    output.size_1 = size;
    output.buffer_2 = nullptr;
    output.size_2 = 0;
    return output;
  } else {
    // two reservations
    auto* header = reinterpret_cast<RecordHeader*>(buffer_ + reservation_pos);
    header->size = actual_size;
    auto commit_idx = reservation_index_.value;
    reservation_index_.value += actual_size;
    header->consumer_state.store(CONSUMER_STATE_RESERVED, std::memory_order_relaxed);
    auto size_1 = remaining_space - sizeof(RecordHeader);
    auto size_2 = actual_size - size_1 - sizeof(RecordHeader);
    ReservationResult output;
    output.commit_idx = commit_idx;
    output.buffer_1 = buffer_ + reservation_pos + sizeof(RecordHeader);
    output.size_1 = size_1;
    output.buffer_2 = &buffer_[0];
    output.size_2 = size_2;
    return output;
  }
  return ReservationResult();
}

bool AgentBuffer::Commit(uint64_t commit_idx) {
  auto* header = reinterpret_cast<RecordHeader*>(buffer_ + GetBufferIdx(commit_idx));
  header->consumer_state.store(CONSUMER_STATE_WRITTEN, std::memory_order_relaxed);
  // hack, assuming commits take place in order
  auto expected_new_producer_idx = commit_idx + header->size;
  producer_index_.value = std::max(producer_index_.value, expected_new_producer_idx);
  return true;
}

bool AgentBuffer::Produce(const char* data, size_t size, size_t contiguous_size) {
  auto consumer_idx = consumer_index_.value;
  auto producer_idx = producer_index_.value;
  auto producer_pos = GetBufferIdx(producer_idx);
  auto consumer_pos = GetBufferIdx(consumer_idx);

  auto actual_size = size + sizeof(RecordHeader);
  actual_size = align_to_eight_bytes(actual_size);
  uint64_t distance = PD3_RING_BUFFER_SIZE - producer_idx + consumer_idx;
  uint64_t buffer = 8; // in case we have to wrap around and leave some space
  if (distance < actual_size + buffer) [[unlikely]] {
    FreeConsumedEntries();
    distance = PD3_RING_BUFFER_SIZE - producer_idx + consumer_index_.value;
    if (distance < actual_size + buffer) {
      return false;
    }
  }

  // we have enough place to produce
  auto remaining_space = PD3_RING_BUFFER_SIZE - producer_pos;
  if (remaining_space < sizeof(RecordHeader) + contiguous_size) {
    producer_index_.value += remaining_space;
    producer_pos = GetBufferIdx(producer_index_.value);
    remaining_space = PD3_RING_BUFFER_SIZE - producer_pos;
  }

  if (remaining_space >= actual_size) {
    // just one production
    auto* header = reinterpret_cast<RecordHeader*>(buffer_ + producer_pos);
    header->size = actual_size;
    std::memcpy(buffer_ + producer_pos + sizeof(RecordHeader), data, size);
    producer_index_.value += actual_size;
    header->consumer_state.store(CONSUMER_STATE_WRITTEN, std::memory_order_release);
  } else {
    // two productions
    auto* header = reinterpret_cast<RecordHeader*>(buffer_ + producer_pos);
    header->size = actual_size;
    size_t size_1 = remaining_space - sizeof(RecordHeader);
    size_t size_2 = actual_size - size_1 - sizeof(RecordHeader);
    producer_index_.value += actual_size;
    producer_pos += sizeof(RecordHeader);
    std::memcpy(buffer_ + producer_pos, data, size_1);
    std::memcpy(buffer_, data + size_1, size_2);
    header->consumer_state.store(CONSUMER_STATE_WRITTEN, std::memory_order_release);
  }

  return true;

}

bool AgentBuffer::ProduceBatch(char** data_list, size_t* size_list, size_t num_items, int* num_inserted, size_t contiguous_size) {
  auto consumer_idx = consumer_index_.value;
  auto producer_idx = producer_index_.value;
  auto producer_pos = GetBufferIdx(producer_idx);
  auto consumer_pos = GetBufferIdx(consumer_idx);

  static constexpr uint64_t MIN_DISTANCE_FOR_BATCHED_RESPONSES = 8192;

  *num_inserted = 0;

  // we need to see if we have enough space to produce the batch
  uint64_t distance = PD3_RING_BUFFER_SIZE - producer_idx + consumer_idx;
  const uint64_t buffer = 8; // in case we have to wrap around and leave some space
  // std::cout << "Distance: " << distance << std::endl;
  if (distance < MIN_DISTANCE_FOR_BATCHED_RESPONSES) [[unlikely]] {
    FreeConsumedEntries();
    distance = PD3_RING_BUFFER_SIZE - producer_idx + consumer_index_.value;
    if (distance < MIN_DISTANCE_FOR_BATCHED_RESPONSES) {
      return false;
    }
  }
  // we have enough space to produce the batch
  for (size_t i = 0; i < num_items; i++) {
    producer_idx = producer_index_.value;
    producer_pos = GetBufferIdx(producer_idx);
    consumer_idx = consumer_index_.value;
    consumer_pos = GetBufferIdx(consumer_idx);
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
      producer_pos = GetBufferIdx(producer_index_.value);
      remaining_space = PD3_RING_BUFFER_SIZE - producer_pos;
    }

    if (remaining_space >= actual_size) {
      // std::cout << "One production" << std::endl;
      // just one production
      auto* header = reinterpret_cast<RecordHeader*>(buffer_ + producer_pos);
      header->size = actual_size;
      std::memcpy(buffer_ + producer_pos + sizeof(RecordHeader), data_list[i], size_list[i]);
      producer_index_.value += actual_size;
      header->consumer_state.store(CONSUMER_STATE_WRITTEN, std::memory_order_release);
    } else {
      // two productions
      auto* header = reinterpret_cast<RecordHeader*>(buffer_ + producer_pos);
      header->size = actual_size;
      size_t size_1 = remaining_space - sizeof(RecordHeader);
      size_t size_2 = actual_size - size_1 - sizeof(RecordHeader);
      producer_index_.value += actual_size;
      producer_pos += sizeof(RecordHeader);
      std::memcpy(buffer_ + producer_pos, data_list[i], size_1);
      std::memcpy(buffer_, data_list[i] + size_1, size_2);
      header->consumer_state.store(CONSUMER_STATE_WRITTEN, std::memory_order_release);
    }

    *num_inserted += 1;
  }

  return true;
}

bool AgentBuffer::ProduceInterleave(char* dlist_1, 
                                    size_t sz1, 
                                    char* dlist_2, 
                                    size_t sz2, 
                                    size_t num_items, 
                                    int* num_inserted) 
{
  auto consumer_idx = consumer_index_.value;
  auto producer_idx = producer_index_.value;
  auto producer_pos = GetBufferIdx(producer_idx);
  auto consumer_pos = GetBufferIdx(consumer_idx);

  static constexpr uint64_t MIN_DISTANCE_FOR_BATCHED_RESPONSES = 8192;

  *num_inserted = 0;

  // we need to see if we have enough space to produce the batch
  uint64_t distance = PD3_RING_BUFFER_SIZE - producer_idx + consumer_idx;
  const uint64_t buffer = 8; // in case we have to wrap around and leave some space
  // std::cout << "Distance: " << distance << std::endl;
  if (distance < MIN_DISTANCE_FOR_BATCHED_RESPONSES) [[unlikely]] {
    FreeConsumedEntries();
    distance = PD3_RING_BUFFER_SIZE - producer_idx + consumer_index_.value;
    if (distance < MIN_DISTANCE_FOR_BATCHED_RESPONSES) {
      return false;
    }
  }
  // we have enough space to produce the batch
  for (size_t i = 0; i < num_items; i++) {
    producer_idx = producer_index_.value;
    producer_pos = GetBufferIdx(producer_idx);
    consumer_idx = consumer_index_.value;
    consumer_pos = GetBufferIdx(consumer_idx);
    auto actual_size = sz1 + sz2 + sizeof(RecordHeader);
    actual_size = align_to_eight_bytes(actual_size);
    distance = PD3_RING_BUFFER_SIZE - producer_idx + consumer_idx;
    if (distance < actual_size + buffer) {
      break;
    }
    // we have enough place to produce
    auto remaining_space = PD3_RING_BUFFER_SIZE - producer_pos;
    if (remaining_space < sizeof(RecordHeader)) {
      producer_index_.value += remaining_space;
      producer_pos = GetBufferIdx(producer_index_.value);
      remaining_space = PD3_RING_BUFFER_SIZE - producer_pos;
    }

    if (remaining_space >= actual_size) {
      // just one production
      auto* header = reinterpret_cast<RecordHeader*>(buffer_ + producer_pos);
      header->size = actual_size;
      // copy the first item
      std::memcpy(buffer_ + producer_pos + sizeof(RecordHeader), &dlist_1[i * sz1], sz1);
      // copy the second item
      std::memcpy(buffer_ + producer_pos + sizeof(RecordHeader) + sz1, &dlist_2[i * sz2], sz2);
      producer_index_.value += actual_size;
      header->consumer_state.store(CONSUMER_STATE_WRITTEN, std::memory_order_release);
    } else {
      // two productions
      auto* header = reinterpret_cast<RecordHeader*>(buffer_ + producer_pos);
      header->size = actual_size;
      size_t size_1 = remaining_space - sizeof(RecordHeader);
      size_t size_2 = actual_size - size_1 - sizeof(RecordHeader);
      producer_index_.value += actual_size;
      producer_pos += sizeof(RecordHeader);
      if (size_1 >= sz1) {
        std::memcpy(buffer_ + producer_pos, &dlist_1[i * sz1], sz1);
        auto second_size = sz2 - size_1;
        std::memcpy(buffer_ + producer_pos + sz1, &dlist_2[i * sz2], second_size);
        std::memcpy(buffer_, &dlist_2[i * sz2 + second_size], size_2 - second_size);
      } else {
        std::memcpy(buffer_ + producer_pos, &dlist_1[i], size_1);
        auto second_size = sz1 - size_1;
        std::memcpy(buffer_, &dlist_1[i * sz1 + size_1], second_size);
        std::memcpy(buffer_ + second_size, &dlist_2[i * sz2], sz2);
      }
      header->consumer_state.store(CONSUMER_STATE_WRITTEN, std::memory_order_release);
    }

    *num_inserted += 1;
  }

  return true;
}

void AgentBuffer::FreeConsumedEntries() {
  auto producer_idx = producer_index_.value;
  auto consumer_idx = consumer_index_.value;
  int64_t count = 0;
  while (consumer_idx < producer_idx) {
    auto remaining_space = PD3_RING_BUFFER_SIZE - GetBufferIdx(consumer_idx);
    if (remaining_space < sizeof(RecordHeader)) {
      consumer_idx += remaining_space;
      continue;
    }
    auto header = reinterpret_cast<RecordHeader*>(buffer_ + GetBufferIdx(consumer_idx));
    auto consumer_state = header->consumer_state.load(std::memory_order_relaxed);
    if (consumer_state == CONSUMER_STATE_COMPLETED || consumer_state == CONSUMER_STATE_EMPTY) {
      consumer_idx += header->size;
    } else {
      break;
    }
  }
  consumer_index_.value = consumer_idx;
}


} // namespace buffer
} // namespace dpf