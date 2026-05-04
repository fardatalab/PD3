#include "spmc_buffer.hpp"

#include "system/utils.hpp"

#include <stdexcept>
#include <cstring>
#include <iostream>
#include <thread>
#include <chrono>
#include <limits>
#include <vector>
#include <algorithm>

namespace dpf {
namespace common {

SpmcBuffer::SpmcBuffer() {
  if (!detail::IsPowerOfTwo(PD3_RING_BUFFER_SIZE)) {
    throw std::invalid_argument("PD3_RING_BUFFER_SIZE must be a power of two");
  }
  buffer_mask_ = PD3_RING_BUFFER_SIZE - 1;
}

void SpmcBuffer::Initialize() {
  producer_index_.value.store(0);
  consumer_index_.value.store(0);
  consumer_res_index_.value.store(0);
  reservation_index_.value = 0;
  consumer_index_buffer_.resize(2);
  std::memset(buffer_, 0, PD3_RING_BUFFER_SIZE);
}

void SpmcBuffer::Initialize(size_t num_consumers) {
  Initialize();
  consumer_index_buffer_.resize(num_consumers);
}

static uint64_t align_to_eight_bytes(uint64_t pos) {
  return (pos + 7) & ~7;
}

ReservationResult SpmcBuffer::Reserve(size_t size, size_t contiguous_size) {
  auto consumer_idx = consumer_index_.value.load(std::memory_order_relaxed);
  auto consumer_idx_pos = GetBufferIdx(consumer_idx);
  auto curr_pos = GetBufferIdx(reservation_index_.value);
  size_t distance = 0;

  auto actual_size = size + sizeof(RecordHeader);
  auto size_w_padding = align_to_eight_bytes(actual_size);
  auto padding = size_w_padding - actual_size;
  actual_size = size_w_padding;

  if (curr_pos < consumer_idx_pos) {
    distance = consumer_idx_pos - curr_pos;
  } else {
    distance = PD3_RING_BUFFER_SIZE - curr_pos + consumer_idx_pos;
  }
  if (distance < actual_size) {
    return ReservationResult();
  }

  auto remaining_space = PD3_RING_BUFFER_SIZE - GetBufferIdx(curr_pos);

  if (remaining_space < sizeof(RecordHeader) + contiguous_size) {
    // need to wrap around
    curr_pos += remaining_space;
  }

  if (remaining_space >= actual_size) {
    // write the record header
    auto* header = reinterpret_cast<RecordHeader*>(buffer_ + GetBufferIdx(curr_pos));
    header->consumer_state.store(CONSUMER_STATE_RESERVED, std::memory_order_relaxed);
    header->size = actual_size;
    reservation_index_.value += actual_size;
    ReservationResult output;
    output.commit_idx = curr_pos;
    curr_pos += sizeof(RecordHeader);
    output.buffer_1 = buffer_ + GetBufferIdx(curr_pos);
    output.size_1 = size;
    output.buffer_2 = nullptr;
    output.size_2 = 0;
    return output;
  } else {
    auto* header = reinterpret_cast<RecordHeader*>(buffer_ + GetBufferIdx(curr_pos));
    header->consumer_state.store(CONSUMER_STATE_RESERVED, std::memory_order_relaxed);
    header->size = actual_size;
    auto size_1 = remaining_space - sizeof(RecordHeader);
    auto size_2 = actual_size - size_1 - sizeof(RecordHeader);
    reservation_index_.value += actual_size;
    ReservationResult output;
    output.commit_idx = curr_pos;
    curr_pos += sizeof(RecordHeader);
    output.buffer_1 = buffer_ + GetBufferIdx(curr_pos);
    output.size_1 = size_1;
    output.buffer_2 = &buffer_[0];
    output.size_2 = size_2;
    return output;
  }
}

bool SpmcBuffer::Commit(uint64_t producer_pos) {
  // walk until reservation index to see how many bytes have been written
  // commit the producer pos
  auto* header = reinterpret_cast<RecordHeader*>(buffer_ + GetBufferIdx(producer_pos));
  header->consumer_state.store(CONSUMER_STATE_WRITTEN, std::memory_order_relaxed);

  auto curr_pos = producer_index_.value.load(std::memory_order_relaxed);
  auto res_idx = reservation_index_.value;
  size_t msgs_written = 0;
  while (curr_pos < res_idx) {
    auto bytes_to_end = PD3_RING_BUFFER_SIZE - GetBufferIdx(curr_pos);
    if (bytes_to_end < sizeof(RecordHeader)) {
      curr_pos += bytes_to_end;
      continue;
    }
    auto header = reinterpret_cast<RecordHeader*>(buffer_ + GetBufferIdx(curr_pos));
    if (header->consumer_state.load(std::memory_order_relaxed) == CONSUMER_STATE_WRITTEN) {
      msgs_written++;
      curr_pos += header->size;
    } else {
      break;
    }
  }
  if (msgs_written == 0) {
    return false;
  }
  producer_index_.value.store(curr_pos, std::memory_order_release);
  return true;
}

// std::pair<char*, size_t> SpscBuffer::Front() {
//   // currently just consumes one record at a time, but will extend it to consume a batch
//   auto curr_pos = consumer_index_.value.load(std::memory_order_relaxed);
//   if (curr_pos + sizeof(RecordHeader) > PD3_RING_BUFFER_SIZE) {
//     curr_pos = 0;  // TODO: we might just have to increment this
//   }
//   auto header = reinterpret_cast<RecordHeader*>(buffer_ + GetBufferIdx(curr_pos));  // TODO: figure alignment here
//   if (!header->is_written) {
//     return std::make_pair(nullptr, 0);
//   }
//   auto size = header->size;
//   return std::make_pair(buffer_ + curr_pos, size);
//}

ConsumerResult SpmcBuffer::Front() {
  ConsumerResult output;
  output.buffer_1 = nullptr;
  output.size_1 = 0;
  output.buffer_2 = nullptr;
  output.size_2 = 0;
  output.pop_idx = 0;
  auto curr_pos_og = consumer_res_index_.value.load(std::memory_order_acquire);
  auto curr_pos = curr_pos_og;
  
  auto remaining_space = PD3_RING_BUFFER_SIZE - GetBufferIdx(curr_pos);
  if (remaining_space < sizeof(RecordHeader)) {
    curr_pos += remaining_space;
  }
  auto header = reinterpret_cast<RecordHeader*>(buffer_ + GetBufferIdx(curr_pos));
  auto expected_state = CONSUMER_STATE_WRITTEN;
  if (!header->consumer_state.compare_exchange_weak(expected_state, CONSUMER_STATE_PROCESSING, std::memory_order_acquire, std::memory_order_relaxed)) {
    // another thread is already processing this message, retry
    return output;
  }
  // we have successfully claimed the message
  auto size = header->size;
  auto size_wout_header = size - sizeof(RecordHeader);
  auto msg_idx = curr_pos + sizeof(RecordHeader);
  auto buf_msg_idx = GetBufferIdx(msg_idx);
  if (consumer_res_index_.value.compare_exchange_weak(curr_pos_og, curr_pos + size, std::memory_order_acquire, std::memory_order_release)) {
    output.buffer_1 = buffer_ + buf_msg_idx;
    if (buf_msg_idx + size_wout_header > PD3_RING_BUFFER_SIZE) {
      output.size_1 = PD3_RING_BUFFER_SIZE - buf_msg_idx;
      output.buffer_2 = buffer_ + 0;
      output.size_2 = size - output.size_1 - sizeof(RecordHeader);
    } else {
      output.size_1 = size_wout_header;
      output.buffer_2 = nullptr;
      output.size_2 = 0;
    }
    output.pop_idx = curr_pos;
    return output;
  }
  return output;
}

void SpmcBuffer::Pop(uint64_t curr_pos) {
  auto header = reinterpret_cast<RecordHeader*>(buffer_ + GetBufferIdx(curr_pos));
  auto expected_state = CONSUMER_STATE_PROCESSING;
  if (!header->consumer_state.compare_exchange_weak(expected_state, CONSUMER_STATE_COMPLETED, std::memory_order_acquire, std::memory_order_release)) {
    // another thread beat us to it
    return;
  }
  // memset(buffer_ + sizeof(RecordHeader), 0, header->size);
  // uint64_t new_consumer_idx = curr_pos + header->size + sizeof(RecordHeader);
  // ConsumeRun();

}

void SpmcBuffer::Pop(uint64_t curr_pos, uint64_t thread_id) {
  auto header = reinterpret_cast<RecordHeader*>(buffer_ + GetBufferIdx(curr_pos));
  auto expected_state = CONSUMER_STATE_PROCESSING;
  if (!header->consumer_state.compare_exchange_weak(expected_state, CONSUMER_STATE_COMPLETED, std::memory_order_acquire, std::memory_order_release)) {
    // another thread beat us to it
    return;
  }
  // consumer_index_buffer_[thread_id].value = curr_pos;
  UpdateConsumerIndex();
  // memset(buffer_ + sizeof(RecordHeader), 0, header->size);
  // uint64_t new_consumer_idx = curr_pos + header->size + sizeof(RecordHeader);
  // ConsumeRun();
}

void SpmcBuffer::ConsumeRun() {
  while (true) {
    auto curr_pos_og = consumer_index_.value.load(std::memory_order_acquire);
    auto res_index = consumer_res_index_.value.load(std::memory_order_acquire);
    auto curr_pos = curr_pos_og;
    if (curr_pos_og >= res_index) {
      return;
    }
    if (curr_pos_og + sizeof(RecordHeader) > PD3_RING_BUFFER_SIZE) {
      curr_pos = 0;
    }
    auto header = reinterpret_cast<RecordHeader*>(buffer_ + GetBufferIdx(curr_pos));
    auto expected_state = CONSUMER_STATE_COMPLETED;
    if (!header->consumer_state.compare_exchange_weak(expected_state, CONSUMER_STATE_EMPTY, std::memory_order_acquire, std::memory_order_release)) {
      if (expected_state == CONSUMER_STATE_PROCESSING) {
        return;
      }
      continue;
    }
    auto size = header->size;
    consumer_index_.value.fetch_add(size, std::memory_order_relaxed);
  }
}

void SpmcBuffer::ConsumeRunToProducerIndex() {
  // while (cosumer)
  // auto curr_pos_og = consumer_index_.value.load(std::memory_order_relaxed);
  // auto res_index = producer_index_.value.load(std::memory_order_relaxed);
  while (true) {
    auto curr_pos_og = consumer_index_.value.load(std::memory_order_acquire);
    auto res_index = producer_index_.value.load(std::memory_order_acquire);
    auto curr_pos = curr_pos_og;
    if (curr_pos_og >= res_index) {
      return;
    }
    if (curr_pos_og + sizeof(RecordHeader) > PD3_RING_BUFFER_SIZE) {
      curr_pos = 0;
    }
    auto header = reinterpret_cast<RecordHeader*>(buffer_ + GetBufferIdx(curr_pos));
    auto expected_state = CONSUMER_STATE_COMPLETED;
    if (!header->consumer_state.compare_exchange_weak(expected_state, CONSUMER_STATE_EMPTY, std::memory_order_acquire, std::memory_order_release)) {
      if (expected_state == CONSUMER_STATE_PROCESSING || expected_state == CONSUMER_STATE_WRITTEN) {
        return;
      }
      continue;
    } 
    auto size = header->size;
    consumer_index_.value.fetch_add(size, std::memory_order_relaxed);
    curr_pos_og += size;
  }
}

char* SpmcBuffer::GetRecordAtPos(uint64_t& pos) {
  auto buffer_pos = GetBufferIdx(pos);
  auto remaining_space = PD3_RING_BUFFER_SIZE - buffer_pos;
  if (remaining_space < sizeof(RecordHeader)) {
    buffer_pos += remaining_space;
    pos = buffer_pos;
  }
  return buffer_ + buffer_pos;
}

uint64_t SpmcBuffer::GetMinConsumerIndex() {
  uint64_t min_value = std::numeric_limits<uint64_t>::max();
  for (int i = 0; i < consumer_index_buffer_.size(); i++) {
    min_value = std::min(min_value, consumer_index_buffer_[i].value);
  }
  return min_value;
}

void SpmcBuffer::StoreConsumerIndex(uint64_t consumer_index, int thread_id) {
  consumer_index_buffer_[thread_id].value = consumer_index;
}

void SpmcBuffer::UpdateConsumerIndex() {
  // consumer_index_.value.store(consumer_index_buffer_[0].value, std::memory_order_relaxed);
  auto curr_value = consumer_index_.value.load(std::memory_order_relaxed);
  auto set_value = GetMinConsumerIndex();
  while (curr_value < set_value) {
    consumer_index_.value.compare_exchange_weak(curr_value, set_value);
    set_value = GetMinConsumerIndex();
  }
}



///
/// AgentBuffer
///

void AgentBuffer::Initialize() {
  producer_index_.value = 0;
  consumer_index_.value = 0;
  std::memset(buffer_, 0, PD3_RING_BUFFER_SIZE);
}

bool AgentBuffer::Produce(const char* data, size_t size, size_t contiguous_size) {
  auto consumer_idx = consumer_index_.value;
  auto producer_idx = producer_index_.value;
  auto producer_pos = GetBufferIdx(producer_idx);
  auto consumer_pos = GetBufferIdx(consumer_idx);
  // std::cout << "Producer pos: " << producer_pos << " Consumer pos: " << consumer_pos << std::endl;
  // std::cout << "Producer idx: " << producer_idx << " Consumer idx: " << consumer_idx << std::endl;

  auto actual_size = size + sizeof(RecordHeader);
  actual_size = align_to_eight_bytes(actual_size);
  // std::cout << "Actual size: " << actual_size << std::endl;
  uint64_t distance = PD3_RING_BUFFER_SIZE - producer_idx + consumer_idx;
  uint64_t buffer = 8; // in case we have to wrap around and leave some space
  // std::cout << "Distance: " << distance << std::endl;
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
    // std::cout << "One production" << std::endl;
    // just one production
    auto* header = reinterpret_cast<RecordHeader*>(buffer_ + producer_pos);
    header->size = actual_size;
    std::memcpy(buffer_ + producer_pos + sizeof(RecordHeader), data, size);
    producer_index_.value += actual_size;
    header->consumer_state.store(CONSUMER_STATE_WRITTEN, std::memory_order_release);
  } else {
    // std::cout << "Two productions" << std::endl;
    // std::cout << "Remaining space: " << remaining_space << std::endl;
    // std::cout << "Actual size: " << actual_size << std::endl;
    // std::cout << "Producer pos: " << GetBufferIdx(producer_idx) << std::endl;
    // std::cout << "Producer idx: " << producer_idx << std::endl;
    // std::cout << "Consumer idx: " << consumer_idx << std::endl;
    // std::cout << "Distance: " << distance << std::endl;
    // std::cout << "Consumer pos: " << consumer_pos << std::endl;
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
  std::cout << "Producer idx: " << producer_idx << " Consumer idx: " << consumer_idx << std::endl;
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

void AgentBuffer::FreeConsumedEntries() {
  auto producer_idx = producer_index_.value;
  auto consumer_idx = consumer_index_.value;
  // std::cout << "consumer_idx_1: " << consumer_idx << '\n';
    // we need to see what we can consume
  // std::cout << "================================================" << '\n';
  std::cout << "producer_idx: " << producer_idx << " consumer_idx: " << consumer_idx << '\n';
  int64_t count = 0;
  while (consumer_idx < producer_idx) {
    auto header = reinterpret_cast<RecordHeader*>(buffer_ + GetBufferIdx(consumer_idx));
    if (consumer_idx == 134217720) {
      std::cout << "GetBufferIdx(consumer_idx): " << GetBufferIdx(consumer_idx) << '\n';
      std::cout << "header->consumer_state: " << header->consumer_state.load(std::memory_order_relaxed) << '\n';
    }
    if (header->consumer_state.load(std::memory_order_relaxed) == CONSUMER_STATE_COMPLETED) {
      // std::cout << "header->consumer_state: " << header->consumer_state.load(std::memory_order_relaxed) << '\n';
      consumer_idx += header->size;
      count++;
      if (count % 100000 == 0) {
        std::cout << "producer_idx: " << producer_idx << " consumer_idx: " << consumer_idx << " inside \n";
      }
    } else {
      break;
    }
  }
  consumer_index_.value = consumer_idx;
  // std::cout << "consumer_idx_2: " << consumer_index_.value << '\n';
  // std::cout << "================================================" << '\n';
}

} // namespace common
} // namespace dpf