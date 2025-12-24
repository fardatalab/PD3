#pragma once

#include <rigtorp/SPSCQueue.h>

#include "json.h"

#include <string>
#include <iostream>
#include <thread>
#include <fstream>
#include <atomic>

namespace dpf {

struct LatencyData {
  uint64_t memory_access_start;
  uint64_t memory_access_end;
  uint64_t consume_start;
  uint64_t consume_end;
  uint64_t context_start;
  uint64_t context_end;
};

class LatencyWriter {

public:

  LatencyWriter(const std::string& filename)
    : filename_(filename),
      queue_(1024 * 1024) 
  {
    running_.store(true, std::memory_order_relaxed);
    writer_thread_ = std::thread(&LatencyWriter::BackgroundWriter, this);
  }

  ~LatencyWriter()
  {
    running_.store(false, std::memory_order_relaxed);
    writer_thread_.join();
  }

  // kinda specific to fig 21
  void MemoryAccessStart() 
  {
    data_.memory_access_start = GetTimestamp();
  }

  void MemoryAccessEnd() 
  {
    data_.memory_access_end = GetTimestamp();
  }

  void ConsumeStart() 
  {
    data_.consume_start = GetTimestamp();
  }

  void ConsumeEnd() 
  {
    data_.consume_end = GetTimestamp();
  }

  void ContextStart()
  {
    data_.context_start = GetTimestamp();
  }

  void ContextEnd()
  {
    data_.context_end = GetTimestamp();
  }

  void Write()
  {
    queue_.push(data_);
  }

private:
  
  void BackgroundWriter()
  {
    // create the file 
    file_.open(filename_, std::ios::out | std::ios::trunc);
    if (!file_.is_open()) {
      std::cout << "Failed to open file: " << filename_ << std::endl;
      return;
    }

    while (running_.load(std::memory_order_relaxed)) {
      auto latency_data = queue_.front();
      if (latency_data == nullptr) {
        std::this_thread::yield();
        continue;
      }
      JSON json;
      json["memory_access_start"] = latency_data->memory_access_start;
      json["memory_access_end"] = latency_data->memory_access_end;
      json["consume_start"] = latency_data->consume_start;
      json["consume_end"] = latency_data->consume_end;
      json["context_start"] = latency_data->context_start;
      json["context_end"] = latency_data->context_end;
      file_ << json.dump() << '\n';
      file_.flush();
      queue_.pop();
    }
  }

  uint64_t GetTimestamp()
  {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    auto timestamp = ts.tv_sec * 1000000000L + ts.tv_nsec;
    return timestamp;
  }

private:
  
  std::thread writer_thread_;
  std::atomic_bool running_;

  std::string filename_;
  std::ofstream file_;

  rigtorp::SPSCQueue<LatencyData> queue_;

  LatencyData data_;

};

} // namespace dpf