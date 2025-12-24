/**
 * Memory backend for the offload engine
 */

#include "rdma_backend/remote_server.hpp"
#include "rdma_backend/mem_server.hpp"

#include "common/args.hpp"
#include "PD3/literals/memory_literals.hpp"

#include <iostream>
#include <signal.h>
#include <atomic>
#include <thread>
#include <chrono>

std::atomic_bool stop_flag(false);

void SignalHandler(int signum) {
  if (signum == SIGINT || signum == SIGTERM) {
    std::cout << "Stopping server..." << std::endl;
    stop_flag.store(true, std::memory_order_relaxed);
  }
}

using namespace dpf::memory_literals;

int main(int argc, char** argv) {
  
  args::ArgumentParser parser("memory backend for the offload engine");
  args::HelpFlag help(parser, "help", "Display this help menu", {'h', "help"});
  args::ValueFlag<std::string> server_addr_flag(parser, "server_addr", "The address of the server", {'a', "addr"});
  args::ValueFlag<std::string> server_port_flag(parser, "server_port", "The port of the server", {'p', "port"});
  args::ValueFlag<std::string> server_name_flag(parser, "server_name", "The name of the server", {'n', "name"});
  args::ValueFlag<uint32_t> max_wr_flag(parser, "max_wr", "The maximum number of WRs", {'m', "max-wr"});
  args::ValueFlag<size_t> segment_size_flag(parser, "segment_size", "The size of the segment", {'s', "segment-size"});
  args::Flag prealloc_flag(parser, "prealloc", "Preallocate memory regions", {'q', "prealloc"});

  try {
    parser.ParseCLI(argc, argv);
  } catch (const args::Completion& e) {
    std::cerr << e.what() << '\n';
    return 0;
  } catch (const args::Help&) {
    std::cout << parser;
    return 0;
  } catch (const args::ParseError& e) {
    std::cerr << e.what() << '\n';
    std::cerr << parser;
    return 1;
  }

  // defaults
  // size_t segment_size = 1_MiB;
  size_t segment_size = 1 << 25;
  uint32_t max_wr = 1024;
  std::string server_addr = "10.10.1.2";
  std::string server_port = "12345";
  std::string server_name = "memory_backend";
  bool prealloc = false;
  
  // parse args
  if (server_addr_flag) {
    server_addr = args::get(server_addr_flag);
  }
  if (server_port_flag) {
    server_port = args::get(server_port_flag);
  }
  if (server_name_flag) {
    server_name = args::get(server_name_flag);
  }
  if (max_wr_flag) {
    max_wr = args::get(max_wr_flag);
  }
  if (segment_size_flag) {
    segment_size = args::get(segment_size_flag);
  }
  if (prealloc_flag) {
    prealloc = true;
  }

  std::cout << "Server address: " << server_addr << std::endl;
  std::cout << "Server port: " << server_port << std::endl;
  std::cout << "Server name: " << server_name << std::endl;
  std::cout << "Max WRs: " << max_wr << std::endl;
  std::cout << "Segment size: " << segment_size << std::endl;

  if (prealloc) {

    dpf::MemoryPreallocatedServer server;
    segment_size = 1024 * 1024 * 1024;
    size_t num_segments = 20;
    server.Configure(server_addr, server_port, server_name, max_wr, segment_size, num_segments);
    server.Run();

    signal(SIGINT, SignalHandler);
    signal(SIGTERM, SignalHandler);

    std::cout << "Stop program with CTRL+C or CTRL+\\\n";

    while (!stop_flag.load(std::memory_order_relaxed)) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    server.Stop();

    return 0;
  }

  dpf::RemoteServer server;
  server.Configure(server_addr, server_port, server_name, max_wr, segment_size);
  server.Run();

  signal(SIGINT, SignalHandler);
  signal(SIGTERM, SignalHandler);

  std::cout << "Stop program with CTRL+C or CTRL+\\\n";

  while (!stop_flag.load(std::memory_order_relaxed)) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }

  server.Stop();

  return 0;
}