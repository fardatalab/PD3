#include "net_hashmap.hpp"

#include <iostream>
#include <atomic>
#include <csignal>

using namespace pd3::network_hashmap;

std::atomic_bool running{true};

void SignalHandler(int signal) {
  running = false;
}

int main(int argc, char* argv[]) {
  std::cout << "Starting hashmap server" << std::endl;


  NetworkHashMap hashmap;

  Config config;
  config.local_capacity = 1_GiB;
  config.database_size = 16_GiB;
  config.num_threads = 16;
  config.server_ip = "0.0.0.0";
  config.server_port = 12345;
  config.batch_size = 1024;
  hashmap.Configure(config);

  hashmap.Run();

  signal(SIGINT, SignalHandler);
  signal(SIGHUP, SIG_IGN);
  signal(SIGPIPE, SIG_IGN);
  signal(SIGTERM, SignalHandler);

  while (running) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }

  hashmap.Stop();
  std::cout << "Server stopped" << std::endl;

  return 0;
}