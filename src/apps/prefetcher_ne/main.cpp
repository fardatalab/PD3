#include "json.h"
#include "network_engine/network_engine_main.hpp"
#include "network_engine/prefetcher_interface.hpp"
#include "prefetcher/prefetcher.hpp"

#include <doca_argp.h>
#include <doca_log.h>

#include <dpdk_utils.h>

#include <fstream>
#include <stdexcept>

DOCA_LOG_REGISTER(PREFETCHER::MAIN);

#define NB_PORTS 1
#define NB_QUEUES 1

struct AppParams {
  static doca_error_t ConfigPathCallback(void *arg, void *params) {
    std::string config_path{(char *)arg};
    std::ifstream config_fstream{config_path};
    if (!config_fstream.is_open()) {
      DOCA_LOG_ERR("Failed to open config file: %s", config_path.c_str());
      return DOCA_ERROR_INVALID_VALUE;
    }
    config_fstream >> ((AppParams *)params)->config;
    return DOCA_SUCCESS;
  }

  JSON config{};
};

doca_error_t
RegisterAppParams() {
  doca_error_t result;
  doca_argp_param *config_path_param;

  result = doca_argp_param_create(&config_path_param);
  if (result != DOCA_SUCCESS) {
    DOCA_LOG_ERR("Failed to create config path param: %s", doca_get_error_string(result));
    return result;
  }
  doca_argp_param_set_short_name(config_path_param, "c");
  doca_argp_param_set_long_name(config_path_param, "config");
  doca_argp_param_set_arguments(config_path_param, "<str>");
  doca_argp_param_set_description(config_path_param, "Path to the app config file");
  doca_argp_param_set_callback(config_path_param, &AppParams::ConfigPathCallback);
  doca_argp_param_set_type(config_path_param, DOCA_ARGP_TYPE_STRING);
  result = doca_argp_register_param(config_path_param);
  if (result != DOCA_SUCCESS) {
    DOCA_LOG_ERR("Failed to register config path param: %s", doca_get_error_string(result));
    return result;
  }

  return result;
}

int
main(int argc, char **argv)
{
  doca_error_t result;
  int exit_status = EXIT_SUCCESS;
  AppParams app_args{};

  // argument parsing
  result = doca_argp_init("prefetcher", &app_args);
  if (result != DOCA_SUCCESS) {
    DOCA_LOG_ERR("Failed to init ARGP resources: %s", doca_get_error_string(result));
    return EXIT_FAILURE;
  }
  doca_argp_set_dpdk_program(dpdk_init);
  std::cout << "Set DPDK program\n";

  result = RegisterAppParams();
  if (result != DOCA_SUCCESS) {
    DOCA_LOG_ERR("Failed to register app params: %s", doca_get_error_string(result));
    doca_argp_destroy();
    return EXIT_FAILURE;
  }

  result = doca_argp_start(argc, argv);
  if (result != DOCA_SUCCESS) {
    DOCA_LOG_ERR("Failed to parse app input: %s", doca_get_error_string(result));
    doca_argp_destroy();
    return EXIT_FAILURE;
  }

  // dpdk init
  application_port_config port_config{
    .nb_ports = NB_PORTS,
    .nb_queues = NB_QUEUES
  };
  application_dpdk_config dpdk_config{
    .port_config = port_config
  };
  result = dpdk_queues_and_ports_init(&dpdk_config);
  if (result != DOCA_SUCCESS) {
    DOCA_LOG_ERR("Failed to update ports and queues");
    dpdk_fini();
    doca_argp_destroy();
    return EXIT_FAILURE;
  }
  std::cout << "Init DPDK\n";

  try {
    // run unsafe application code that might throw
    dpf::Prefetcher prefetcher{};

    // Set the Prefetcher pointer so the Network Engine code can access it
    SetPrefetcher(&prefetcher);

    bool run_networkengine_only = app_args.config.count("ne_only") &&
                                  app_args.config["ne_only"].get<bool>();
    if (!run_networkengine_only) {
      prefetcher.Initialize(app_args.config, true);
      std::cout << "Initialized prefetcher\n";
      prefetcher.Run();  // spins off a thread for the prefetcher
      std::cout << "Started prefetcher\n";
    }
    std::cout << "Running network engine\n";
    dpf::RunNetworkEngineMain(app_args.config);  // can be stopped with CTRL-C
    std::cout << "Stopped network engine\n";
    
    prefetcher.Stop();
  } catch (const std::exception& e) {
    DOCA_LOG_ERR("Encountered exception: %s", e.what());
    exit_status = EXIT_FAILURE;
  } catch (...) {
    DOCA_LOG_ERR("Encountered unknown exception");
    exit_status = EXIT_FAILURE;
  }

  // cleanup resources
  DOCA_LOG_INFO("Cleaning up...");
  dpdk_queues_and_ports_fini(&dpdk_config);
  dpdk_fini();
  doca_argp_destroy();
  return exit_status;
}
