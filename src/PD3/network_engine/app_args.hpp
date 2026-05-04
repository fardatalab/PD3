#pragma once

#include "json.h"
#include "flow_common.h" // from doca-flow

#include <string>
#include <iostream>
#include <fstream>

struct AppArgs {
  struct flow_dev_ctx flow_dev_ctx = {}; // devices for doca-flow
  JSON config = {}; // config for other parts of the app
};

struct flow_dev_ctx* ContextConverter(void* user_ctx) {
  AppArgs* app_args = (AppArgs*)user_ctx;
  return &app_args->flow_dev_ctx;
}

doca_error_t ConfigCallback(void* arg, void* params) {
  std::string config_path{(char*)arg};
  std::ifstream config_fstream{config_path};
  if (!config_fstream.is_open()) {
    std::cerr << "Failed to open config file: " << config_path << std::endl;
    return DOCA_ERROR_INVALID_VALUE;
  }
  config_fstream >> ((AppArgs*)params)->config;
  return DOCA_SUCCESS;
}

doca_error_t RegisterConfigParam() {
  doca_error_t result;
  struct doca_argp_param* config_path_param;

  result = doca_argp_param_create(&config_path_param);
  if (result != DOCA_SUCCESS) {
    std::cerr << "Failed to create config path param: " << doca_error_get_descr(result) << '\n';
    return result;
  }

  doca_argp_param_set_short_name(config_path_param, "c");
  doca_argp_param_set_long_name(config_path_param, "config");
  doca_argp_param_set_arguments(config_path_param, "<str>");
  doca_argp_param_set_description(config_path_param, "Path to the app config file");
  doca_argp_param_set_callback(config_path_param, ConfigCallback);
  doca_argp_param_set_type(config_path_param, DOCA_ARGP_TYPE_STRING);
  result = doca_argp_register_param(config_path_param);
  if (result != DOCA_SUCCESS) {
    std::cerr << "Failed to register config path param: " << doca_error_get_descr(result) << '\n';
    return result;
  }

  return result;
}

doca_error_t RegisterFlowDevParams() {
  return register_flow_device_params(ContextConverter);
}

doca_error_t RegisterFlowStatsParams() {
  return register_flow_stats_params();
}

doca_error_t InitDpdk(int argc, char** argv) {
  return flow_init_dpdk(argc, argv);
}

doca_error_t InitDevices(AppArgs* app_args) {
  return init_doca_flow_devs(&app_args->flow_dev_ctx);
}