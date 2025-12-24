// Code in this file is adapted from /opt/mellanox/doca/samples/doca_flow/flow_rss_meta/

#include <iostream>
#include <arpa/inet.h>

#include <doca_error.h>
#include <doca_flow.h>
#include <doca_flow_net.h>
#include <doca_log.h>

#include "PD3/system/logger.hpp"

extern "C" {
#include "flow_common.h"
}

#include "network_engine.hpp"
#include "network_engine_main.hpp"

DOCA_LOG_REGISTER(NETWORK_ENGINE::MAIN);

#define NB_PORTS 1
#define NB_QUEUES 1
#define PORT_ID 0
#define QUEUE_ID 0

namespace {

struct NetworkEngineConfig {
  NetworkEngineConfig(const JSON &ne_config)
      : sf_port(ne_config["sf_port"].get<std::string>()),
        pf_port(ne_config["pf_port"].get<std::string>()),
        ovs_bridge(ne_config["ovs_bridge"].get<std::string>()),
        dst_ip_addr(
            ParseIPv4String(ne_config["dst_ip_addr"].get<std::string>())),
        src_ip_addr(
            ParseIPv4String(ne_config["src_ip_addr"].get<std::string>())),
        dst_port(htons(ne_config["dst_port"].get<uint16_t>())) {}

  std::string sf_port;
  std::string pf_port;
  std::string ovs_bridge;
  doca_be32_t dst_ip_addr;
  doca_be32_t src_ip_addr;
  doca_be16_t dst_port;

 private:
  static doca_be32_t ParseIPv4String(const std::string &ip_str) {
    doca_be32_t ip;
    if (inet_pton(AF_INET, ip_str.c_str(), &ip) != 1) {
      throw std::invalid_argument("Invalid IPv4 address: " + ip_str);
    }
    return ip;
  }
};

//
// Set up packet mirroring from a DPU's physical function
// to a scalable function.
//
int SetUpMirror(const NetworkEngineConfig &ne_config) {
  char cmd[1024];
  FILE *fp;
  char output[1024];

  sprintf(cmd, "sudo ovs-vsctl \
    -- --id=@p1 get port %s \
    -- --id=@p2 get port %s \
    -- --id=@m create mirror name=m0 select-dst-port=@p2 select-src-port=@p2 output-port=@p1 \
    -- set bridge %s mirrors=@m 2>&1", ne_config.sf_port.c_str(), ne_config.pf_port.c_str(), ne_config.ovs_bridge.c_str());

  printf("Setting up packet mirroring: %s\n", cmd);

  fp = popen(cmd, "r");
  if (fp == NULL) {
    return -1;
  }

  while (fgets(output, sizeof(output), fp) != NULL) {
  }

  pclose(fp);
  return 0;
}

//
// Tear down packet mirroring.
//
int TearDownMirror(const NetworkEngineConfig &ne_config) {
  char cmd[1024];
  FILE *fp;
  char output[1024];

  sprintf(cmd, "sudo ovs-vsctl clear bridge %s mirrors", ne_config.ovs_bridge.c_str());

  fp = popen(cmd, "r");
  if (fp == NULL) {
    return -1;
  }

  while (fgets(output, sizeof(output), fp) != NULL) {
  }

  pclose(fp);
  return 0;
}

/*
 * Create DOCA Flow pipe with 5 tuple match, forward RSS and forward miss drop
 *
 * @port [in]: port of the pipe
 * @pipe [out]: created pipe pointer
 * @error [out]: output error
 * @return: 0 on success, negative value otherwise and error is set.
 */
int
CreateRSSPipe(struct doca_flow_port *port, struct doca_flow_pipe **pipe)
{
  struct doca_flow_match match;
  struct doca_flow_actions actions, *actions_arr[NB_ACTIONS_ARR];
  struct doca_flow_fwd fwd;
  struct doca_flow_fwd fwd_miss;
  struct doca_flow_pipe_cfg* pipe_cfg;
  uint16_t rss_queues[1];

  memset(&match, 0, sizeof(match));
  memset(&actions, 0, sizeof(actions));
  memset(&fwd, 0, sizeof(fwd));
  memset(&fwd_miss, 0, sizeof(fwd_miss));

  actions.meta.pkt_meta = UINT32_MAX;

  /* 5 tuple match */
  match.outer.l4_type_ext = DOCA_FLOW_L4_TYPE_EXT_TCP;
  match.outer.l3_type = DOCA_FLOW_L3_TYPE_IP4;
  match.outer.ip4.src_ip = 0xffffffff;
  match.outer.ip4.dst_ip = 0xffffffff;
  match.outer.tcp.l4_port.src_port = 0xffff; // src port is ignored
  match.outer.tcp.l4_port.dst_port = 0xffff;

  doca_error_t result = doca_flow_pipe_cfg_create(&pipe_cfg, port);
  if (result != DOCA_SUCCESS) {
    DOCA_LOG_ERR("Failed to create doca_flow_pipe_cfg: %s", doca_error_get_descr(result));
    return result;
  }
  result = set_flow_pipe_cfg(pipe_cfg, "RSS_META_PIPE", DOCA_FLOW_PIPE_BASIC, true);
  if (result != DOCA_SUCCESS) {
    DOCA_LOG_ERR("Failed to set doca_flow_pipe_cfg: %s", doca_error_get_descr(result));
    doca_flow_pipe_cfg_destroy(pipe_cfg);
    return -1;
  }
  result = doca_flow_pipe_cfg_set_match(pipe_cfg, &match, NULL);
  if (result != DOCA_SUCCESS) {
    DOCA_LOG_ERR("Failed to set doca_flow_pipe_cfg match: %s", doca_error_get_descr(result));
    doca_flow_pipe_cfg_destroy(pipe_cfg);
    return -1;
  }
  result = doca_flow_pipe_cfg_set_actions(pipe_cfg, actions_arr, NULL, NULL, NB_ACTIONS_ARR);
  if (result != DOCA_SUCCESS) {
    DOCA_LOG_ERR("Failed to set doca_flow_pipe_cfg actions: %s", doca_error_get_descr(result));
    doca_flow_pipe_cfg_destroy(pipe_cfg);
    return -1;
  }

  /* RSS queue - send matched traffic to queue 0  */
  rss_queues[0] = 0;
  fwd.type = DOCA_FLOW_FWD_RSS;
	fwd.rss_type = DOCA_FLOW_RESOURCE_TYPE_NON_SHARED;
  fwd.rss.queues_array = rss_queues;
  fwd.rss.inner_flags = DOCA_FLOW_RSS_TCP | DOCA_FLOW_RSS_IPV4;
  fwd.rss.nr_queues = NB_QUEUES;

  fwd_miss.type = DOCA_FLOW_FWD_DROP;

  auto res = doca_flow_pipe_create(pipe_cfg, &fwd, &fwd_miss, pipe);
  if (res != DOCA_SUCCESS) {
    DOCA_LOG_ERR("Failed to create pipe - %s (%s)", doca_error_get_name(res));
    doca_flow_pipe_cfg_destroy(pipe_cfg);
    return -1;
  }

  return 0;
}

/*
 * Add DOCA Flow pipe entry with 5 tuple to match
 *
 * @pipe [in]: pipe of the entry
 * @port [in]: port of the entry
 * @error [out]: output error
 * @return: 0 on success, negative value otherwise and error is set.
*/
int AddRSSPipeEntry(struct doca_flow_pipe *flowpipe,
                    struct doca_flow_port *port,
                    struct entries_status *status,
                    const NetworkEngineConfig &ne_config) {
  struct doca_flow_match match;
  struct doca_flow_pipe_entry *entry;
  struct doca_flow_actions actions;
  int num_of_entries = 1;
  doca_error_t res;

  /* actual 5-tuple to match */
  memset(&match, 0, sizeof(match));
  memset(&actions, 0, sizeof(actions));

  match.outer.ip4.dst_ip = ne_config.dst_ip_addr;
  match.outer.ip4.src_ip = ne_config.src_ip_addr;
  match.outer.tcp.l4_port.dst_port = ne_config.dst_port;
  match.outer.tcp.l4_port.src_port = 0;
  std::cout << "dst_port: " << match.outer.tcp.l4_port.dst_port << '\n';
  std::cout << "src_port: " << match.outer.tcp.l4_port.src_port << '\n';
  std::cout << "dst_ip: " << match.outer.ip4.dst_ip << '\n';
  std::cout << "src_ip: " << match.outer.ip4.src_ip << '\n';

  actions.meta.pkt_meta = 10;

  res = doca_flow_pipe_add_entry(0, flowpipe, &match, 0, &actions, nullptr, nullptr, 0, status, &entry);
  if (res != DOCA_SUCCESS) {
    std::cout << "Failed to add entry - " << doca_error_get_name(res) << '\n';
    return -1;
  }
  res = doca_flow_entries_process(port, 0, DEFAULT_TIMEOUT_US, num_of_entries);
  if (res != DOCA_SUCCESS) {
    std::cout << "Failed to process entries - " << doca_error_get_name(res) << '\n';
    return -1;
  }
  std::cout << "nb_processed: " << status->nb_processed << '\n';
  std::cout << "failure: " << status->failure << '\n';
  // if (status->nb_processed != num_of_entries || status->failure) {
  //   DOCA_LOG_ERR("Failed to add entry - %s", doca_error_get_name(res));
  //   return -1;
  // }
  return 0;
}

/*
 * Run a DOCA Flow program that matches packets in the TCP stream of interest
 * and places them in a RSS queue for processing by the network engine.
 */
int FlowRSS(const NetworkEngineConfig &ne_config) {
  struct flow_resources resource = {};
  uint32_t nr_shared_resources[DOCA_FLOW_SHARED_RESOURCE_RSS] = {};
  struct doca_flow_port *ports[NB_PORTS];
  uint32_t actions_mem_size[NB_PORTS];
  struct doca_flow_pipe *pipe;
  struct entries_status status;
  memset(&status, 0, sizeof(status));
	int num_of_entries = 1;
  int result;

  {
    uint32_t memsize = ACTIONS_MEM_SIZE(num_of_entries);
    std::fill_n(actions_mem_size, NB_PORTS, memsize);
  }

  resource.mode = DOCA_FLOW_RESOURCE_MODE_PORT;
  resource.nr_rss = num_of_entries;
  if (init_doca_flow(NB_QUEUES, "vnf", &resource, nr_shared_resources) < 0) {
    DOCA_LOG_ERR("Failed to init DOCA Flow");
    return -1;
  }

  auto res = init_doca_flow_vnf_ports(NB_PORTS, ports, actions_mem_size, &resource);
  if (res != DOCA_SUCCESS) {
    // DOCA_LOG_ERR("Failed to init DOCA ports, {}", doca_error_get_name(res));
    dpf::LOG_ERRORF("Failed to init DOCA ports, {}", doca_error_get_name(res));
    doca_flow_destroy();
    return -1;
  }

  result = CreateRSSPipe(ports[PORT_ID], &pipe);
  if (result < 0) {
    DOCA_LOG_ERR("Failed to create pipe");
    stop_doca_flow_ports(NB_PORTS, ports);
    doca_flow_destroy();
    return -1;
  }


  result = AddRSSPipeEntry(pipe, ports[PORT_ID], &status, ne_config);
  if (result < 0) {
   DOCA_LOG_ERR("Failed to add entry");
   stop_doca_flow_ports(NB_PORTS, ports);
   doca_flow_destroy();
   return -1;
  }

  RunNetworkEngine(PORT_ID, QUEUE_ID);

  stop_doca_flow_ports(NB_PORTS, ports);
  doca_flow_destroy();
  return 0;
}

} // namespace

namespace dpf {

int RunNetworkEngineMain(const JSON &config) {
  const NetworkEngineConfig ne_config{config["network_engine"]};

  int ret;
  int exit_status = EXIT_SUCCESS;

  ret = FlowRSS(ne_config);
  if (ret < 0) {
    DOCA_LOG_ERR("FlowRSS encountered errors");
    exit_status = EXIT_FAILURE;
  }
  std::cout << "Post flow RSS\n";

  return exit_status;
}

} // namespace dpf
