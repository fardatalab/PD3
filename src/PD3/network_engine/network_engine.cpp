#include "network_engine.hpp"

#include <doca_log.h>
#include <rte_ethdev.h>
#include <rte_ether.h>
#include <signal.h>
#include <iostream>

#include <chrono>

#include "hashmap.hpp"
#include "garnet.hpp"
#include "network_engine/prefetcher_interface.hpp"

DOCA_LOG_REGISTER(NETWORK_ENGINE);

#define PACKET_BURST 128	/* The number of packets in the rx queue */

static volatile bool run = true;

static void HandleSigint(int sig) {
  (void)sig; // unused
  run = false;
}

// Process a single Ethernet packet
void ProcessPacket(struct rte_mbuf *mbuf, dpf::user_defined::HashmapParser& hashmap_parser) {
  struct rte_ether_hdr *eth_hdr = rte_pktmbuf_mtod(mbuf, struct rte_ether_hdr *);
  if (rte_be_to_cpu_16(eth_hdr->ether_type) != RTE_ETHER_TYPE_IPV4) {
    // Not an IPv4 packet
    std::cout << "Received a non-IPv4 packet\n";
    return;
  }

  struct rte_ipv4_hdr *ip_hdr = (struct rte_ipv4_hdr *)(eth_hdr + 1);
  if (ip_hdr->next_proto_id != IPPROTO_TCP) {
    // Not a TCP packet
    std::cout << "Received a non-TCP packet\n";
    return;
  }
  // this is hardcoded for now, TODO: make it configurable
  if (ip_hdr->src_addr != 1677789706) {
    return;
  }
  int ip_pkt_sz = rte_be_to_cpu_16(ip_hdr->total_length);
  int ip_hdr_sz = rte_ipv4_hdr_len(ip_hdr);


  struct rte_tcp_hdr *tcp_hdr = (struct rte_tcp_hdr *)((char *)ip_hdr + ip_hdr_sz);
  int tcp_hdr_sz = (tcp_hdr->data_off >> 4) * 4;
  int tcp_payload_sz = ip_pkt_sz - ip_hdr_sz - tcp_hdr_sz;
  char* tcp_payload = (char *)tcp_hdr + tcp_hdr_sz;
  auto src_port = rte_be_to_cpu_16(tcp_hdr->src_port);
  auto dst_port = rte_be_to_cpu_16(tcp_hdr->dst_port);

  hashmap_parser.ProcessPacket(src_port, dst_port, tcp_payload, tcp_payload_sz);
}

void ProcessPacketGarnet(struct rte_mbuf *mbuf, dpf::user_defined::GarnetParser& garnet_parser) {
  struct rte_ether_hdr *eth_hdr = rte_pktmbuf_mtod(mbuf, struct rte_ether_hdr *);
  if (rte_be_to_cpu_16(eth_hdr->ether_type) != RTE_ETHER_TYPE_IPV4) {
    // Not an IPv4 packet
    std::cout << "Received a non-IPv4 packet\n";
    return;
  }

  struct rte_ipv4_hdr *ip_hdr = (struct rte_ipv4_hdr *)(eth_hdr + 1);
  if (ip_hdr->next_proto_id != IPPROTO_TCP) {
    // Not a TCP packet
    std::cout << "Received a non-TCP packet\n";
    return;
  }
  // this is hardcoded for now, TODO: make it configurable
  if (ip_hdr->src_addr != 1677789706) {
    return;
  }
  int ip_pkt_sz = rte_be_to_cpu_16(ip_hdr->total_length);
  int ip_hdr_sz = rte_ipv4_hdr_len(ip_hdr);


  struct rte_tcp_hdr *tcp_hdr = (struct rte_tcp_hdr *)((char *)ip_hdr + ip_hdr_sz);
  int tcp_hdr_sz = (tcp_hdr->data_off >> 4) * 4;
  int tcp_payload_sz = ip_pkt_sz - ip_hdr_sz - tcp_hdr_sz;
  char* tcp_payload = (char *)tcp_hdr + tcp_hdr_sz;
  auto src_port = rte_be_to_cpu_16(tcp_hdr->src_port);
  auto dst_port = rte_be_to_cpu_16(tcp_hdr->dst_port);

  garnet_parser.ProcessPacket(src_port, dst_port, tcp_payload, tcp_payload_sz);
}

// The main loop of the Network Engine.
void RunNetworkEngine(int ingress_port, int queue_index) {
  struct rte_mbuf *packets[PACKET_BURST];
  int nb_packets;
  int i;

  signal(SIGINT, HandleSigint);

  /* For the Hashmap application */
  // dpf::user_defined::HashmapParser hashmap_parser;
  // hashmap_parser.SetPrefetcher(GetPrefetcher());

  /* For the Garnet application */
  dpf::user_defined::GarnetParser garnet_parser;
  garnet_parser.SetPrefetcher(GetPrefetcher());

  // DOCA_LOG_INFO("Starting packet polling loop...");
  std::cout << "Starting packet polling loop...\n";
  uint64_t num_packets = 0;

  while (run) {
    nb_packets = rte_eth_rx_burst(ingress_port, queue_index, packets, PACKET_BURST);
    if (nb_packets > 0) {
      for (i = 0; i < nb_packets; i++) {
        // ProcessPacket(packets[i], hashmap_parser);
        ProcessPacketGarnet(packets[i], garnet_parser);
        rte_pktmbuf_free(packets[i]);
      }
    }
  }

  // DOCA_LOG_INFO("Stopped packet polling loop...");
  std::cout << "Stopped packet polling loop...\n";

  garnet_parser.PrintStatistics();
  // hashmap_parser.PrintStatistics();

  // Print port stats
  struct rte_eth_stats stats;
  if (rte_eth_stats_get(ingress_port, &stats) == 0) {
    std::cout << "=== Ethernet Stats (port " << ingress_port << ") ===\n";
	  std::cout << "ipackets: " << stats.ipackets << '\n';
	  std::cout << "opackets: " << stats.opackets << '\n';
	  std::cout << "ibytes: " << stats.ibytes << '\n';
    std::cout << "num user packets: " << num_packets << '\n';
	  std::cout << "imissed: " << stats.imissed << '\n';
	  std::cout << "ierrors: " << stats.ierrors << '\n';
    std::cout << "oerrors: " << stats.oerrors << '\n';
    std::cout << "rx_nombuf: " << stats.rx_nombuf << '\n';
  }
}
