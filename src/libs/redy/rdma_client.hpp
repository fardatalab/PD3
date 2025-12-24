/**
 * The RDMA client is used to send batched requests to the remote memory server / servers. It exposes the following interface:
 * - ConnectServer : connects to a remote memory server
 * - BatchRequest: adds a request to the current working batch 
 * - ExecuteBatch: sends the current batch to the remote memory server
 * - ProcessCompletion: processes the response from the remote memory server
 * Credit: inspiration from Redy
 */

#pragma once

#include "types/rdma_conn.hpp"

#include "json.h"

#include <vector>
#include <unordered_map>
#include <string>
#include <cstdint>

namespace dpf {

class RdmaClient {

  struct LocalBuffer {
    ~LocalBuffer();
  
    struct ibv_mr *buffer_mr;
    void *buffer;
  };

public:
  
  static constexpr size_t RDMA_BUFFER_SIZE = 8192 * 4;

  RdmaClient() = default;
  ~RdmaClient();
  
  /// @brief This function parses the config and stores parameters: server address, port, queue depth and more
  /// @param config a JSON object containing the rdma client configuration
  void Configure(const JSON& config);
  
  /// @brief This function initializes the RDMA client, sets up the network and connects to the remote memory server
  /// @return true if the initialization was successful, false otherwise
  bool Init(struct ibv_pd** pd = nullptr, bool set_pd = false);  
  
  /// @brief This function adds a request to the current working batch upto max capacity
  /// @param slot_id the slot id to use for this request
  /// @param is_read whether the request is a remote read or a remote write
  /// @param segment_id the physical memory segment to read from / write to
  /// @param memory_address the remote memory address to read from / write to
  /// @param bytes the number of bytes to read / write
  bool BatchRequest(uint32_t slot_id, bool is_read, uint32_t segment_id, uint64_t memory_address, uint64_t app_address, uint64_t bytes);

  /// @brief This function checks if the current batch can accomodate a request
  /// @param slot_id the slot id to use for this request
  /// @param bytes the number of bytes to read / write
  /// @return true if the batch can accomodate the request, false otherwise
  bool BatchCanAccomodateRequest(uint32_t slot_id, uint64_t bytes) const;
  
  /// @brief This function checks if the current batch is full
  /// @param slot_id the slot id to use for this request
  /// @return true if the batch is full, false otherwise
  bool BatchFull(uint32_t slot_id) const;
  
  /// @brief This function sends a one sided RDMA request to the remote memory server
  /// @param is_read whether the request is a remote read or a remote write
  /// @param segment_id the physical memory segment to read from / write to
  /// @param memory_address the remote memory address to read from / write to
  /// @param bytes the number of bytes to read / write
  /// @param data the data to write to the remote memory address
  void OneSidedRequest(bool is_read, uint32_t segment_id, uint64_t memory_address, uint64_t bytes, char* data);

  /// @brief This function sends the current batch to the remote memory server
  /// @param slot_id the slot id to use for this request
  void ExecuteBatch(uint32_t slot_id);
  
  /// @brief This function processes the response from the remote memory server
  /// @param slot_id the slot id to use for this request
  void ProcessCompletion(uint32_t slot_id);
  
  /// @brief This function polls the given slot id to see if a response is available
  /// @param slot_id slot id to use for this request
  /// @return true if a response is available, false otherwise
  bool PollMessageAt(uint32_t slot_id);
  
  /// @brief This function returns the response at the given slot id
  /// @param slot_id slot id to use for this request
  /// @param bytes_read the number of bytes read, set by the function
  /// @return the response at the given slot id
  char* GetResponseAt(uint32_t slot_id, uint32_t* bytes_read);
  
  /// @brief This function removes the remote response at the given slot id
  /// @param slot_id slot id to remove the response for
  void RemoveResponseAt(uint32_t slot_id);


private:
  
  // RDMA helpers
  bool PostSend(RdmaConnection* conn, 
                uint64_t request_id, 
                struct ibv_sge* sg_list, 
                size_t num_sge, 
                uint32_t flags);
  
  bool PostRecv(RdmaConnection* conn, 
                uint64_t request_id, 
                struct ibv_sge* sg_list, 
                size_t num_sge);
  
  bool PostWrite(RdmaConnection* conn,
                 uint64_t request_id,
                 struct ibv_sge* sg_list,
                 size_t num_sge,
                 uint64_t remote_addr,
                 uint32_t remote_rkey,
                 uint32_t flags);
  
  /// @brief This function waits for the completion of the given request
  /// @param conn the connection to wait for
  /// @return true if the completion was successful, false otherwise
  bool WaitForCompletion(RdmaConnection* conn);



private:
  
  // the max number of requests that can be in flight at any time
  uint64_t queue_depth_ = 0;
  uint64_t batch_size_ = 0;

  // NIC configuration
  std::string server_addr_ = "";
  std::string server_port_ = "";
  uint64_t max_send_wr_ = 0;
  uint64_t max_sge_ = 0;

  // server connection for control plane
  // TODO if needed

  // connections
  std::vector<RdmaConnection*> connections_;

  // local buffer for one sided requests
  LocalBuffer one_sided_buffer_;

  // any physical memory regions
  // TODO

};

} // namespace dpf