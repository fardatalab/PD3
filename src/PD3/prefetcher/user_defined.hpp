#pragma once

// Declarations of all the user-defined stuff the user
// of DPU prefetcher needs to define

#include "types.hpp"

#include <unordered_map>
#include <vector>

extern const std::unordered_map<ClientRequestType, ClientRequestAction>
  request_action_map;

// A user-defined function that tells us how to parse a remote memory write
// request to generate the list of records that will be spilled over
std::vector<std::pair<RecordIdT, PrefetchRequest>>
ParseRemoteWrite(RemoteAddressT remote_addr,
                 const char* local_addr,
                 size_t len);
