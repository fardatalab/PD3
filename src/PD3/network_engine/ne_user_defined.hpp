#pragma once

#include <cstdint>

// The user defined function to parse a single TCP payload
void ProcessPacketMsg(char *msg, int msg_len, uint64_t& num_packets);
