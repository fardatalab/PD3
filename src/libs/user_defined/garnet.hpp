#pragma once

// Declarations of all the user-defined stuff the user
// of DPU prefetcher needs to define

#include <cstdint>
#include <cstddef>


int ParseRemoteWrite(const char* data, size_t len, uint64_t* keys);
