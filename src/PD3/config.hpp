#pragma once

/**
 * File contains configuration definitions for the application
 */
#include "PD3/literals/memory_literals.hpp"


namespace dpf {

static constexpr size_t PD3_RING_BUFFER_SIZE = 256_MiB;
static constexpr size_t PD3_MAX_PRODUCER_ADVANCEMENT = 1_GiB;

static constexpr size_t PD3_REMOTE_SERVER_CHUNK_SIZE = 1_GiB;
static constexpr size_t PD3_REMOTE_SERVER_QUEUE_DEPTH = 128;


#define PD3_USE_MINMAX_FILTER 0
#define PD3_HV_ADD_ON_CLIENT_WRITE 1 // change this based on the application
#define PD3_HV_ADD_ON_REMOTE_READ 0 // change this based on the application, by corollary this also means add on prefetch

static constexpr size_t PD3_TE_MAX_READ_SIZE = 32; // bytes of max read size


} // namespace pd3