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

// TODO: put other compile time config options here

} // namespace pd3