#pragma once

namespace dpf {

/// @brief Allocate a shm segment (traditional POSIX shared memory)
class ShmAllocator;

/// @brief Attach to a shm segment (POSIX shared memory)
class ShmAttacher;

/// @brief Allocate a shm segment (device DPU / DOCA shared memory)
class DeviceShmAllocator;

/// @brief Attach to a shm segment (device DPU / DOCA shared memory)
class DeviceShmAttacher;

}

