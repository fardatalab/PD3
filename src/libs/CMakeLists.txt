add_subdirectory(garnet_devices)
add_subdirectory(common)
add_subdirectory(FASTER)
add_subdirectory(rdma_backend)
add_subdirectory(benchmarking)
add_subdirectory(redy)
add_subdirectory(user_defined)

if (BUILD_DPU)
    add_subdirectory(network_engine)
endif()

set(COMMON_COMPILE_OPTIONS
    -Wall
    -Wextra
    -Wpedantic
    -Werror
    PARENT_SCOPE
)
