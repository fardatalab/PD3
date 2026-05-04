add_subdirectory(buffer)
add_subdirectory(loading_zone)
add_subdirectory(rdma_backend)
add_subdirectory(transfer_engine)
add_subdirectory(prefetcher)

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