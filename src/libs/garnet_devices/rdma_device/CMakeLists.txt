add_library(rdma_device SHARED
    rdma_device.cpp
    device_wrapper.cpp
    rdma_context.cpp
    null_device.cpp
)

target_include_directories(rdma_device PUBLIC
    ${COMMON_INCLUDE_DIRS}
)

find_package(TBB REQUIRED)

target_link_libraries(rdma_device PUBLIC FASTER TBB::tbb offload_engine)