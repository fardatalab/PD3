add_library(offload_engine SHARED
    transfer_client.cpp
    transfer_engine.cpp
    transfer_engine_rdma.cpp
    # client.cpp
    # offload_server.cpp
    # offload_server_dma.cpp
    # offload_server_rdma.cpp
    # dma_client.cpp
    # rdma_client.cpp
)

# evaluate adding a new library here

target_include_directories(offload_engine PUBLIC
    ${COMMON_INCLUDE_DIRS}
    "/opt/mellanox/doca/include"
)

find_library(IBVERBS_LIBRARY ibverbs HINTS /usr/local/lib)
message(STATUS "IBVERBS library: ${IBVERBS_LIBRARY}")

target_link_libraries(offload_engine PUBLIC
    ${IBVERBS_LIBRARY}
    rdmacm
    common
    fmt::fmt
    buffer
    user_defined
)