add_library(rdma_backend SHARED
    remote_server.cpp
    batched_server.cpp
    mem_server.cpp
)

target_include_directories(rdma_backend PUBLIC
    ${COMMON_INCLUDE_DIRS}
)

find_library(IBVERBS_LIBRARY ibverbs HINTS /usr/local/lib)

target_link_libraries(rdma_backend
  ${IBVERBS_LIBRARY}
  rdmacm
  fmt::fmt
)