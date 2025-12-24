add_library(pd3_rdma_backend SHARED
    pd3_server.cpp
)

target_include_directories(pd3_rdma_backend PUBLIC
    ${COMMON_INCLUDE_DIRS}
)

find_library(IBVERBS_LIBRARY ibverbs HINTS /usr/local/lib)

target_link_libraries(pd3_rdma_backend
  ${IBVERBS_LIBRARY}
  rdmacm
  fmt::fmt
)