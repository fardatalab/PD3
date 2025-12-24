add_library(redy SHARED rdma_client.cpp)

find_library(IBVERBS_LIBRARY ibverbs HINTS /usr/local/lib)
message(STATUS "IBVERBS library: ${IBVERBS_LIBRARY}")

find_library(TBB_LIBRARY tbb HINTS /usr/lib)
message(STATUS "TBB library: ${TBB_LIBRARY}")

target_link_libraries(redy PUBLIC
  ${IBVERBS_LIBRARY}
  ${TBB_LIBRARY}
  rdmacm
  fmt::fmt
)
