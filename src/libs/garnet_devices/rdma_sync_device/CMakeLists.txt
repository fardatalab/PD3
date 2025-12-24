add_library(rdma_sync_device SHARED sync_client.cpp rdma_sync_device.cpp sync_device_wrapper.cpp)

find_library(IBVERBS_LIBRARY ibverbs HINTS /usr/local/lib)

target_link_libraries(rdma_sync_device PUBLIC
  ${IBVERBS_LIBRARY}
  rdmacm
  fmt::fmt
  FASTER
)