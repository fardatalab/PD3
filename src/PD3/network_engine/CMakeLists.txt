
add_library(pd3_network_engine SHARED
  network_engine_main.cpp
  network_engine.cpp
  hashmap.cpp
  garnet.cpp
  "/opt/mellanox/doca/samples/doca_flow/flow_common.c"
)

find_package(PkgConfig REQUIRED)
pkg_check_modules(DOCA_FLOW REQUIRED doca-flow)
pkg_check_modules(DPDK REQUIRED libdpdk)

target_include_directories(pd3_network_engine PUBLIC
  ${COMMON_INCLUDE_DIRS}
  ${DOCA_INCLUDE_DIRS}
  ${DPDK_INCLUDE_DIRS}
  "/opt/mellanox/doca/samples/doca_flow"
  "/opt/mellanox/doca/samples"
)

target_link_libraries(pd3_network_engine PUBLIC
  ${DOCA_LIBRARY_DIRS}
  ${DPDK_LINK_LIBRARIES}
  prefetcher
)

target_compile_definitions(pd3_network_engine PUBLIC
  RTE_FORCE_INTRINSICS
  DOCA_ALLOW_EXPERIMENTAL_API
)
