
add_library(network_engine SHARED
  network_engine_main.cpp
  network_engine.cpp
  ne_user_defined_garnet.cpp
  hashmap.cpp
  garnet.cpp
  "/opt/mellanox/doca/samples/doca_flow/flow_common.c"
)

find_package(PkgConfig REQUIRED)
pkg_check_modules(DOCA REQUIRED doca-flow)
pkg_check_modules(DPDK REQUIRED libdpdk)

target_include_directories(network_engine PUBLIC
  ${COMMON_INCLUDE_DIRS}
  ${DOCA_INCLUDE_DIRS}
  ${DPDK_INCLUDE_DIRS}
  "/opt/mellanox/doca/samples/doca_flow"
  "/opt/mellanox/doca/samples"
)

message(STATUS "DOCA DIRS: ${DOCA_LIBRARY_DIRS}")
message(STATUS "DPDK DIRS: ${DPDK_LIBRARY_DIRS}")

target_link_libraries(network_engine PUBLIC
	${DOCA_LINK_LIBRARIES}
  ${DPDK_LINK_LIBRARIES}
  prefetcher
)

target_compile_definitions(network_engine PUBLIC
  RTE_FORCE_INTRINSICS
  DOCA_ALLOW_EXPERIMENTAL_API
)
