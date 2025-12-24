
add_executable(prefetcher_ne_app
  main.cpp
  dpdk_utils.c
  /opt/mellanox/doca/applications/common/src/offload_rules.c
)

find_package(PkgConfig REQUIRED)
pkg_check_modules(DOCA REQUIRED doca-flow)
pkg_check_modules(DPDK REQUIRED libdpdk)

target_include_directories(prefetcher_ne_app PRIVATE
  ${DOCA_INCLUDE_DIRS}
  ${DPDK_INCLUDE_DIRS}
  /opt/mellanox/doca/applications/common/src
)

target_link_libraries(prefetcher_ne_app PRIVATE
  ${DOCA_LINK_LIBRARIES}
  ${DPDK_LINK_LIBRARIES}
  prefetcher
  network_engine
)

target_compile_definitions(prefetcher_ne_app PRIVATE
  RTE_FORCE_INTRINSICS
  DOCA_ALLOW_EXPERIMENTAL_API
)
