add_executable(hashmap_server server.cpp net_hashmap.cpp)
add_executable(hashmap_client client.cpp req_gen.cpp)
add_executable(latency_client latency_client.cpp req_gen.cpp)

find_package(TBB REQUIRED)

target_include_directories(hashmap_server PUBLIC
    ${COMMON_INCLUDE_DIRS}
    "/opt/mellanox/doca/include"
)

target_link_libraries(hashmap_server TBB::tbb pthread rdma_sync_device redy_device agent)
target_link_libraries(hashmap_client TBB::tbb pthread benchmark)
target_link_libraries(latency_client TBB::tbb pthread benchmark)