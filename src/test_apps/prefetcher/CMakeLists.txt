add_executable(prefetcher_test main.cpp)

target_link_libraries(prefetcher_test PRIVATE common buffer prefetcher pthread)