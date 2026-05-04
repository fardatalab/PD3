add_executable(tests 
garnet_parsing_test.cpp
lru_test.cpp 
baseline_test.cpp 
leap_test.cpp 
transfer_engine_test.cpp
transfer_engine_client_test.cpp
refresher_test.cpp
prefetcher_test.cpp
)

target_link_libraries(tests PRIVATE Catch2::Catch2WithMain common buffer benchmark pthread offload_engine prefetcher)