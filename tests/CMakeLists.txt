add_executable(tests 
garnet_parsing_test.cpp
lru_test.cpp 
baseline_test.cpp 
leap_test.cpp 
transfer_engine_test.cpp
te_client_test.cpp)

target_link_libraries(tests PRIVATE Catch2::Catch2WithMain common buffer benchmark pthread offload_engine)