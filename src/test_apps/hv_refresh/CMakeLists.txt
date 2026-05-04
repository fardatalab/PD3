add_executable(hv_refresh hv_refresh.cpp)

target_link_libraries(hv_refresh PRIVATE common buffer benchmark pthread offload_engine)