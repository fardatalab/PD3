# Build the native_device library.

add_library(FASTER SHARED lss_allocator.cpp thread_manual.cpp)

target_include_directories(FASTER PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

if (UNIX)
  target_link_libraries(FASTER PRIVATE stdc++fs aio)
endif()
