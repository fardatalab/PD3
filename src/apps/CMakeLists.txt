add_subdirectory(hello_world)
add_subdirectory(memory_backend)
add_subdirectory(hashmap)

if (BUILD_DPU)
    add_subdirectory(prefetcher_ne)
endif()

set(BINARY_OUTPUT_PATH ${CMAKE_BINARY_DIR}/bin)