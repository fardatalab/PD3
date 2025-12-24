add_library(prefetcher SHARED
    prefetcher.cpp
)

target_include_directories(prefetcher PUBLIC
    ${COMMON_INCLUDE_DIRS}
)

target_compile_options(prefetcher PRIVATE
    -march=native
)

target_link_libraries(prefetcher PUBLIC
    offload_engine
)