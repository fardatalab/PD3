
add_executable(memory_backend main.cpp)
add_executable(batched_memory_backend batched.cpp)
add_executable(pd3_memory_backend pd3.cpp)

target_link_libraries(memory_backend
    rdma_backend
)

target_link_libraries(batched_memory_backend
    rdma_backend
)

target_link_libraries(pd3_memory_backend
    pd3_rdma_backend
)