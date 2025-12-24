
add_library(dpf_system SHARED 
    shm_allocator.cpp
    shm_attacher.cpp
    logger.cpp
)

target_link_libraries(dpf_system PRIVATE fmt)

