add_library(redy_device SHARED
    redy_device.cpp
    device_wrapper.cpp
    device_wrapper_single.cpp
    redy_device_single.cpp
)

target_include_directories(redy_device PUBLIC
    ${COMMON_INCLUDE_DIRS}
)

target_link_libraries(redy_device PUBLIC FASTER redy)