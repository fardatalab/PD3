add_library(common SHARED
    doca_common.cpp
    app_utils.cpp
)

target_include_directories(common PUBLIC
    ${COMMON_INCLUDE_DIRS}
    "/opt/mellanox/doca/include"
)

find_package(PkgConfig REQUIRED)
pkg_check_modules(DOCA REQUIRED doca-common doca-dma)

message(STATUS "DOCA library dirs: ${DOCA_LDFLAGS}")

link_directories(${DOCA_LIBRARY_DIRS})

target_link_libraries(common PUBLIC
    ${DOCA_LDFLAGS}
)