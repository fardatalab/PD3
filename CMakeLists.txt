cmake_minimum_required(VERSION 3.16 FATAL_ERROR)
project(dpu-prefetcher VERSION 0.1.0)

# Enable compilation database
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Set output directories for all builds (Debug, Release, etc.)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)  # For executables
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)  # For shared libraries (.so, .dll)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)  # For static libraries (.a, .lib)

# Ensure the paths are set for multi-configuration generators (like Visual Studio)
foreach(OUTPUTCONFIG ${CMAKE_CONFIGURATION_TYPES})
    string(TOUPPER ${OUTPUTCONFIG} OUTPUTCONFIG)
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_${OUTPUTCONFIG} ${CMAKE_BINARY_DIR}/bin)
    set(CMAKE_LIBRARY_OUTPUT_DIRECTORY_${OUTPUTCONFIG} ${CMAKE_BINARY_DIR}/lib)
    set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY_${OUTPUTCONFIG} ${CMAKE_BINARY_DIR}/lib)
endforeach()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

option(BUILD_DPU "Build binaries for DPU" OFF) 

# Add O2 optimization globally
add_compile_options(-O2)

add_library(ibverbs SHARED IMPORTED)

set(PROJECT_ROOT ${CMAKE_CURRENT_SOURCE_DIR})
set(SRC_ROOT ${PROJECT_ROOT}/src)
set(LIBS_ROOT ${SRC_ROOT}/libs)

# get fnt
include(FetchContent)

FetchContent_Declare(
  fmt
  GIT_REPOSITORY https://github.com/fmtlib/fmt
  GIT_TAG        e69e5f977d458f2650bb346dadf2ad30c5320281) # 10.2.1

  FetchContent_Declare(
  Catch2
  GIT_REPOSITORY https://github.com/catchorg/Catch2.git
  GIT_TAG        v3.4.0 # or a later release
)

FetchContent_MakeAvailable(fmt)
FetchContent_MakeAvailable(Catch2)
include_directories("${PROJECT_SOURCE_DIR}/src")
include_directories("${PROJECT_SOURCE_DIR}/src/libs")
include_directories("${PROJECT_SOURCE_DIR}/src/config")
include_directories("${PROJECT_SOURCE_DIR}/external/SPSCQueue/include")
include_directories("${PROJECT_SOURCE_DIR}/external/json/include")

add_subdirectory(src)
add_subdirectory(tests)
