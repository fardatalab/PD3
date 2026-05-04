add_subdirectory(common)
add_subdirectory(FASTER)
add_subdirectory(user_defined)

set(COMMON_COMPILE_OPTIONS
    -Wall
    -Wextra
    -Wpedantic
    -Werror
    PARENT_SCOPE
)
