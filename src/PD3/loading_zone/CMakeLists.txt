add_library(agent SHARED agent.cpp agent_wrapper.cpp)

target_link_libraries(agent PRIVATE common fmt::fmt buffer pthread)