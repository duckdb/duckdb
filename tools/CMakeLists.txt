if(NOT SUN AND BUILD_SHELL)
  add_subdirectory(shell)
endif()

add_executable(plan_serializer utils/plan_serializer.cpp)
target_link_libraries(plan_serializer duckdb_static ${DUCKDB_EXTRA_LINK_FLAGS})
link_extension_libraries(plan_serializer "")
