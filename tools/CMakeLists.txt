if(NOT SUN AND BUILD_SHELL)
  add_subdirectory(shell)
endif()

add_executable(plan_serializer utils/plan_serializer.cpp)
target_link_libraries(plan_serializer duckdb_static
                      dummy_static_extension_loader ${DUCKDB_EXTRA_LINK_FLAGS})
