if(NOT SUN AND BUILD_SHELL)
  add_subdirectory(shell)
endif()

if(BUILD_SHELL)
  add_executable(plan_serializer utils/plan_serializer.cpp)
  if(TARGET duckdb AND NOT WIN32)
    target_link_libraries(plan_serializer duckdb ${DUCKDB_EXTRA_LINK_FLAGS})
  else()
    target_link_libraries(plan_serializer duckdb_static
                          ${DUCKDB_EXTRA_LINK_FLAGS})
    link_extension_libraries(plan_serializer "")
  endif()
endif()

add_subdirectory(cpp)
