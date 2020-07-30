add_subdirectory(index)
add_subdirectory(parallelism)
add_subdirectory(prepared)
add_subdirectory(storage)
add_subdirectory(udf_function)

if(NOT WIN32 AND NOT SUN)
  add_subdirectory(tpch)
  add_subdirectory(tpcds)
endif()

set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} PARENT_SCOPE)
