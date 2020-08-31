add_subdirectory(index)
add_subdirectory(parallelism)
add_subdirectory(storage)

if(NOT WIN32 AND NOT SUN)
  add_subdirectory(tpcds)
endif()

set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} PARENT_SCOPE)
