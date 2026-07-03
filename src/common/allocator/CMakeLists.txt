# Conditionally compile the allocator implementation based on whether jemalloc
# is enabled or not
if(JEMALLOC_ENABLED)
  add_library_unity(duckdb_common_allocator OBJECT allocator.cpp
                    allocator_jemalloc.cpp)
else()
  add_library_unity(duckdb_common_allocator OBJECT allocator.cpp
                    allocator_standard.cpp)
endif()

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_common_allocator>
    PARENT_SCOPE)
