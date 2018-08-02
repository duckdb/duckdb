
add_subdirectory(catalog)
add_subdirectory(common)
add_subdirectory(execution)
add_subdirectory(main)
add_subdirectory(parser)
add_subdirectory(planner)
add_subdirectory(storage)

add_library(duckdb SHARED ${ALL_OBJECT_FILES})
target_link_libraries(duckdb pg_query Threads::Threads)
