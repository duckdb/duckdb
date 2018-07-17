
add_subdirectory(catalog)
add_subdirectory(common)
add_subdirectory(main)
add_subdirectory(parser)

add_library(duckdb STATIC ${ALL_OBJECT_FILES})
target_link_libraries(duckdb pg_query)
