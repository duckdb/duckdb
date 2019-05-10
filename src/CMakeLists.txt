add_definitions(-DDUCKDB)

IF(NOT MSVC)
	SET(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -Wextra -Wno-unused-parameter -Wnarrowing")
ENDIF()


add_subdirectory(optimizer)
add_subdirectory(planner)
add_subdirectory(parser)
add_subdirectory(function)
add_subdirectory(catalog)
add_subdirectory(common)
add_subdirectory(execution)
add_subdirectory(main)
add_subdirectory(storage)
add_subdirectory(transaction)

set(DUCKDB_LINK_LIBS pg_query hyperloglog re2 miniz Threads::Threads)

add_library(duckdb SHARED ${ALL_OBJECT_FILES})
target_link_libraries(duckdb ${DUCKDB_LINK_LIBS})

add_library(duckdb_static ${ALL_OBJECT_FILES})
target_link_libraries(duckdb_static ${DUCKDB_LINK_LIBS})
