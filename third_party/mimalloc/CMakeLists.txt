add_library(duckdb_mimalloc STATIC stats.cpp random.cpp os.cpp bitmap.cpp arena.cpp segment-cache.cpp segment.cpp page.cpp alloc.cpp alloc-aligned.cpp alloc-posix.cpp heap.cpp options.cpp init.cpp)

target_include_directories(duckdb_mimalloc PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>)
set_target_properties(duckdb_mimalloc PROPERTIES EXPORT_NAME duckdb_mimalloc)

install(TARGETS duckdb_mimalloc
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

disable_target_warnings(duckdb_mimalloc)
