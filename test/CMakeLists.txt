include_directories(../third_party/catch)
include_directories(../third_party/tpce-tool/include)
include_directories(../third_party/sqlite/include)
include_directories(include)

add_subdirectory(api)
add_subdirectory(appender)
add_subdirectory(common)
add_subdirectory(extension)
add_subdirectory(helpers)
add_subdirectory(sql)
add_subdirectory(sqlite)
add_subdirectory(ossfuzz)

if(BUILD_ARROW_ABI_TEST)
  add_subdirectory(arrow)
endif()

if(NOT WIN32 AND NOT SUN)
  if(${BUILD_TPCE})
    add_subdirectory(tpce)
  endif()
  add_subdirectory(persistence)
endif()

add_definitions(-DDUCKDB_ROOT_DIRECTORY="${PROJECT_SOURCE_DIR}")

add_executable(unittest unittest.cpp ${ALL_OBJECT_FILES})

if(BUILD_ARROW_ABI_TEST)
  add_dependencies(
    unittest
    RE2
    arrow_ep
    UTF8
    ZLIB
    SNAPPY
    duckdb)
  target_link_libraries(
    unittest
    ${CMAKE_BINARY_DIR}/third_party/arrow/install/lib/libparquet.a
    ${CMAKE_BINARY_DIR}/third_party/arrow/install/lib/libarrow_bundled_dependencies.a
    ${CMAKE_BINARY_DIR}/third_party/arrow/install/lib/libarrow.a
    ${CMAKE_BINARY_DIR}/third_party/zlib/install/lib/libz.a
    ${CMAKE_BINARY_DIR}/third_party/utf8/install/lib/libutf8proc.a
    ${CMAKE_BINARY_DIR}/third_party/re2/install/lib/libre2.a
    ${CMAKE_BINARY_DIR}/third_party/snappy/install/lib/libsnappy.a
    Threads::Threads)
endif()

if(NOT WIN32 AND NOT SUN)
  target_link_libraries(unittest duckdb dsdgen test_helpers)
  if(${BUILD_TPCE})
    target_link_libraries(unittest tpce)
  endif()
else()
  target_link_libraries(unittest duckdb_static test_helpers)
endif()

link_extension_libraries(unittest)

if(${BUILD_TPCH_EXTENSION})
  if(${BUILD_SQLSMITH})
    add_subdirectory(sqlsmith)
  endif()
endif()
