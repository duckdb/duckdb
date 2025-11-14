include_directories(../third_party/catch)
include_directories(../third_party/tpce-tool/include)
include_directories(../third_party/sqlite/include)
include_directories(include)

# Allows loading tests from registered extensions
include_directories("${PROJECT_BINARY_DIR}/codegen/include/")
add_definitions(-DGENERATED_EXTENSION_HEADERS=1)
add_definitions(-DDUCKDB_MAJOR_VERSION=${DUCKDB_MAJOR_VERSION})
add_definitions(-DDUCKDB_MINOR_VERSION=${DUCKDB_MINOR_VERSION})
add_definitions(-DDUCKDB_PATCH_VERSION=${DUCKDB_PATCH_VERSION})

if(${ENABLE_UNITTEST_CPP_TESTS})
  add_subdirectory(api)
  add_subdirectory(appender)
  add_subdirectory(arrow)
  add_subdirectory(catalog)
  add_subdirectory(common)
  add_subdirectory(logging)
  add_subdirectory(memoryleak)
  add_subdirectory(parallel_csv)
  add_subdirectory(secrets)
  add_subdirectory(optimizer)
  add_subdirectory(serialize)
  add_subdirectory(sql)
  add_subdirectory(ossfuzz)
  add_subdirectory(mbedtls)
endif()
add_subdirectory(extension)
add_subdirectory(helpers)
add_subdirectory(sqlite)

if(NOT WIN32 AND NOT SUN)
  if(${BUILD_TPCE})
    add_subdirectory(tpce)
  endif()
  if(${ENABLE_UNITTEST_CPP_TESTS})
    add_subdirectory(persistence)
  endif()
endif()

set(UNITTEST_ROOT_DIRECTORY
    ""
    CACHE
      STRING
      "The unittest root directory (defaults to the project source directory)")
if(DEFINED UNITTEST_ROOT_DIRECTORY AND NOT UNITTEST_ROOT_DIRECTORY STREQUAL "")
  add_definitions(-DDUCKDB_ROOT_DIRECTORY="${UNITTEST_ROOT_DIRECTORY}")
else()
  add_definitions(-DDUCKDB_ROOT_DIRECTORY="${PROJECT_SOURCE_DIR}")
endif()

add_executable(unittest unittest.cpp ${ALL_OBJECT_FILES})

if(NOT WIN32
   AND NOT SUN
   AND NOT ZOS)
  target_link_libraries(unittest duckdb test_helpers)
  if(${BUILD_TPCE})
    target_link_libraries(unittest tpce)
  endif()
else()
  target_link_libraries(unittest duckdb_static test_helpers)
endif()
