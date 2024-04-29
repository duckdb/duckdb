include_directories(../third_party/catch)
include_directories(../third_party/tpce-tool/include)
include_directories(../third_party/sqlite/include)
include_directories(include)

# Allows loading tests from registered extensions
include_directories("${PROJECT_BINARY_DIR}/codegen/include/")
add_definitions(-DGENERATED_EXTENSION_HEADERS=1)

add_subdirectory(api)
add_subdirectory(appender)
add_subdirectory(arrow)
add_subdirectory(catalog)
add_subdirectory(common)
add_subdirectory(extension)
add_subdirectory(helpers)
add_subdirectory(memoryleak)
add_subdirectory(parallel_csv)
add_subdirectory(secrets)
add_subdirectory(serialize)
add_subdirectory(sql)
add_subdirectory(sqlite)
add_subdirectory(ossfuzz)
add_subdirectory(mbedtls)

if(NOT WIN32 AND NOT SUN)
  if(${BUILD_TPCE})
    add_subdirectory(tpce)
  endif()
  add_subdirectory(persistence)
endif()

add_definitions(-DDUCKDB_ROOT_DIRECTORY="${PROJECT_SOURCE_DIR}")

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
