if(${DISABLE_THREADS})
  add_definitions(-DDUCKDB_NO_THREADS)
endif()

add_subdirectory(temp_files)
add_subdirectory(wal)

set(TEST_ENCRYPTION_OBJECTS)

if(NOT WIN32)
  set(TEST_ENCRYPTION_OBJECTS ${TEST_ENCRYPTION_OBJECTS})
endif()

if(DUCKDB_EXTENSION_TPCH_SHOULD_LINK)
  include_directories(../../extension/tpch/include)
  set(TEST_ENCRYPTION_OBJECTS
      ${TEST_ENCRYPTION_OBJECTS})
endif()

add_library(test_encryption OBJECT ${TEST_ENCRYPTION_OBJECTS} test_basic_encryption.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_encryption>
    PARENT_SCOPE)
