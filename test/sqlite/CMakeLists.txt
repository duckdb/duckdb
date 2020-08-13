if(${BUILD_ICU_EXTENSION})
  add_definitions(-DBUILD_ICU_EXTENSION=${BUILD_ICU_EXTENSION})
  include_directories(../../extension/icu/include)
endif()

if(${BUILD_PARQUET_EXTENSION})
  add_definitions(-DBUILD_PARQUET_EXTENSION=${BUILD_PARQUET_EXTENSION})
  include_directories(../../extension/parquet/include)
endif()

add_definitions(-DDUCKDB_ROOT_DIRECTORY="${PROJECT_SOURCE_DIR}")

add_library_unity(test_sqlite
                  OBJECT
                  md5.cpp
                  test_sqllogictest.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sqlite>
    PARENT_SCOPE)
