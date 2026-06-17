include_directories(${PROJECT_SOURCE_DIR}/third_party/nanoarrow)
add_library_unity(duckdb_adbc OBJECT adbc.cpp)
add_subdirectory(nanoarrow)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_adbc>
    PARENT_SCOPE)
