# nanoarrow.c must be compiled as C (not C++) to avoid name mangling issues.
# Using add_library directly instead of add_library_unity to skip the unity
# build which would wrap it in a .cpp translation unit.
add_library(duckdb_nanoarrow_c OBJECT
            ${PROJECT_SOURCE_DIR}/third_party/nanoarrow/nanoarrow.c)
target_include_directories(duckdb_nanoarrow_c
                           PRIVATE ${PROJECT_SOURCE_DIR}/third_party/nanoarrow)
disable_target_warnings(duckdb_nanoarrow_c)
if(NOT WIN32)
  target_compile_definitions(duckdb_nanoarrow_c PRIVATE "NANOARROW_DLL=")
  target_compile_options(duckdb_nanoarrow_c PRIVATE -fvisibility=hidden)
endif()

add_library_unity(duckdb_adbc_nanoarrow OBJECT single_batch_array_stream.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_nanoarrow_c>
    $<TARGET_OBJECTS:duckdb_adbc_nanoarrow>
    PARENT_SCOPE)
