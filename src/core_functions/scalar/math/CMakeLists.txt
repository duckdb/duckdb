include_directories(../../../../third_party/fastmod/include)
include_directories(../../../../third_party/libdivide)

add_library_unity(duckdb_func_math OBJECT numeric.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_func_math>
    PARENT_SCOPE)
