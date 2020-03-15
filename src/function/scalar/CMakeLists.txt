add_subdirectory(date)
add_subdirectory(math)
add_subdirectory(operators)
add_subdirectory(sequence)
add_subdirectory(string)
add_subdirectory(nested)

add_library_unity(duckdb_func_scalar
                  OBJECT
                  date_functions.cpp
                  string_functions.cpp
                  nested_functions.cpp
                  math_functions.cpp
                  operators.cpp
                  sequence_functions.cpp
                  trigonometrics_functions.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_func_scalar>
    PARENT_SCOPE)
