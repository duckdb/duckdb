add_subdirectory(date)
add_subdirectory(math)
add_subdirectory(sequence)
add_subdirectory(string)
add_subdirectory(trigonometrics)

add_library_unity(duckdb_scalar_function
                  OBJECT
                  date_functions.cpp
                  string_functions.cpp
                  math_functions.cpp
                  sequence_functions.cpp
                  trigonometrics_functions.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES}
                     $<TARGET_OBJECTS:duckdb_scalar_function> PARENT_SCOPE)
