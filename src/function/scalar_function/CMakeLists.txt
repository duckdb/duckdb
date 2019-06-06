add_library_unity(duckdb_scalar_function OBJECT math.cpp round.cpp concat.cpp date_part.cpp length.cpp nextval.cpp regexp.cpp substring.cpp year.cpp caseconvert.cpp trigonometrics.cpp age.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_scalar_function> PARENT_SCOPE)
