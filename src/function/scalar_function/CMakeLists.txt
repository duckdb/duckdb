add_library_unity(duckdb_scalar_function OBJECT abs.cpp ceilfloor.cpp concat.cpp date_part.cpp length.cpp nextval.cpp regexp_matches.cpp regexp_replace.cpp substring.cpp year.cpp round.cpp caseconvert.cpp trigonometrics.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_scalar_function> PARENT_SCOPE)
