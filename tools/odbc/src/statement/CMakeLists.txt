add_library_unity(odbc_statement OBJECT statement.cpp statement_functions.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:odbc_statement>
    PARENT_SCOPE)
