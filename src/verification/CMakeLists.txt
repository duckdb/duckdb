add_library(
  duckdb_verification OBJECT
  prepared_statement_verifier.cpp explain_statement_verifier.cpp
  statement_verifier.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_verification>
    PARENT_SCOPE)
