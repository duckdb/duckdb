add_library(
  duckdb_verification OBJECT
  external_statement_verifier.cpp
  fetch_row_verifier.cpp
  prepared_statement_verifier.cpp
  explain_statement_verifier.cpp
  statement_verifier.cpp
  no_operator_caching_verifier.cpp
  unoptimized_statement_verifier.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_verification>
    PARENT_SCOPE)
