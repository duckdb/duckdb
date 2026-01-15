add_library(
  duckdb_icu_stubdata
  OBJECT
stubdata.cpp
)
disable_target_warnings(duckdb_icu_stubdata)
set(ICU_LIBRARY_FILES
    ${ICU_LIBRARY_FILES} $<TARGET_OBJECTS:duckdb_icu_stubdata>
    PARENT_SCOPE)
