add_library(
  duckdb_icu_stubdata
  OBJECT
stubdata.cpp
)
set(ICU_LIBRARY_FILES
    ${ICU_LIBRARY_FILES} $<TARGET_OBJECTS:duckdb_icu_stubdata>
    PARENT_SCOPE)
