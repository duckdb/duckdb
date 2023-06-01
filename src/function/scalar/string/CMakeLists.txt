add_subdirectory(regexp)
add_library_unity(
  duckdb_func_string_main
  OBJECT
  caseconvert.cpp
  concat.cpp
  length.cpp
  like.cpp
  nfc_normalize.cpp
  regexp.cpp
  substring.cpp
  prefix.cpp
  strip_accents.cpp
  suffix.cpp
  contains.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_func_string_main>
    PARENT_SCOPE)
