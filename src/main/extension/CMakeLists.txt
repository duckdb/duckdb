include_directories(../../../third_party/httplib/)

# To link extensions into DuckDB and auto-load them on startup, we generated an
# include file and a loader function based on the `DUCKDB_EXTENSION_NAMES`
# parameter.

# generated_extension_headers.hpp
configure_file(generated_extension_headers.hpp.in
               "${PROJECT_BINARY_DIR}/include/generated_extension_headers.hpp")
if(NOT ${DISABLE_BUILTIN_EXTENSIONS})
  foreach(EXT_NAME IN LISTS DUCKDB_EXTENSION_NAMES)
    string(TOUPPER ${EXT_NAME} EXT_NAME_UPPERCASE)
    if(${DUCKDB_EXTENSION_${EXT_NAME_UPPERCASE}_SHOULD_LINK})
      set(DUCKDB_EXTENSION_HEADER "${EXT_NAME}_extension.hpp")
      file(APPEND
           "${PROJECT_BINARY_DIR}/include/generated_extension_headers.hpp"
           "#include \"${DUCKDB_EXTENSION_HEADER}\"\n")
    endif()
  endforeach()
endif()

# generated_extension_loader.hpp
set(EXT_LOADER_NAME_LIST "")
set(EXT_LOADER_BODY "")
if(NOT ${DISABLE_BUILTIN_EXTENSIONS})
  foreach(EXT_NAME IN LISTS DUCKDB_EXTENSION_NAMES)
    string(TOUPPER ${EXT_NAME} EXT_NAME_UPPERCASE)
    if(${DUCKDB_EXTENSION_${EXT_NAME_UPPERCASE}_SHOULD_LINK})
      # Assumes lowercase input!
      set(EXTENSION_CLASS ${EXT_NAME}Extension)
      string(SUBSTRING ${EXT_NAME} 0 1 FIRST_LETTER)
      string(TOUPPER ${FIRST_LETTER} FIRST_LETTER)
      string(REGEX REPLACE "^.(.*)" "${FIRST_LETTER}\\1" EXTENSION_CLASS
                           "${EXT_NAME}")

      set(EXT_LOADER_NAME_LIST "${EXT_LOADER_NAME_LIST},\"${EXT_NAME}\"")
      set(EXT_LOADER_BODY
          "${EXT_LOADER_BODY}\
    if (extension==\"${EXT_NAME}\") {
        db.LoadExtension<${EXTENSION_CLASS}Extension>();
        return true;
    }
")
    endif()
  endforeach()
endif()

# trim comma
if(NOT ${EXT_LOADER_NAME_LIST} STREQUAL "")
  string(SUBSTRING ${EXT_LOADER_NAME_LIST} 1 -1 EXT_NAME_VECTOR_INITIALIZER)
endif()

configure_file(generated_extension_loader.hpp.in
               "${PROJECT_BINARY_DIR}/include/generated_extension_loader.hpp")

if(${DISABLE_EXTENSION_LOAD})
  add_definitions(-DDUCKDB_DISABLE_EXTENSION_LOAD)
endif()
add_definitions(-DGENERATED_EXTENSION_HEADERS=1)

# include generated includes
include_directories("${PROJECT_BINARY_DIR}/include/")

if(${EXIT_TIME_DESTRUCTORS_WARNING})
  set(CMAKE_CXX_FLAGS_DEBUG
      "${CMAKE_CXX_FLAGS_DEBUG} -Wno-exit-time-destructors")
endif()
add_library_unity(
  duckdb_main_extension
  OBJECT
  extension_alias.cpp
  extension_helper.cpp
  extension_install.cpp
  extension_load.cpp
  extension_util.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_main_extension>
    PARENT_SCOPE)

add_extension_definitions(duckdb_main_extension)
