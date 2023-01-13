include_directories(../../../third_party/httplib/)

# To link out-of-tree extensions into DuckDB and auto-load them on startup, we
# do some code generation here - extension_oote_headers.hpp    contains the
# includes of the OOTE header - extension_oote_loader.hpp     contains the call
# to ExtensionClass::Load() for loading on startup

# extension_oote_headers.hpp
configure_file(extension_oote_headers.hpp.in
               "${PROJECT_BINARY_DIR}/include/extension_oote_headers.hpp")
if(NOT ${DISABLE_BUILTIN_EXTENSIONS})
  foreach(OOTE_NAME IN LISTS DUCKDB_OOTE_EXTENSION_NAMES)
    set(DUCKDB_EXTENSION_HEADER "${OOTE_NAME}_extension.hpp")
    file(APPEND "${PROJECT_BINARY_DIR}/include/extension_oote_headers.hpp"
         "#include \"${DUCKDB_EXTENSION_HEADER}\"\n")
  endforeach()
endif()

# extension_oote_loader.hpp
set(OOTE_LOADER_NAME_LIST "")
set(OOTE_LOADER_BODY "")
if(NOT ${DISABLE_BUILTIN_EXTENSIONS})
  foreach(OOTE_NAME IN LISTS DUCKDB_OOTE_EXTENSION_NAMES)
    # Assumes lowercase input!
    set(EXTENSION_CLASS ${OOTE_NAME}Extension)
    string(SUBSTRING ${OOTE_NAME} 0 1 FIRST_LETTER)
    string(TOUPPER ${FIRST_LETTER} FIRST_LETTER)
    string(REGEX REPLACE "^.(.*)" "${FIRST_LETTER}\\1" EXTENSION_CLASS
                         "${OOTE_NAME}")

    set(OOTE_LOADER_NAME_LIST "${OOTE_LOADER_NAME_LIST},\"${OOTE_NAME}\"")
    set(OOTE_LOADER_BODY
        "${OOTE_LOADER_BODY}\
      if (extension==\"${OOTE_NAME}\") {
        db.LoadExtension<${EXTENSION_CLASS}Extension>();
        return true;
      }
      ")
  endforeach()
endif()

# trim comma
if(NOT ${OOTE_LOADER_NAME_LIST} STREQUAL "")
  string(SUBSTRING ${OOTE_LOADER_NAME_LIST} 1 -1 OOTE_NAME_VECTOR_INITIALIZER)
endif()

configure_file(extension_oote_loader.hpp.in
               "${PROJECT_BINARY_DIR}/include/extension_oote_loader.hpp")

add_definitions(-DOOTE_HEADERS_AVAILABLE=1)

# include generated includes
include_directories("${PROJECT_BINARY_DIR}/include/")

if(${EXIT_TIME_DESTRUCTORS_WARNING})
  set(CMAKE_CXX_FLAGS_DEBUG
      "${CMAKE_CXX_FLAGS_DEBUG} -Wno-exit-time-destructors")
endif()
add_library_unity(duckdb_main_extension OBJECT extension_alias.cpp
                  extension_helper.cpp extension_install.cpp extension_load.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_main_extension>
    PARENT_SCOPE)

add_extension_definitions(duckdb_main_extension)
