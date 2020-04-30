cmake_minimum_required(VERSION 2.8.7)
# if(APPLE) # needs to be before project() SET(CMAKE_CXX_FLAGS -stdlib=libc++)
# SET(CMAKE_OSX_DEPLOYMENT_TARGET "10.7" CACHE STRING "Minimum OS X deployment
# version") endif()

project(DuckDB)

set(DUCKDB_MAJOR_VERSION 0)
set(DUCKDB_MINOR_VERSION 1)
set(DUCKDB_PATCH_VERSION 1)
set(DUCKDB_VERSION
    ${DUCKDB_MAJOR_VERSION}.${DUCKDB_MINOR_VERSION}.${DUCKDB_PATCH_VERSION})

find_package(Threads REQUIRED)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_VERBOSE_MAKEFILE OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_MACOSX_RPATH 1)

find_program(CCACHE_PROGRAM ccache)
if(CCACHE_PROGRAM)
  set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE "${CCACHE_PROGRAM}")
endif()

# Determine install paths
set(INSTALL_LIB_DIR lib CACHE PATH "Installation directory for libraries")
set(INSTALL_BIN_DIR bin CACHE PATH "Installation directory for executables")
set(INSTALL_INCLUDE_DIR
    include
    CACHE PATH "Installation directory for header files")
if(WIN32 AND NOT CYGWIN)
  set(DEF_INSTALL_CMAKE_DIR cmake)
else()
  set(DEF_INSTALL_CMAKE_DIR lib/cmake/DuckDB)
endif()
set(INSTALL_CMAKE_DIR
    ${DEF_INSTALL_CMAKE_DIR}
    CACHE PATH "Installation directory for CMake files")
set(DUCKDB_EXPORT_SET "DuckDBExports")

# Make relative install paths absolute
foreach(p
        LIB
        BIN
        INCLUDE
        CMAKE)
  set(var INSTALL_${p}_DIR)
  if(NOT IS_ABSOLUTE "${${var}}")
    set(${var} "${CMAKE_INSTALL_PREFIX}/${${var}}")
  endif()
endforeach()

set(M32_FLAG "")
if(FORCE_32_BIT)
  set(M32_FLAG " -m32 ")
endif()

option(FORCE_COLORED_OUTPUT
       "Always produce ANSI-colored output (GNU/Clang only)." FALSE)
if(${FORCE_COLORED_OUTPUT})
  if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    add_compile_options(-fdiagnostics-color=always)
  elseif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
    add_compile_options(-fcolor-diagnostics)
  endif()
endif()

option(ENABLE_SANITIZER "Enable sanitizer." TRUE)
if(${ENABLE_SANITIZER})
  set(CXX_EXTRA_DEBUG "${CXX_EXTRA_DEBUG} -fsanitize=address")
endif()

option(EXPLICIT_EXCEPTIONS "Explicitly enable C++ exceptions." FALSE)
if(${EXPLICIT_EXCEPTIONS})
  set(CXX_EXTRA "${CXX_EXTRA} -fexceptions")
endif()

set(SUN FALSE)
if(${CMAKE_SYSTEM_NAME} STREQUAL "SunOS")
  set(CXX_EXTRA "${CXX_EXTRA} -mimpure-text")
  add_definitions(-DSUN=1)
  set(SUN TRUE)
endif()

execute_process(COMMAND git
                        log
                        -1
                        --format=%h
                WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
                OUTPUT_VARIABLE GIT_COMMIT_HASH
                OUTPUT_STRIP_TRAILING_WHITESPACE)

option(AMALGAMATION_BUILD
       "Build from the amalgamation files, rather than from the normal sources."
       FALSE)

option(JDBC_DRIVER "Build the DuckDB JDBC driver" FALSE)

option(TREAT_WARNINGS_AS_ERRORS "Treat warnings as errors" FALSE)

if(TREAT_WARNINGS_AS_ERRORS)
  message("Treating warnings as errors.")
endif()

if(NOT MSVC)
  set(CMAKE_CXX_FLAGS_DEBUG
      "${CMAKE_CXX_FLAGS_DEBUG} -g -O0 -DDEBUG -Wall ${M32_FLAG} ${CXX_EXTRA}")
  set(CMAKE_CXX_FLAGS_RELEASE
      "${CMAKE_CXX_FLAGS_RELEASE} -O3 -DNDEBUG ${M32_FLAG} ${CXX_EXTRA}")
  set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELEASE} -g")

  set(
    CXX_EXTRA_DEBUG
    "${CXX_EXTRA_DEBUG} -Wunused-variable -Wunused-const-variable -Werror=vla -Wnarrowing"
    )
  if(TREAT_WARNINGS_AS_ERRORS)
    set(CXX_EXTRA_DEBUG "${CXX_EXTRA_DEBUG} -Werror")
  endif()

  if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU"
     AND CMAKE_CXX_COMPILER_VERSION VERSION_GREATER 8.0)
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} ${CXX_EXTRA_DEBUG}")
  elseif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang"
         AND CMAKE_CXX_COMPILER_VERSION VERSION_GREATER 9.0)
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} ${CXX_EXTRA_DEBUG}")
  else()
    message(WARNING "Please use a recent compiler for debug builds")
  endif()
else()
  set(CMAKE_CXX_WINDOWS_FLAGS
      "/wd4244 /wd4267 /wd4200 /wd26451 /wd26495 /D_CRT_SECURE_NO_WARNINGS")
  if(TREAT_WARNINGS_AS_ERRORS)
    set(CMAKE_CXX_WINDOWS_FLAGS "${CMAKE_CXX_WINDOWS_FLAGS} /WX")
  endif()
  # remove warning from CXX flags
  string(REGEX
         REPLACE "/W[0-4]"
                 ""
                 CMAKE_CXX_FLAGS
                 "${CMAKE_CXX_FLAGS}")
  # add to-be-ignored warnings
  set(
    CMAKE_CXX_FLAGS
    "${CMAKE_CXX_FLAGS} /wd4244 /wd4267 /wd4200 /wd26451 /wd26495 /D_CRT_SECURE_NO_WARNINGS"
    )
endif()

# todo use CHECK_CXX_COMPILER_FLAG(-fsanitize=address SUPPORTS_SANITIZER) etc.

set(CMAKE_C_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG}")
set(CMAKE_C_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE}")
set(CMAKE_C_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO}")

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(DEFAULT_BUILD_TYPE "Release")
  message(STATUS "Setting build type to '${DEFAULT_BUILD_TYPE}'.")
  set(CMAKE_BUILD_TYPE
      "${DEFAULT_BUILD_TYPE}"
      CACHE STRING "Choose the type of build." FORCE)
endif()

include_directories(src/include)
include_directories(third_party/fmt/include)
include_directories(third_party/hyperloglog)
include_directories(third_party/re2)
include_directories(third_party/miniz)
include_directories(third_party/utf8proc/include)
include_directories(third_party/miniparquet)

# todo only regenerate ub file if one of the input files changed hack alert
function(enable_unity_build UB_SUFFIX SOURCE_VARIABLE_NAME)
  set(files ${${SOURCE_VARIABLE_NAME}})

  # Generate a unique filename for the unity build translation unit
  set(unit_build_file ${CMAKE_CURRENT_BINARY_DIR}/ub_${UB_SUFFIX}.cpp)
  set(temp_unit_build_file ${CMAKE_CURRENT_BINARY_DIR}/ub_${UB_SUFFIX}.cpp.tmp)
  # Exclude all translation units from compilation
  set_source_files_properties(${files}
                              PROPERTIES
                              HEADER_FILE_ONLY
                              true)

  set(rebuild FALSE)
  # check if any of the source files have changed
  foreach(source_file ${files})
    if(${CMAKE_CURRENT_SOURCE_DIR}/${source_file} IS_NEWER_THAN
       ${unit_build_file})
      set(rebuild TRUE)
    endif()
  endforeach(source_file)
  # write a temporary file
  file(WRITE ${temp_unit_build_file} "// Unity Build generated by CMake\n")
  foreach(source_file ${files})
    file(
      APPEND ${temp_unit_build_file}
      "#line 0 \"${source_file}\"\n#include <${CMAKE_CURRENT_SOURCE_DIR}/${source_file}>\n"
      )
  endforeach(source_file)

  execute_process(COMMAND ${CMAKE_COMMAND}
                          -E
                          compare_files
                          ${unit_build_file}
                          ${temp_unit_build_file}
                  RESULT_VARIABLE compare_result
                  OUTPUT_VARIABLE bla
                  ERROR_VARIABLE bla)
  if(compare_result EQUAL 0)
    # files are identical: do nothing
  elseif(compare_result EQUAL 1)
    # files are different: rebuild
    set(rebuild TRUE)
  else()
    # error while compiling: rebuild
    set(rebuild TRUE)
  endif()

  if(${rebuild})
    file(WRITE ${unit_build_file} "// Unity Build generated by CMake\n")
    foreach(source_file ${files})
      file(
        APPEND ${unit_build_file}
        "#line 0 \"${source_file}\"\n#include <${CMAKE_CURRENT_SOURCE_DIR}/${source_file}>\n"
        )
    endforeach(source_file)
  endif()

  # Complement list of translation units with the name of ub
  set(${SOURCE_VARIABLE_NAME}
      ${${SOURCE_VARIABLE_NAME}} ${unit_build_file}
      PARENT_SCOPE)
endfunction(enable_unity_build)

function(add_library_unity NAME MODE)
  set(SRCS ${ARGN})
  if(NOT DISABLE_UNITY)
    enable_unity_build(${NAME} SRCS)
  endif()
  add_library(${NAME} OBJECT ${SRCS})
endfunction()

function(disable_target_warnings NAME)
  if(MSVC)
    target_compile_options(${NAME} PRIVATE "/W0")
  elseif("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang"
         OR "${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    target_compile_options(${NAME} PRIVATE "-w")
  endif()
endfunction()

add_subdirectory(extension)
add_subdirectory(src)
add_subdirectory(third_party)
add_subdirectory(test)
add_subdirectory(tools)
if(NOT WIN32 AND NOT SUN)
  add_subdirectory(benchmark)
endif()

# Write the export set for build and install tree
install(EXPORT "${DUCKDB_EXPORT_SET}" DESTINATION "${INSTALL_CMAKE_DIR}")
export(EXPORT "${DUCKDB_EXPORT_SET}"
       FILE "${PROJECT_BINARY_DIR}/${DUCKDB_EXPORT_SET}.cmake")

# Only write the cmake package configuration if the templates exist
set(CMAKE_CONFIG_TEMPLATE "${CMAKE_SOURCE_DIR}/DuckDBConfig.cmake.in")
set(CMAKE_CONFIG_VERSION_TEMPLATE
    "${CMAKE_SOURCE_DIR}/DuckDBConfigVersion.cmake.in")
if(EXISTS ${CMAKE_CONFIG_TEMPLATE} AND EXISTS ${CMAKE_CONFIG_VERSION_TEMPLATE})

  # Configure cmake package config for the build tree
  set(CONF_INCLUDE_DIRS "${PROJECT_SOURCE_DIR}/src/include")
  configure_file(${CMAKE_CONFIG_TEMPLATE}
                 "${PROJECT_BINARY_DIR}/DuckDBConfig.cmake" @ONLY)

  # Configure cmake package config for the install tree
  file(RELATIVE_PATH
       REL_INCLUDE_DIR
       "${INSTALL_CMAKE_DIR}"
       "${INSTALL_INCLUDE_DIR}")
  set(CONF_INCLUDE_DIRS "\${DuckDB_CMAKE_DIR}/${REL_INCLUDE_DIR}")
  configure_file(
    ${CMAKE_CONFIG_TEMPLATE}
    "${PROJECT_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/DuckDBConfig.cmake" @ONLY)

  # Configure cmake package version for build and install tree
  configure_file(${CMAKE_CONFIG_VERSION_TEMPLATE}
                 "${PROJECT_BINARY_DIR}/DuckDBConfigVersion.cmake" @ONLY)

  # Install the cmake package
  install(
    FILES "${PROJECT_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/DuckDBConfig.cmake"
          "${PROJECT_BINARY_DIR}/DuckDBConfigVersion.cmake"
    DESTINATION "${INSTALL_CMAKE_DIR}")
endif()
