cmake_minimum_required(VERSION 3.5)

if(NOT CLANG_TIDY)
  find_package(Python3)
endif()

if(POLICY CMP0026)
  cmake_policy(SET CMP0026 NEW)
endif()

if(POLICY CMP0051)
  cmake_policy(SET CMP0051 NEW)
endif()

if(POLICY CMP0054)
  cmake_policy(SET CMP0054 NEW)
endif()

if(POLICY CMP0063)
  cmake_policy(SET CMP0063 NEW)
endif()

project(DuckDB)

find_package(Threads REQUIRED)

set(DUCKDB_MODULE_BASE_DIR "${CMAKE_CURRENT_LIST_DIR}")

set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(CMAKE_CXX_STANDARD "11" CACHE STRING "C++ standard to enforce")

set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_VERBOSE_MAKEFILE OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_MACOSX_RPATH 1)

find_program(CCACHE_PROGRAM ccache)
if(CCACHE_PROGRAM)
  set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE "${CCACHE_PROGRAM}")
else()
  find_program(CCACHE_PROGRAM sccache)
  if(CCACHE_PROGRAM)
    set_property(GLOBAL PROPERTY RULE_LAUNCH_COMPILE "${CCACHE_PROGRAM}")
  endif()
endif()

# Determine install paths
set(INSTALL_LIB_DIR
    lib
    CACHE PATH "Installation directory for libraries")
set(INSTALL_BIN_DIR
    bin
    CACHE PATH "Installation directory for executables")
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
foreach(p LIB BIN INCLUDE CMAKE)
  set(var INSTALL_${p}_DIR)
  if(NOT IS_ABSOLUTE "${${var}}")
    set(${var} "${CMAKE_INSTALL_PREFIX}/${${var}}")
  endif()
endforeach()

# This option allows --gc-sections flag during extension linking to discard any unused functions or data
if (EXTENSION_STATIC_BUILD AND "${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
  if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -ffunction-sections -fdata-sections")
  elseif(WIN32 AND MVSC)
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} /Gy")
  endif()
endif()

option(DISABLE_UNITY "Disable unity builds." FALSE)
option(USE_WASM_THREADS "Should threads be used" FALSE)
if (${USE_WASM_THREADS})
   set(WASM_THREAD_FLAGS
       -pthread
       -sSHARED_MEMORY=1
   )
endif()

option(FORCE_COLORED_OUTPUT
       "Always produce ANSI-colored output (GNU/Clang only)." FALSE)
if(${FORCE_COLORED_OUTPUT})
  if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    add_compile_options(-fdiagnostics-color=always)
  elseif("${CMAKE_CXX_COMPILER_ID}" MATCHES "Clang$")
    add_compile_options(-fcolor-diagnostics)
  endif()
endif()

set(M32_FLAG "")
if(FORCE_32_BIT)
  set(M32_FLAG " -m32 ")
endif()

set(OS_NAME "unknown")
set(OS_ARCH "amd64")

string(REGEX MATCH "(arm64|aarch64)" IS_ARM "${CMAKE_SYSTEM_PROCESSOR}")
if(IS_ARM)
  set(OS_ARCH "arm64")
elseif(FORCE_32_BIT)
  set(OS_ARCH "i386")
endif()

if(APPLE)
  set(OS_NAME "osx")
endif()
if(WIN32)
  set(OS_NAME "windows")
endif()
if(UNIX AND NOT APPLE)
  set(OS_NAME "linux") # sorry BSD
endif()

option(FORCE_WARN_UNUSED "Unused code objects lead to compiler warnings." FALSE)

option(ENABLE_EXTENSION_AUTOLOADING "Enable extension auto-loading by default." FALSE)
option(ENABLE_EXTENSION_AUTOINSTALL "Enable extension auto-installing by default." FALSE)
option(EXTENSION_TESTS_ONLY "Only load the tests for extensions, don't actually build them; useful for testing loadable extensions" FALSE)
option(WASM_LOADABLE_EXTENSIONS "WebAssembly build with loadable extensions." FALSE)
option(ENABLE_SANITIZER "Enable address sanitizer." TRUE)
option(ENABLE_THREAD_SANITIZER "Enable thread sanitizer." FALSE)
option(ENABLE_UBSAN "Enable undefined behavior sanitizer." TRUE)
option(DISABLE_VPTR_SANITIZER "Disable vptr sanitizer; work-around for sanitizer false positive on Macbook M1" FALSE)
option(
  FORCE_SANITIZER
  "Forces building with sanitizers even if the Python and R modules are enabled."
  FALSE)
if((BUILD_PYTHON)
   AND (ENABLE_SANITIZER OR ENABLE_UBSAN)
   AND ("${CMAKE_BUILD_TYPE}" STREQUAL "Debug"))
  if(FORCE_SANITIZER)
    message(
      WARNING
        "FORCE_SANITIZER is set and the Python/R/Node builds are enabled. Sanitizers will be linked as a shared library (-shared-libasan). You may need to do LD_PRELOAD tricks to load packages built in this way."
    )
    set(CXX_EXTRA_DEBUG "${CXX_EXTRA_DEBUG} -shared-libasan")
  else()
    message(
      WARNING
        "Sanitizers are enabled but will not be built because the Python/R builds are enabled. Use FORCE_SANITIZER to force building of the sanitizers even when building these packages."
    )
    set(ENABLE_SANITIZER FALSE)
    set(ENABLE_UBSAN FALSE)
  endif()
endif()
if(${ENABLE_THREAD_SANITIZER})
  if(${ENABLE_SANITIZER})
    message(
      WARNING
        "Both thread and address sanitizers are enabled. This is not supported. The address sanitizer will be disabled, and we will run with only the thread sanitizer."
    )
  endif()
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DDUCKDB_THREAD_SANITIZER")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=thread")
elseif(${ENABLE_SANITIZER})
  if(FORCE_ASSERT)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address")
  else()
    set(CXX_EXTRA_DEBUG "${CXX_EXTRA_DEBUG} -fsanitize=address")
  endif()
endif()

if (${DISABLE_VPTR_SANITIZER})
else()
  if(APPLE AND CMAKE_SYSTEM_PROCESSOR MATCHES "arm64")
    if("${CMAKE_CXX_COMPILER_VERSION}" VERSION_GREATER 14.0)
      message(
        WARNING
          "Not disabling vptr sanitizer on M1 Macbook - set DISABLE_VPTR_SANITIZER manually if you run into issues with false positives in the sanitizer"
      )
    else()
    set(DISABLE_VPTR_SANITIZER TRUE)
    endif()
  endif()
endif()

if(${ENABLE_UBSAN})
  if(${ENABLE_THREAD_SANITIZER})
    message(
      WARNING
        "Both thread and undefined sanitizers are enabled. This is not supported. The undefined sanitizer will be disabled, and we will run with only the thread sanitizer."
    )
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DDUCKDB_THREAD_SANITIZER")
  else()
    if(FORCE_ASSERT)
      set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=undefined -fno-sanitize-recover=all")
      if (${DISABLE_VPTR_SANITIZER})
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-sanitize=vptr")
      endif()
    else()
      set(CXX_EXTRA_DEBUG "${CXX_EXTRA_DEBUG} -fsanitize=undefined -fno-sanitize-recover=all")
      if (${DISABLE_VPTR_SANITIZER})
        set(CXX_EXTRA_DEBUG "${CXX_EXTRA_DEBUG} -fno-sanitize=vptr")
      endif()
    endif()
  endif()
endif()

option(EXPLICIT_EXCEPTIONS "Explicitly enable C++ exceptions." FALSE)
if(${EXPLICIT_EXCEPTIONS})
  set(CXX_EXTRA "${CXX_EXTRA} -fexceptions")
endif()

if (ENABLE_EXTENSION_AUTOLOADING)
  add_definitions(-DDUCKDB_EXTENSION_AUTOLOAD_DEFAULT=1)
endif()
if (ENABLE_EXTENSION_AUTOINSTALL)
  add_definitions(-DDUCKDB_EXTENSION_AUTOINSTALL_DEFAULT=${ENABLE_EXTENSION_AUTOINSTALL})
endif()

option(OSX_BUILD_UNIVERSAL "Build both architectures on OSX and create a single binary containing both." FALSE)
if (OSX_BUILD_UNIVERSAL)
  if (NOT APPLE)
    error("This only makes sense on OSX")
  endif()
  SET(CMAKE_OSX_ARCHITECTURES "x86_64;arm64" CACHE STRING "Build architectures for Mac OS X" FORCE)
  set(CMAKE_OSX_DEPLOYMENT_TARGET 11.0 CACHE STRING "Minimum OS X deployment version" FORCE)
endif()

if (OSX_BUILD_ARCH)
  message(STATUS "building for OSX architecture: ${OSX_BUILD_ARCH}")
  if (NOT APPLE)
    error("This only makes sense on OSX")
  endif()
  SET(CMAKE_OSX_ARCHITECTURES "${OSX_BUILD_ARCH}" CACHE STRING "Build architectures for Mac OS X" FORCE)
  set(CMAKE_OSX_DEPLOYMENT_TARGET 11.0 CACHE STRING "Minimum OS X deployment version" FORCE)
endif()

set(SUN FALSE)
if(${CMAKE_SYSTEM_NAME} STREQUAL "SunOS")
  set(CXX_EXTRA "${CXX_EXTRA} -mimpure-text")
  add_definitions(-DSUN=1)
  set(SUN TRUE)
endif()


if (OVERRIDE_GIT_DESCRIBE)
  if (OVERRIDE_GIT_DESCRIBE MATCHES "^v[0-9]+\.[0-9]+\.[0-9]+\-[0-9]+\-g[a-f0-9]+$")
    set(GIT_DESCRIBE "${OVERRIDE_GIT_DESCRIBE}")
  elseif(OVERRIDE_GIT_DESCRIBE MATCHES "^v[0-9]+\.[0-9]+\.[0-9]+$")
    find_package(Git)
    if(Git_FOUND)
      execute_process(
            COMMAND ${GIT_EXECUTABLE} log -1 --format=%h
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
            RESULT_VARIABLE GIT_RESULT
            OUTPUT_VARIABLE GIT_COMMIT_HASH
            OUTPUT_STRIP_TRAILING_WHITESPACE)
        set(GIT_DESCRIBE "${OVERRIDE_GIT_DESCRIBE}-0-g${GIT_COMMIT_HASH}")
    else()
      set(GIT_DESCRIBE "${OVERRIDE_GIT_DESCRIBE}-0-g0123456789")
    endif()
  else()
    message(FATAL_ERROR "Provided OVERRIDE_GIT_DESCRIBE '${OVERRIDE_GIT_DESCRIBE}' do not match supported versions, either fully specified 'vX.Y.Z-N-gGITHASH123' or version only 'vX.Y.Z'")
  endif()
else()
  find_package(Git)
  if(Git_FOUND)
     if (NOT DEFINED GIT_COMMIT_HASH)
     execute_process(
            COMMAND ${GIT_EXECUTABLE} log -1 --format=%h
            WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
            RESULT_VARIABLE GIT_RESULT
            OUTPUT_VARIABLE GIT_COMMIT_HASH
            OUTPUT_STRIP_TRAILING_WHITESPACE)
    endif()
    execute_process(
          COMMAND ${GIT_EXECUTABLE} describe --tags --long
          WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
          OUTPUT_VARIABLE GIT_DESCRIBE
          OUTPUT_STRIP_TRAILING_WHITESPACE)
  else()
    message("Git NOT FOUND and EXTERNAL_GIT_DESCRIBE not provided, continuing with dummy version v0.0.1")
    set(GIT_DESCRIBE "v0.0.1-0-g0123456789")
  endif()
endif()

string(REGEX REPLACE "v([0-9]+)\.[0-9]+\.[0-9]+\-.*" "\\1" DUCKDB_MAJOR_VERSION "${GIT_DESCRIBE}")
string(REGEX REPLACE "v[0-9]+\.([0-9]+)\.[0-9]+\-.*" "\\1" DUCKDB_MINOR_VERSION "${GIT_DESCRIBE}")
string(REGEX REPLACE "v[0-9]+\.[0-9]+\.([0-9]+)\-.*" "\\1" DUCKDB_PATCH_VERSION "${GIT_DESCRIBE}")
string(REGEX REPLACE "v[0-9]+\.[0-9]+\.[0-9]+\-([0-9]+)\-g.*" "\\1" DUCKDB_DEV_ITERATION "${GIT_DESCRIBE}")
if (NOT DEFINED GIT_COMMIT_HASH)
  string(REGEX REPLACE "v[0-9]+\.[0-9]+\.[0-9]+\-[0-9]+\-g([a-f0-9]+)" "\\1" GIT_COMMIT_HASH "${GIT_DESCRIBE}")
endif()

string(LENGTH "${GIT_COMMIT_HASH}" LENGTH_GIT_COMMIT_HASH)
if (NOT ${LENGTH_GIT_COMMIT_HASH} EQUAL 10)
   message(STATUS "GIT_COMMIT_HASH has lenght ${LENGTH_GIT_COMMIT_HASH} different than the expected 10")
endif()

string(SUBSTRING "${GIT_COMMIT_HASH}" 0 10 GIT_COMMIT_HASH)

if(DUCKDB_DEV_ITERATION EQUAL 0)
  # on a tag; directly use the version
  set(DUCKDB_VERSION "v${DUCKDB_MAJOR_VERSION}.${DUCKDB_MINOR_VERSION}.${DUCKDB_PATCH_VERSION}")
else()
  # not on a tag, increment the patch version by one and add a -devX suffix
  math(EXPR DUCKDB_PATCH_VERSION "${DUCKDB_PATCH_VERSION}+1")
  set(DUCKDB_VERSION "v${DUCKDB_MAJOR_VERSION}.${DUCKDB_MINOR_VERSION}.${DUCKDB_PATCH_VERSION}-dev${DUCKDB_DEV_ITERATION}")
endif()

string(REGEX MATCH ".*dev.*" DUCKDB_EXTENSION_FOLDER_IS_VERSION "${DUCKDB_VERSION}")

if(DUCKDB_EXTENSION_FOLDER_IS_VERSION AND NOT GIT_COMMIT_HASH STREQUAL "")
  set(DUCKDB_NORMALIZED_VERSION "${GIT_COMMIT_HASH}")
else()
  set(DUCKDB_NORMALIZED_VERSION "${DUCKDB_VERSION}")
endif()

if(EMSCRIPTEN)
  set(EXTENSION_POSTFIX "duckdb_extension.wasm")
else()
  set(EXTENSION_POSTFIX "duckdb_extension")
endif()

message(STATUS "git hash ${GIT_COMMIT_HASH}, version ${DUCKDB_VERSION}, extension folder ${DUCKDB_NORMALIZED_VERSION}")

option(AMALGAMATION_BUILD
       "Build from the amalgamation files, rather than from the normal sources."
       FALSE)

option(BUILD_MAIN_DUCKDB_LIBRARY
        "Build the main duckdb library and executable."
        TRUE)
option(EXTENSION_STATIC_BUILD
        "Extension build linking statically with DuckDB. Required for building linux loadable extensions."
        FALSE)

if(WIN32 OR ZOS)
  set(EXTENSION_STATIC_BUILD TRUE)
endif()

option(BUILD_EXTENSIONS_ONLY "Build all extension as linkable, overriding DONT_LINK, and don't build core." FALSE)
option(BUILD_CORE_FUNCTIONS_EXTENSION "Build the core functions." TRUE)
option(BUILD_BENCHMARKS "Enable building of the benchmark suite." FALSE)
option(BUILD_TPCE "Enable building of the TPC-E tool." FALSE)
option(DISABLE_BUILTIN_EXTENSIONS "Disable linking extensions." FALSE)
option(BUILD_ODBC_DRIVER "Build the DuckDB ODBC driver" FALSE)
option(BUILD_PYTHON "Build the DuckDB Python extension" FALSE)
option(USER_SPACE "Build the DuckDB Python in the user space" FALSE)
option(FORCE_QUERY_LOG "If enabled, all queries will be logged to the specified path" OFF)
option(BUILD_SHELL "Build the DuckDB Shell and SQLite API Wrappers" TRUE)
option(DISABLE_THREADS "Disable support for multi-threading" FALSE)
option(DISABLE_EXTENSION_LOAD "Disable support for loading and installing extensions" FALSE)
option(DISABLE_STR_INLINE "Debug setting: disable inlining of strings" FALSE)
option(DISABLE_MEMORY_SAFETY "Debug setting: disable memory access checks at runtime" FALSE)
option(DISABLE_ASSERTIONS "Debug setting: disable assertions" FALSE)
option(ALTERNATIVE_VERIFY "Debug setting: use alternative verify mode" FALSE)
option(RUN_SLOW_VERIFIERS "Debug setting: enable a more extensive set of verifiers" FALSE)
option(DESTROY_UNPINNED_BLOCKS "Debug setting: destroy unpinned buffer-managed blocks" FALSE)
option(FORCE_ASYNC_SINK_SOURCE "Debug setting: forces sinks/sources to block the first 2 times they're called" FALSE)
option(DEBUG_ALLOCATION "Debug setting: keep track of outstanding allocations to detect memory leaks" FALSE)
option(DEBUG_STACKTRACE "Debug setting: print a stracktrace on asserts and when testing crashes" FALSE)
option(DEBUG_MOVE "Debug setting: Ensure std::move is being used" FALSE)
option(VERIFY_VECTOR "Debug setting: verify vectors (options: dictionary_expression, dictionary_operator, constant_operator, sequence_operator, nested_shuffle)" "none")
option(CLANG_TIDY "Enable build for clang-tidy, this disables all source files excluding the core database. This does not produce a working build." FALSE)
option(BUILD_UNITTESTS "Build the C++ Unit Tests." TRUE)
option(EXTENSION_CONFIG_BUILD "Produce extension configuration artifacts instead of building. (such as shared vcpkg.json, extensions.txt)" FALSE)
option(CUSTOM_LINKER "Use a custom linker program" "")
option(
  ASSERT_EXCEPTION
  "Throw an exception on an assert failing, instead of triggering a sigabort"
  TRUE)
option(FORCE_ASSERT "Enable checking of assertions, even in release mode" FALSE)

option(TREAT_WARNINGS_AS_ERRORS "Treat warnings as errors" FALSE)
option(EXPORT_DLL_SYMBOLS "Export dll symbols on Windows, else import" TRUE)
option(BUILD_RDTSC "Enable the rdtsc instruction." FALSE)
option(TEST_REMOTE_INSTALL "Test installation of specific extensions." FALSE)

if(${BUILD_RDTSC})
  add_compile_definitions(RDTSC)
endif()

if(BUILD_EXTENSIONS_ONLY)
  set(BUILD_MAIN_DUCKDB_LIBRARY FALSE)
endif()

if(EXTENSION_CONFIG_BUILD)
  set(BUILD_MAIN_DUCKDB_LIBRARY FALSE)
endif()

if (NOT BUILD_MAIN_DUCKDB_LIBRARY)
  set(BUILD_UNITTESTS FALSE)
  set(BUILD_SHELL FALSE)
  set(DISABLE_BUILTIN_EXTENSIONS TRUE)
endif()

if(TREAT_WARNINGS_AS_ERRORS)
  message("Treating warnings as errors.")
endif()

if(ASSERT_EXCEPTION)
else()
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DDUCKDB_CRASH_ON_ASSERT")
endif()

if(DISABLE_STR_INLINE)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DDUCKDB_DEBUG_NO_INLINE")
endif()

if(DISABLE_MEMORY_SAFETY)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DDUCKDB_DEBUG_NO_SAFETY")
endif()

if(DISABLE_ASSERTIONS)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DDISABLE_ASSERTIONS")
endif()

if(DESTROY_UNPINNED_BLOCKS)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DDUCKDB_DEBUG_DESTROY_BLOCKS")
endif()

if(FORCE_ASYNC_SINK_SOURCE)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DDUCKDB_DEBUG_ASYNC_SINK_SOURCE")
endif()

if(RUN_SLOW_VERIFIERS)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DDUCKDB_RUN_SLOW_VERIFIERS")
endif()

if(ALTERNATIVE_VERIFY)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DDUCKDB_ALTERNATIVE_VERIFY")
endif()

if(DEBUG_ALLOCATION)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DDUCKDB_DEBUG_ALLOCATION")
endif()

if(DEBUG_MOVE)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DDUCKDB_DEBUG_MOVE")
endif()

if (CLANG_TIDY)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DDUCKDB_CLANG_TIDY")
endif()

if(FORCE_ASSERT)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DDUCKDB_FORCE_ASSERT")
endif()

if("${VERIFY_VECTOR}" STREQUAL "none" OR "${VERIFY_VECTOR}" STREQUAL "OFF")
  # do nothing
else()
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DDUCKDB_VERIFY_VECTOR")
  if("${VERIFY_VECTOR}" STREQUAL "dictionary_expression")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DDUCKDB_VERIFY_DICTIONARY_EXPRESSION")
  else()
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DDUCKDB_VERIFY_VECTOR_OPERATOR")
    if("${VERIFY_VECTOR}" STREQUAL "dictionary_operator")
      set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DDUCKDB_VERIFY_DICTIONARY_OPERATOR")
    elseif("${VERIFY_VECTOR}" STREQUAL "constant_operator")
      set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DDUCKDB_VERIFY_CONSTANT_OPERATOR")
    elseif("${VERIFY_VECTOR}" STREQUAL "sequence_operator")
      set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DDUCKDB_VERIFY_SEQUENCE_OPERATOR")
    elseif("${VERIFY_VECTOR}" STREQUAL "nested_shuffle")
      set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DDUCKDB_VERIFY_NESTED_SHUFFLE")
    else()
      message(FATAL_ERROR "unsupported option for VERIFY_VECTOR ${VERIFY_VECTOR}")
    endif()
  endif()
endif()

if(CUSTOM_LINKER)
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fuse-ld=${CUSTOM_LINKER}")
  set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -fuse-ld=${CUSTOM_LINKER}")
endif()

if(NOT MSVC)
  if(${FORCE_WARN_UNUSED})
    set(CXX_EXTRA "${CXX_EXTRA} -Wunused")
  endif()
  if(TREAT_WARNINGS_AS_ERRORS)
    set(CXX_EXTRA "${CXX_EXTRA} -Werror")
  endif()
  set(CMAKE_CXX_FLAGS_DEBUG
      "${CMAKE_CXX_FLAGS_DEBUG} -g -O0 -DDEBUG -Wall ${M32_FLAG} ${CXX_EXTRA}")
  set(CMAKE_CXX_FLAGS_RELEASE
      "${CMAKE_CXX_FLAGS_RELEASE} -O3 -DNDEBUG ${M32_FLAG} ${CXX_EXTRA}")
  if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang" AND CMAKE_LTO)
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -flto=${CMAKE_LTO}")
  elseif (CMAKE_CXX_COMPILER_ID STREQUAL "GNU" AND CMAKE_LTO)
    set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -flto")
  endif()
  set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELEASE} -g")

  set(CXX_EXTRA_DEBUG
      "${CXX_EXTRA_DEBUG} -Wunused -Werror=vla -Wnarrowing -pedantic"
  )

  if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU" AND CMAKE_CXX_COMPILER_VERSION
                                                   VERSION_GREATER 8.0)
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} ${CXX_EXTRA_DEBUG}")
  elseif("${CMAKE_CXX_COMPILER_ID}" MATCHES "Clang$"
         AND CMAKE_CXX_COMPILER_VERSION VERSION_GREATER 9.0)
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} ${CXX_EXTRA_DEBUG}")
  else()
    message(WARNING "Please use a recent compiler for debug builds")
  endif()
else()
  set(CMAKE_CXX_WINDOWS_FLAGS
      "/wd4244 /wd4267 /wd4200 /wd26451 /wd26495 /D_CRT_SECURE_NO_WARNINGS /utf-8")
  if(TREAT_WARNINGS_AS_ERRORS)
    set(CMAKE_CXX_WINDOWS_FLAGS "${CMAKE_CXX_WINDOWS_FLAGS} /WX")
  endif()
  # remove warning from CXX flags
  string(REGEX REPLACE "/W[0-4]" "" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")
  # add to-be-ignored warnings
  set(CMAKE_CXX_FLAGS
      "${CMAKE_CXX_FLAGS} ${CMAKE_CXX_WINDOWS_FLAGS}"
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

if(OS_NAME STREQUAL "windows")
  list (FIND DUCKDB_EXTENSION_NAMES jemalloc _index)
  if (${_index} GREATER -1)
    # have to throw an error because this will crash at runtime
    message(FATAL_ERROR "The jemalloc extension is not supported on Windows")
  endif()
endif()


include_directories(src/include)
include_directories(third_party/fsst)
include_directories(third_party/fmt/include)
include_directories(third_party/hyperloglog)
include_directories(third_party/fastpforlib)
include_directories(third_party/skiplist)
include_directories(third_party/fast_float)
include_directories(third_party/re2)
include_directories(third_party/miniz)
include_directories(third_party/utf8proc/include)
include_directories(third_party/concurrentqueue)
include_directories(third_party/pcg)
include_directories(third_party/tdigest)
include_directories(third_party/mbedtls/include)
include_directories(third_party/jaro_winkler)

# todo only regenerate ub file if one of the input files changed hack alert
function(enable_unity_build UB_SUFFIX SOURCE_VARIABLE_NAME)
  set(files ${${SOURCE_VARIABLE_NAME}})

  # Generate a unique filename for the unity build translation unit
  set(unit_build_file ${CMAKE_CURRENT_BINARY_DIR}/ub_${UB_SUFFIX}.cpp)
  set(temp_unit_build_file ${CMAKE_CURRENT_BINARY_DIR}/ub_${UB_SUFFIX}.cpp.tmp)
  # Exclude all translation units from compilation
  set_source_files_properties(${files} PROPERTIES HEADER_FILE_ONLY true)

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
      "#include <${CMAKE_CURRENT_SOURCE_DIR}/${source_file}>\n"
    )
  endforeach(source_file)

  execute_process(
    COMMAND ${CMAKE_COMMAND} -E compare_files ${unit_build_file}
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
        "#include <${CMAKE_CURRENT_SOURCE_DIR}/${source_file}>\n"
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
  elseif("${CMAKE_CXX_COMPILER_ID}" MATCHES "Clang$"
         OR "${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
    target_compile_options(${NAME} PRIVATE "-w")
  endif()
endfunction()

function(add_extension_definitions)
  include_directories(${PROJECT_SOURCE_DIR}/extension)
  if(NOT "${TEST_WITH_LOADABLE_EXTENSION}" STREQUAL "")
    string(REPLACE ";"  "," COMMA_SEPARATED_EXTENSIONS "${TEST_WITH_LOADABLE_EXTENSION}")
    # Note: weird commas are for easy substring matching in c++
    add_definitions(-DDUCKDB_EXTENSIONS_TEST_WITH_LOADABLE=\",${COMMA_SEPARATED_EXTENSIONS},\")
    add_definitions(-DDUCKDB_EXTENSIONS_BUILD_PATH="${CMAKE_BINARY_DIR}/extension")
  endif()
  if(NOT("${TEST_REMOTE_INSTALL}" STREQUAL "OFF"))
    add_definitions(-DDUCKDB_TEST_REMOTE_INSTALL="${TEST_REMOTE_INSTALL}")
  endif()

  if(${DISABLE_BUILTIN_EXTENSIONS})
    add_definitions(-DDISABLE_BUILTIN_EXTENSIONS=${DISABLE_BUILTIN_EXTENSIONS})
  endif()

  # Include paths for any registered out-of-tree extensions
  foreach(EXT_NAME IN LISTS DUCKDB_EXTENSION_NAMES)
    string(TOUPPER ${EXT_NAME} EXT_NAME_UPPERCASE)
    if(${DUCKDB_EXTENSION_${EXT_NAME_UPPERCASE}_SHOULD_LINK})
      add_definitions(-DDUCKDB_EXTENSION_${EXT_NAME_UPPERCASE}_LINKED=1)
      if (DEFINED DUCKDB_EXTENSION_${EXT_NAME_UPPERCASE}_INCLUDE_PATH)
        include_directories("${DUCKDB_EXTENSION_${EXT_NAME_UPPERCASE}_INCLUDE_PATH}")
      else()
        # We try the default locations for headers
        include_directories("${PROJECT_SOURCE_DIR}/extension_external/${EXT_NAME}/src/include")
        include_directories("${PROJECT_SOURCE_DIR}/extension_external/${EXT_NAME}/include")
      endif()
    endif()
  endforeach()
endfunction()

function(add_extension_dependencies LIBRARY)
  foreach(EXT_NAME IN LISTS DUCKDB_EXTENSION_NAMES)
    string(TOUPPER ${EXT_NAME} EXTENSION_NAME_UPPERCASE)
    if (DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_SHOULD_LINK)
      add_dependencies(${LIBRARY} ${EXT_NAME}_extension)
    endif()
  endforeach()
endfunction()

function(link_extension_libraries LIBRARY)
  if(${DISABLE_BUILTIN_EXTENSIONS})
    return()
  endif()

  # Now link against any registered out-of-tree extensions
  foreach(EXT_NAME IN LISTS DUCKDB_EXTENSION_NAMES)
    string(TOUPPER ${EXT_NAME} EXT_NAME_UPPERCASE)
    if (${DUCKDB_EXTENSION_${EXT_NAME_UPPERCASE}_SHOULD_LINK})
      target_link_libraries(${LIBRARY} ${EXT_NAME}_extension)
    endif()
  endforeach()

endfunction()

function(link_threads LIBRARY)
    target_link_libraries(${LIBRARY} Threads::Threads)
endfunction()

# Deploys extensions to a local repository (a folder structure that contains the duckdb version + binary arch)
if ("${LOCAL_EXTENSION_REPO}" STREQUAL "")
  set(LOCAL_EXTENSION_REPO_DIR ${CMAKE_BINARY_DIR}/repository)
else()
  if (NOT Python3_FOUND)
    MESSAGE(FATAL_ERROR "Could not find python3 executable, when providing LOCAL_EXTENSION_REPO this is compulsory")
  endif()
  set(LOCAL_EXTENSION_REPO_DIR ${LOCAL_EXTENSION_REPO})
endif()

set(LOCAL_EXTENSION_REPO FALSE)
if (NOT EXTENSION_CONFIG_BUILD AND NOT ${EXTENSION_TESTS_ONLY} AND NOT CLANG_TIDY)
  if (NOT Python3_FOUND)
    add_custom_target(
          duckdb_local_extension_repo ALL)
    MESSAGE(STATUS "Could not find python3, create extension directory step will be skipped")
  else()
    add_custom_target(
          duckdb_local_extension_repo ALL
          COMMAND
          ${Python3_EXECUTABLE} scripts/create_local_extension_repo.py "${DUCKDB_NORMALIZED_VERSION}" "${CMAKE_CURRENT_BINARY_DIR}/duckdb_platform_out" "${CMAKE_CURRENT_BINARY_DIR}" "${LOCAL_EXTENSION_REPO_DIR}" "${EXTENSION_POSTFIX}"
          WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
          COMMENT Create local extension repository)
    add_dependencies(duckdb_local_extension_repo duckdb_platform)
    set(LOCAL_EXTENSION_REPO TRUE)
    message(STATUS "Extensions will be deployed to: ${LOCAL_EXTENSION_REPO_DIR}")
  endif()
endif()

function(build_loadable_extension_directory NAME OUTPUT_DIRECTORY EXTENSION_VERSION PARAMETERS)
  set(TARGET_NAME ${NAME}_loadable_extension)
  if (LOCAL_EXTENSION_REPO)
    add_dependencies(duckdb_local_extension_repo ${NAME}_loadable_extension)
  endif()
  # all parameters after output_directory
  set(FILES "${ARGV}")
  # remove name, output_directory, extension_version, parameters
  list(REMOVE_AT FILES 0 1 2 3)

  # parse parameters
  string(FIND "${PARAMETERS}" "-no-warnings" IGNORE_WARNINGS)

  add_library(${TARGET_NAME} SHARED ${FILES})
  # this disables the -Dsome_target_EXPORTS define being added by cmake which otherwise trips clang-tidy (yay)
  set_target_properties(${TARGET_NAME} PROPERTIES DEFINE_SYMBOL "")
  set_target_properties(${TARGET_NAME} PROPERTIES OUTPUT_NAME ${NAME})
  set_target_properties(${TARGET_NAME} PROPERTIES PREFIX "")
  if(${IGNORE_WARNINGS} GREATER -1)
    disable_target_warnings(${TARGET_NAME})
  endif()
  # loadable extension binaries can be built two ways:
  # 1. EXTENSION_STATIC_BUILD=1
  #    DuckDB is statically linked into each extension binary. This increases portability because in several situations
  #    DuckDB itself may have been loaded with RTLD_LOCAL. This is currently the main way we distribute the loadable
  #    extension binaries
  # 2. EXTENSION_STATIC_BUILD=0
  #    The DuckDB symbols required by the loadable extensions are left unresolved. This will reduce the size of the binaries
  #    and works well when running the DuckDB cli directly. For windows this uses delay loading. For MacOS and linux the
  #    dynamic loader will look up the missing symbols when the extension is dlopen-ed.
  if(WASM_LOADABLE_EXTENSIONS)
    set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -sSIDE_MODULE=1 -DWASM_LOADABLE_EXTENSIONS")
  elseif (EXTENSION_STATIC_BUILD)
    if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU" OR "${CMAKE_CXX_COMPILER_ID}" MATCHES "Clang")
      if (APPLE)
        set_target_properties(${TARGET_NAME} PROPERTIES CXX_VISIBILITY_PRESET hidden)
        # Note that on MacOS we need to use the -exported_symbol whitelist feature due to a lack of -exclude-libs flag in mac's ld variant
        set(WHITELIST "-Wl,-exported_symbol,_${NAME}_init -Wl,-exported_symbol,_${NAME}_version -Wl,-exported_symbol,_${NAME}_storage_ini*")
        target_link_libraries(${TARGET_NAME} duckdb_static ${DUCKDB_EXTRA_LINK_FLAGS} -Wl,-dead_strip ${WHITELIST})
      elseif (ZOS)
        target_link_libraries(${TARGET_NAME} duckdb_static ${DUCKDB_EXTRA_LINK_FLAGS})
      else()
        # For GNU we rely on fvisibility=hidden to hide the extension symbols and use -exclude-libs to hide the duckdb symbols
        set_target_properties(${TARGET_NAME} PROPERTIES CXX_VISIBILITY_PRESET hidden)
        target_link_libraries(${TARGET_NAME} duckdb_static ${DUCKDB_EXTRA_LINK_FLAGS} -Wl,--gc-sections -Wl,--exclude-libs,ALL)
      endif()
    elseif (WIN32)
      target_link_libraries(${TARGET_NAME} duckdb_static ${DUCKDB_EXTRA_LINK_FLAGS})
    else()
      error("EXTENSION static build is only intended for Linux and Windows on MVSC")
    endif()
  else()
    if (WIN32)
      target_link_libraries(${TARGET_NAME} duckdb ${DUCKDB_EXTRA_LINK_FLAGS})
    elseif("${CMAKE_CXX_COMPILER_ID}" MATCHES "Clang$")
      if (APPLE)
        set_target_properties(${TARGET_NAME} PROPERTIES LINK_FLAGS "-undefined dynamic_lookup")
      endif()
    endif()
  endif()


  target_compile_definitions(${TARGET_NAME} PUBLIC -DDUCKDB_BUILD_LOADABLE_EXTENSION)
  set_target_properties(${TARGET_NAME} PROPERTIES SUFFIX
          ".duckdb_extension")

  if(EMSCRIPTEN)
    set_target_properties(${TARGET_NAME} PROPERTIES SUFFIX ".duckdb_extension.wasm")
  endif()

  if(MSVC)
    set_target_properties(
            ${TARGET_NAME} PROPERTIES RUNTIME_OUTPUT_DIRECTORY_DEBUG
            "${CMAKE_BINARY_DIR}/${OUTPUT_DIRECTORY}")
    set_target_properties(
            ${TARGET_NAME} PROPERTIES RUNTIME_OUTPUT_DIRECTORY_RELEASE
            "${CMAKE_BINARY_DIR}/${OUTPUT_DIRECTORY}")
  endif()

  if(EMSCRIPTEN)
    # Copy file.duckdb_extension.wasm to file.duckdb_extension.wasm.lib
    add_custom_command(
      TARGET ${TARGET_NAME}
      POST_BUILD
      COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:${TARGET_NAME}> $<TARGET_FILE:${TARGET_NAME}>.lib
      )
    # Compile the library into the actual wasm file
    string(TOUPPER ${NAME} EXTENSION_NAME_UPPERCASE)
    add_custom_command(
      TARGET ${TARGET_NAME}
      POST_BUILD
      COMMAND emcc $<TARGET_FILE:${TARGET_NAME}>.lib -o $<TARGET_FILE:${TARGET_NAME}> -O3 -sSIDE_MODULE=2 -sEXPORTED_FUNCTIONS="_${NAME}_init" ${WASM_THREAD_FLAGS} ${DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_LINKED_LIBS}
      )
  endif()
  add_custom_command(
    TARGET ${TARGET_NAME}
    POST_BUILD
    COMMAND
    ${CMAKE_COMMAND} -DEXTENSION=$<TARGET_FILE:${TARGET_NAME}> -DPLATFORM_FILE=${DuckDB_BINARY_DIR}/duckdb_platform_out -DDUCKDB_VERSION="${DUCKDB_NORMALIZED_VERSION}" -DEXTENSION_VERSION="${EXTENSION_VERSION}" -DNULL_FILE=${DUCKDB_MODULE_BASE_DIR}/scripts/null.txt -P ${DUCKDB_MODULE_BASE_DIR}/scripts/append_metadata.cmake
    )
  add_dependencies(${TARGET_NAME} duckdb_platform)
  if (NOT EXTENSION_CONFIG_BUILD AND NOT ${EXTENSION_TESTS_ONLY} AND NOT CLANG_TIDY)
    add_dependencies(duckdb_local_extension_repo ${TARGET_NAME})
  endif()
endfunction()

function(build_loadable_extension NAME PARAMETERS)
  # all parameters after name
  set(FILES "${ARGV}")
  list(REMOVE_AT FILES 0 1)
  string(TOUPPER ${NAME} EXTENSION_NAME_UPPERCASE)

  build_loadable_extension_directory(${NAME} "extension/${NAME}" "${DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_EXT_VERSION}" "${PARAMETERS}" ${FILES})
endfunction()

function(build_static_extension NAME PARAMETERS)
  # all parameters after name
  set(FILES "${ARGV}")
  list(REMOVE_AT FILES 0)
  add_library(${NAME}_extension STATIC ${FILES})
  target_link_libraries(${NAME}_extension duckdb_static)
endfunction()

# Internal extension register function
function(register_extension NAME DONT_LINK DONT_BUILD LOAD_TESTS PATH INCLUDE_PATH TEST_PATH LINKED_LIBS)
  string(TOLOWER ${NAME} EXTENSION_NAME_LOWERCASE)
  string(TOUPPER ${NAME} EXTENSION_NAME_UPPERCASE)

  set(DUCKDB_EXTENSION_NAMES ${DUCKDB_EXTENSION_NAMES} ${EXTENSION_NAME_LOWERCASE} PARENT_SCOPE)

  if ("${LOAD_TESTS}")
    set(DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_LOAD_TESTS TRUE PARENT_SCOPE)
  else()
    set(DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_LOAD_TESTS FALSE PARENT_SCOPE)
  endif()

  if (${BUILD_EXTENSIONS_ONLY})
    set(DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_SHOULD_LINK TRUE PARENT_SCOPE)
  else ()
    if (NOT ${DONT_LINK} AND NOT DISABLE_BUILTIN_EXTENSIONS)
      set(DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_SHOULD_LINK TRUE PARENT_SCOPE)
    else()
      set(DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_SHOULD_LINK FALSE PARENT_SCOPE)
    endif()
  endif()

  set(DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_LINKED_LIBS "${LINKED_LIBS}" PARENT_SCOPE)

  # Allows explicitly disabling extensions that may be specified in other configurations
  if (NOT ${DONT_BUILD} AND NOT ${EXTENSION_TESTS_ONLY})
    set(DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_SHOULD_BUILD TRUE PARENT_SCOPE)
  else()
    set(DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_SHOULD_BUILD FALSE PARENT_SCOPE)
    set(DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_SHOULD_LINK FALSE PARENT_SCOPE)
  endif()

  if ("${PATH}" STREQUAL "")
    message(FATAL_ERROR "Invalid path set for extension '${NAME}' : '${INCLUDE}'")
  endif()
  if ("${INCLUDE_PATH}" STREQUAL "")
    message(FATAL_ERROR "Invalid include path for extension '${NAME}' : '${INCLUDE_PATH}'")
  endif()
  if ("${TEST_PATH}" STREQUAL "" AND "${LOAD_TESTS}")
    message(FATAL_ERROR "Invalid include path for extension '${NAME}' : '${INCLUDE_PATH}'")
  endif()

  set(DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_PATH ${PATH} PARENT_SCOPE)
  set(DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_INCLUDE_PATH ${INCLUDE_PATH} PARENT_SCOPE)
  set(DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_TEST_PATH ${TEST_PATH} PARENT_SCOPE)
  set(DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_EXT_VERSION ${GIT_COMMIT_HASH} PARENT_SCOPE)
endfunction()

# Downloads the external extension repo at the specified commit and calls register_extension
macro(register_external_extension NAME URL COMMIT DONT_LINK DONT_BUILD LOAD_TESTS PATH INCLUDE_PATH TEST_PATH APPLY_PATCHES LINKED_LIBS SUBMODULES)
  include(FetchContent)
  if (${APPLY_PATCHES})
    set(PATCH_COMMAND python3 ${CMAKE_SOURCE_DIR}/scripts/apply_extension_patches.py ${CMAKE_SOURCE_DIR}/.github/patches/extensions/${NAME}/)
  endif()
  FETCHCONTENT_DECLARE(
          ${NAME}_extension_fc
          GIT_REPOSITORY ${URL}
          GIT_TAG ${COMMIT}
          GIT_SUBMODULES "${SUBMODULES}"
          PATCH_COMMAND ${PATCH_COMMAND}
  )
  FETCHCONTENT_POPULATE(${NAME}_EXTENSION_FC)

  find_package(Git)
  if(Git_FOUND)
    execute_process(
        COMMAND ${GIT_EXECUTABLE} log -1 --format=%h
        WORKING_DIRECTORY "${${NAME}_extension_fc_SOURCE_DIR}"
        RESULT_VARIABLE GIT_RESULT
        OUTPUT_VARIABLE GIT_SHORT_COMMIT
        OUTPUT_STRIP_TRAILING_WHITESPACE)
  else()
    message(WARNING "Git not found, using ${COMMIT} as GIT_SHORT_COMMIT")
    set(GIT_SHORT_COMMIT "${COMMIT}")
  endif()

  message(STATUS "Load extension '${NAME}' from ${URL} @ ${GIT_SHORT_COMMIT}")

  string(TOUPPER ${NAME} EXTENSION_NAME_UPPERCASE)
  set(DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_EXT_VERSION "${GIT_SHORT_COMMIT}" PARENT_SCOPE)

  if ("${INCLUDE_PATH}" STREQUAL "")
    set(INCLUDE_FULL_PATH "${${NAME}_extension_fc_SOURCE_DIR}/src/include")
  else()
    set(INCLUDE_FULL_PATH "${${NAME}_extension_fc_SOURCE_DIR}/${INCLUDE_PATH}")
  endif()

  if ("${TEST_PATH}" STREQUAL "")
    set(TEST_FULL_PATH "${${NAME}_extension_fc_SOURCE_DIR}/test/sql")
  else()
    set(TEST_FULL_PATH "${${NAME}_extension_fc_SOURCE_DIR}/${TEST_PATH}")
  endif()

  register_extension(${NAME} ${DONT_LINK} ${DONT_BUILD} ${LOAD_TESTS} ${${NAME}_extension_fc_SOURCE_DIR}/${PATH} "${INCLUDE_FULL_PATH}" "${TEST_FULL_PATH}" "${LINKED_LIBS}")
endmacro()

function(duckdb_extension_load NAME)
  # Parameter parsing
  set(options DONT_LINK DONT_BUILD LOAD_TESTS APPLY_PATCHES)
  set(oneValueArgs SOURCE_DIR INCLUDE_DIR TEST_DIR GIT_URL GIT_TAG SUBMODULES EXTENSION_VERSION LINKED_LIBS)
  cmake_parse_arguments(duckdb_extension_load "${options}" "${oneValueArgs}" "" ${ARGN})

  string(TOLOWER ${NAME} EXTENSION_NAME_LOWERCASE)
  string(TOUPPER ${NAME} EXTENSION_NAME_UPPERCASE)

  # If extension was set already, we ignore subsequent calls
  list (FIND DUCKDB_EXTENSION_NAMES ${EXTENSION_NAME_LOWERCASE} _index)
  if (${_index} GREATER -1)
    return()
  endif()

  list (FIND SKIP_EXTENSIONS ${EXTENSION_NAME_LOWERCASE} _index)
  if (${_index} GREATER -1)
    return()
  endif()

  # Remote Git extension
  if (${duckdb_extension_load_DONT_BUILD})
    register_extension(${NAME} "${duckdb_extension_load_DONT_LINK}" "${duckdb_extension_load_DONT_BUILD}" "" "" "" "" "")
  elseif (NOT "${duckdb_extension_load_GIT_URL}" STREQUAL "")
    if ("${duckdb_extension_load_GIT_TAG}" STREQUAL "")
      error("Git URL specified but no valid GIT_TAG was found for ${NAME} extension")
    endif()
    register_external_extension(${NAME} "${duckdb_extension_load_GIT_URL}" "${duckdb_extension_load_GIT_TAG}" "${duckdb_extension_load_DONT_LINK}" "${duckdb_extension_load_DONT_BUILD}" "${duckdb_extension_load_LOAD_TESTS}" "${duckdb_extension_load_SOURCE_DIR}" "${duckdb_extension_load_INCLUDE_DIR}" "${duckdb_extension_load_TEST_DIR}" "${duckdb_extension_load_APPLY_PATCHES}" "${duckdb_extension_load_LINKED_LIBS}" "${duckdb_extension_load_SUBMODULES}")
    if (NOT "${duckdb_extension_load_EXTENSION_VERSION}" STREQUAL "")
      set(DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_EXT_VERSION "${duckdb_extension_load_EXTENSION_VERSION}" PARENT_SCOPE)
    endif()
  elseif (NOT "${duckdb_extension_load_SOURCE_DIR}" STREQUAL "")
    # Local extension, custom path
    message(STATUS "Load extension '${NAME}' from '${duckdb_extension_load_SOURCE_DIR}'")

    # If no include path specified, use default
    if ("${duckdb_extension_load_INCLUDE_DIR}" STREQUAL "")
      set(INCLUDE_PATH_DEFAULT "${duckdb_extension_load_SOURCE_DIR}/src/include")
    else()
      set(INCLUDE_PATH_DEFAULT ${duckdb_extension_load_INCLUDE_DIR})
    endif()

    # If no test path specified, use default
    if ("${duckdb_extension_load_TEST_DIR}" STREQUAL "")
      set(TEST_PATH_DEFAULT "${duckdb_extension_load_SOURCE_DIR}/test/sql")
    else()
      set(TEST_PATH_DEFAULT ${duckdb_extension_load_TEST_DIR})
    endif()

    if (NOT "${duckdb_extension_load_EXTENSION_VERSION}" STREQUAL "")
      set(DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_EXT_VERSION "${duckdb_extension_load_EXTENSION_VERSION}" PARENT_SCOPE)
    else()
      find_package(Git)
      if(Git_FOUND)
          execute_process(
            COMMAND ${GIT_EXECUTABLE} log -1 --format=%h
            WORKING_DIRECTORY ${duckdb_extension_load_SOURCE_DIR}
            RESULT_VARIABLE GIT_RESULT
            OUTPUT_VARIABLE GIT_COMMIT
            OUTPUT_STRIP_TRAILING_WHITESPACE)
        set(DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_EXT_VERSION "${GIT_COMMIT}" PARENT_SCOPE)
      else()
        set(DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_EXT_VERSION "" PARENT_SCOPE)
      endif()
    endif()
    register_extension(${NAME} "${duckdb_extension_load_DONT_LINK}" "${duckdb_extension_load_DONT_BUILD}" "${duckdb_extension_load_LOAD_TESTS}" "${duckdb_extension_load_SOURCE_DIR}" "${INCLUDE_PATH_DEFAULT}" "${TEST_PATH_DEFAULT}" "${duckdb_extension_load_LINKED_LIBS}")
  elseif(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/extension_external/${NAME})
    # Local extension, default path
    message(STATUS "Load extension '${NAME}' from '${CMAKE_CURRENT_SOURCE_DIR}/extension_external'")
    register_extension(${NAME} ${duckdb_extension_load_DONT_LINK} "${duckdb_extension_load_DONT_BUILD}" "${duckdb_extension_load_LOAD_TESTS}"  "${CMAKE_CURRENT_SOURCE_DIR}/extension_external/${NAME}" "${CMAKE_CURRENT_SOURCE_DIR}/extension_external/${NAME}/src/include" "${CMAKE_CURRENT_SOURCE_DIR}/extension_external/${NAME}/test/sql")
    set(DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_EXT_VERSION "${DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_EXT_VERSION}" PARENT_SCOPE)
  else()
    # Local extension, default path
    message(STATUS "Load extension '${NAME}' from '${CMAKE_CURRENT_SOURCE_DIR}/extensions'")
    register_extension(${NAME} ${duckdb_extension_load_DONT_LINK} "${duckdb_extension_load_DONT_BUILD}" "${duckdb_extension_load_LOAD_TESTS}" "${CMAKE_CURRENT_SOURCE_DIR}/extension/${NAME}" "${CMAKE_CURRENT_SOURCE_DIR}/extension/${NAME}/include" "${CMAKE_CURRENT_SOURCE_DIR}/extension/${NAME}/test/sql" "${duckdb_extension_load_LINKED_LIBS}")
    set(DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_EXT_VERSION "${DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_EXT_VERSION}" PARENT_SCOPE)
  endif()

  # Propagate variables set by register_extension
  set(DUCKDB_EXTENSION_NAMES ${DUCKDB_EXTENSION_NAMES} PARENT_SCOPE)
  set(DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_SHOULD_BUILD ${DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_SHOULD_BUILD} PARENT_SCOPE)
  set(DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_SHOULD_LINK ${DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_SHOULD_LINK} PARENT_SCOPE)
  set(DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_LOAD_TESTS ${DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_LOAD_TESTS} PARENT_SCOPE)
  set(DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_PATH ${DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_PATH} PARENT_SCOPE)
  set(DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_INCLUDE_PATH ${DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_INCLUDE_PATH} PARENT_SCOPE)
  set(DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_TEST_PATH ${DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_TEST_PATH} PARENT_SCOPE)
  set(DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_LINKED_LIBS ${DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_LINKED_LIBS} PARENT_SCOPE)
endfunction()

if(${EXPORT_DLL_SYMBOLS})
  # For Windows DLL export symbols
  add_definitions(-DDUCKDB_BUILD_LIBRARY)
endif()

# Log extensions that are built by directly passing cmake variables
foreach(EXT IN LISTS DUCKDB_EXTENSION_NAMES)
  if (NOT "${EXT}" STREQUAL "")
    string(TOUPPER ${EXT} EXTENSION_NAME_UPPERCASE)
    message(STATUS "Load extension '${EXT}' from '${DUCKDB_EXTENSION_${EXTENSION_NAME_UPPERCASE}_PATH}'")
  endif()
endforeach()

# Load extensions passed through cmake config var
foreach(EXT IN LISTS BUILD_EXTENSIONS)
  if(NOT "${EXT}" STREQUAL "")
    duckdb_extension_load(${EXT})
  endif()
endforeach()

# Custom extension configs passed in DUCKDB_EXTENSION_CONFIGS parameter
foreach(DUCKDB_EXTENSION_CONFIG IN LISTS DUCKDB_EXTENSION_CONFIGS)
  if (NOT "${DUCKDB_EXTENSION_CONFIG}" STREQUAL "")
    include(${DUCKDB_EXTENSION_CONFIG})
  endif()
endforeach()

# Local extension config
if (EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/extension/extension_config_local.cmake)
  include(${CMAKE_CURRENT_SOURCE_DIR}/extension/extension_config_local.cmake)
endif()

# Load client specific config
if(BUILD_PYTHON)
  include(${CMAKE_CURRENT_SOURCE_DIR}/tools/pythonpkg/duckdb_extension_config.cmake)
endif()

# Load base extension config
include(${CMAKE_CURRENT_SOURCE_DIR}/extension/extension_config.cmake)

# For extensions whose tests were loaded, but not linked into duckdb, we need to ensure they are registered to have
# the sqllogictest "require" statement load the loadable extensions instead of the baked in static one
foreach(EXT_NAME IN LISTS DUCKDB_EXTENSION_NAMES)
  string(TOUPPER ${EXT_NAME} EXT_NAME_UPPERCASE)
  if (NOT "${DUCKDB_EXTENSION_${EXT_NAME_UPPERCASE}_SHOULD_LINK}" AND "${DUCKDB_EXTENSION_${EXT_NAME_UPPERCASE}_LOAD_TESTS}")
    list(APPEND TEST_WITH_LOADABLE_EXTENSION ${EXT_NAME})
  endif()
endforeach()

if (BUILD_MAIN_DUCKDB_LIBRARY)
  add_subdirectory(src)
  add_subdirectory(tools)
endif()

# Add subdirectories for registered extensions
foreach(EXT_NAME IN LISTS DUCKDB_EXTENSION_NAMES)
  string(TOUPPER ${EXT_NAME} EXT_NAME_UPPERCASE)

  if (NOT DEFINED DUCKDB_EXTENSION_${EXT_NAME_UPPERCASE}_SHOULD_BUILD)
    set(DUCKDB_EXTENSION_${EXT_NAME_UPPERCASE}_SHOULD_BUILD TRUE)
  endif()

  # Skip explicitly disabled extensions
  if (NOT ${DUCKDB_EXTENSION_${EXT_NAME_UPPERCASE}_SHOULD_BUILD} OR ${EXTENSION_CONFIG_BUILD})
    continue()
  endif()

  # Warning for trying to load vcpkg extensions without having VCPKG_BUILD SET
  if (EXISTS "${DUCKDB_EXTENSION_${EXT_NAME_UPPERCASE}_PATH}/vcpkg.json" AND NOT DEFINED VCPKG_BUILD)
    message(WARNING "Extension '${EXT_NAME}' has a vcpkg.json, but build was not run with VCPKG. If build fails, check out VCPKG build instructions in 'duckdb/extension/README.md' or try manually installing the dependencies in ${DUCKDB_EXTENSION_${EXT_NAME_UPPERCASE}_PATH}vcpkg.json")
  endif()

  if (DEFINED DUCKDB_EXTENSION_${EXT_NAME_UPPERCASE}_PATH)
    add_subdirectory(${DUCKDB_EXTENSION_${EXT_NAME_UPPERCASE}_PATH} extension/${EXT_NAME})
  else()
    message(FATAL_ERROR "No path found for registered extension '${EXT_NAME}'")
  endif()
endforeach()

# Output the extensions that we linked into DuckDB for some nice build logs
set(LINKED_EXTENSIONS "")
set(NONLINKED_EXTENSIONS "")
set(SKIPPED_EXTENSIONS "")
set(TEST_LOADED_EXTENSIONS "")
foreach(EXT_NAME IN LISTS DUCKDB_EXTENSION_NAMES)
  string(TOUPPER ${EXT_NAME} EXT_NAME_UPPERCASE)
  if (NOT ${DUCKDB_EXTENSION_${EXT_NAME_UPPERCASE}_SHOULD_BUILD})
    list(APPEND SKIPPED_EXTENSIONS ${EXT_NAME})
  elseif (${DUCKDB_EXTENSION_${EXT_NAME_UPPERCASE}_SHOULD_LINK})
    list(APPEND LINKED_EXTENSIONS ${EXT_NAME})
  else()
    list(APPEND NONLINKED_EXTENSIONS ${EXT_NAME})
  endif()

  if (${DUCKDB_EXTENSION_${EXT_NAME_UPPERCASE}_LOAD_TESTS})
    list(APPEND TEST_LOADED_EXTENSIONS ${EXT_NAME})
  endif()
endforeach()

if(NOT "${LINKED_EXTENSIONS}" STREQUAL "")
  string(REPLACE ";"  ", " EXT_LIST_DEBUG_MESSAGE "${LINKED_EXTENSIONS}")
  message(STATUS "Extensions linked into DuckDB: [${EXT_LIST_DEBUG_MESSAGE}]")
endif()
if(NOT "${NONLINKED_EXTENSIONS}" STREQUAL "")
  string(REPLACE ";"  ", " EXT_LIST_DEBUG_MESSAGE "${NONLINKED_EXTENSIONS}")
  message(STATUS "Extensions built but not linked: [${EXT_LIST_DEBUG_MESSAGE}]")
endif()
if(NOT "${SKIPPED_EXTENSIONS}" STREQUAL "")
  string(REPLACE ";"  ", " EXT_LIST_DEBUG_MESSAGE "${SKIPPED_EXTENSIONS}")
  message(STATUS "Extensions explicitly skipped: [${EXT_LIST_DEBUG_MESSAGE}]")
endif()
if(NOT "${TEST_LOADED_EXTENSIONS}" STREQUAL "")
  string(REPLACE ";"  ", " EXT_LIST_DEBUG_MESSAGE "${TEST_LOADED_EXTENSIONS}")
  message(STATUS "Tests loaded for extensions: [${EXT_LIST_DEBUG_MESSAGE}]")
endif()

# Special build where instead of building duckdb, we produce several artifact that require parsing the
# extension config, such as a merged vcpg.json file for extension dependencies.
if(${EXTENSION_CONFIG_BUILD})
  set(VCPKG_PATHS "")
  set(VCPKG_NAMES "")
  foreach(EXT_NAME IN LISTS DUCKDB_EXTENSION_NAMES)
    string(TOUPPER ${EXT_NAME} EXT_NAME_UPPERCASE)
    if (EXISTS "${DUCKDB_EXTENSION_${EXT_NAME_UPPERCASE}_PATH}/vcpkg.json")
      list(APPEND VCPKG_NAMES ${EXT_NAME})
      list(APPEND VCPKG_PATHS ${DUCKDB_EXTENSION_${EXT_NAME_UPPERCASE}_PATH}/vcpkg.json)
    endif()
  endforeach()

  add_custom_target(
          duckdb_merge_vcpkg_manifests ALL
          COMMAND  python3 scripts/merge_vcpkg_deps.py ${VCPKG_PATHS} ${EXT_NAMES}
          WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
          COMMENT Generates a shared vcpkg manifest from the individual extensions)
  string(REPLACE ";"  ", " VCPKG_NAMES_COMMAS "${VCPKG_NAMES}")
  Message(STATUS "Combined vcpkg manifest created from extensions: ${VCPKG_NAMES_COMMAS}")

  # Write linked extensions that will be built to extensions_linked.txt
  FILE(WRITE ${CMAKE_BINARY_DIR}/extensions.txt "")
  foreach(EXT_NAME IN LISTS DUCKDB_EXTENSION_NAMES)
    string(TOUPPER ${EXT_NAME} EXT_NAME_UPPERCASE)
    if (${DUCKDB_EXTENSION_${EXT_NAME_UPPERCASE}_SHOULD_BUILD})
      FILE(APPEND ${CMAKE_BINARY_DIR}/extensions.txt "${EXT_NAME}\r")
    endif()
  endforeach()

  return()
endif()

if(BUILD_PYTHON)
  if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(ALL_COMPILE_FLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_DEBUG}")
  elseif(CMAKE_BUILD_TYPE STREQUAL "Release")
    set(ALL_COMPILE_FLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_RELEASE}")
  elseif(CMAKE_BUILD_TYPE STREQUAL "RelWithDebInfo")
    set(ALL_COMPILE_FLAGS
            "${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_RELWITHDEBINFO}")
  else()
    set(ALL_COMPILE_FLAGS "${CMAKE_CXX_FLAGS}")
  endif()

  get_target_property(duckdb_libs duckdb LINK_LIBRARIES)

  set(PIP_COMMAND
      cmake
      -E
      env
      DUCKDB_BINARY_DIR=${PROJECT_BINARY_DIR}
      DUCKDB_COMPILE_FLAGS=${ALL_COMPILE_FLAGS}
      DUCKDB_LIBS="${duckdb_libs}"
	)

  if(PYTHON_EDITABLE_BUILD)
    set(PIP_COMMAND ${PIP_COMMAND} python3 -m pip install --editable .)
  else()
    set(PIP_COMMAND ${PIP_COMMAND} python3 -m pip install .)
  endif()

  if(USER_SPACE)
    set(PIP_COMMAND ${PIP_COMMAND} --user)
  endif()
  add_custom_target(
    duckdb_python ALL
    COMMAND ${PIP_COMMAND}
    DEPENDS duckdb duckdb_static
    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/tools/pythonpkg
    COMMENT Build
    Python package)
  add_extension_dependencies(duckdb_python)
endif()

if(NOT DUCKDB_EXPLICIT_PLATFORM)
  set(VERSION_SOURCES tools/utils/test_platform.cpp)

  add_executable(duckdb_platform_binary ${VERSION_SOURCES})
  link_threads(duckdb_platform_binary)

  set_target_properties(duckdb_platform_binary PROPERTIES OUTPUT_NAME duckdb_platform_binary)
  set_target_properties(duckdb_platform_binary PROPERTIES RUNTIME_OUTPUT_DIRECTORY
                                         ${PROJECT_BINARY_DIR})
  add_custom_target(
          duckdb_platform ALL
          COMMAND duckdb_platform_binary > duckdb_platform_out || ( echo "Provide explicit DUCKDB_PLATFORM=your_target_arch to avoid build-type detection of the platform" && exit 1 )
          )
  add_dependencies(duckdb_platform duckdb_platform_binary)
else()
  add_custom_target(
          duckdb_platform ALL
          COMMAND
          ${CMAKE_COMMAND} -E echo_append \"${DUCKDB_EXPLICIT_PLATFORM}\" > duckdb_platform_out
          )
endif()

if(NOT CLANG_TIDY)
  if(${BUILD_UNITTESTS})
    add_subdirectory(test)
    if(NOT WIN32
      AND NOT SUN
      AND ${BUILD_BENCHMARKS})
      add_subdirectory(benchmark)
    endif()
  endif()
  if (NOT EXTENSION_CONFIG_BUILD)
    add_subdirectory(third_party)
  endif()
endif()

# Write the export set for build and install tree
install(EXPORT "${DUCKDB_EXPORT_SET}" DESTINATION "${INSTALL_CMAKE_DIR}")
export(EXPORT "${DUCKDB_EXPORT_SET}"
       FILE "${PROJECT_BINARY_DIR}/${DUCKDB_EXPORT_SET}.cmake")

# Only write the cmake package configuration if the templates exist
set(CMAKE_CONFIG_TEMPLATE "${CMAKE_CURRENT_SOURCE_DIR}/DuckDBConfig.cmake.in")
set(CMAKE_CONFIG_VERSION_TEMPLATE
    "${CMAKE_CURRENT_SOURCE_DIR}/DuckDBConfigVersion.cmake.in")
if(EXISTS ${CMAKE_CONFIG_TEMPLATE} AND EXISTS ${CMAKE_CONFIG_VERSION_TEMPLATE})

  # Configure cmake package config for the build tree
  set(CONF_INCLUDE_DIRS "${PROJECT_SOURCE_DIR}/src/include")
  configure_file(${CMAKE_CONFIG_TEMPLATE}
                 "${PROJECT_BINARY_DIR}/DuckDBConfig.cmake" @ONLY)

  # Configure cmake package config for the install tree
  file(RELATIVE_PATH REL_INCLUDE_DIR "${INSTALL_CMAKE_DIR}"
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
