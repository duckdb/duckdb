# Resolves libduckdb and defines the consumer targets:
#   duckdb::duckdb   the prebuilt engine library (imported)
#   duckdb::cpp_api  the C++ API, compiled into the consumer
#
# Requires, set by the including CMakeLists:
#   DUCKDB_CPP_DIR                      directory holding duckdb_cpp.cpp and headers
#   DUCKDB_CPP_DEFAULT_DUCKDB_VERSION   the libduckdb floor version, e.g. "1.5.4"
#
# Resolution modes, mutually exclusive, never falling back to one another:
#   (nothing set)               download the floor version for this platform
#   DUCKDB_VERSION=<v|nightly>  download that version or channel instead
#   DUCKDB_PROVIDER=system      find an installed library in standard prefixes
#   DUCKDB_ROOT=<prefix>        use exactly that prefix
#
# DUCKDB_DOWNLOAD_BASE_URL overrides the download host (mirrors, air-gapped
# environments, local testing against a file:// channel).

if(NOT DUCKDB_CPP_DIR OR NOT DUCKDB_CPP_DEFAULT_DUCKDB_VERSION)
  message(
    FATAL_ERROR
      "DuckDBCppApi.cmake requires DUCKDB_CPP_DIR and DUCKDB_CPP_DEFAULT_DUCKDB_VERSION to be set"
  )
endif()

# Mode selection. Conflicting knobs are an error, not a precedence order.
if(DUCKDB_ROOT AND DUCKDB_PROVIDER)
  message(
    FATAL_ERROR
      "DUCKDB_ROOT and DUCKDB_PROVIDER are mutually exclusive; set exactly one"
  )
endif()
if(DUCKDB_VERSION AND (DUCKDB_ROOT OR DUCKDB_PROVIDER))
  message(
    FATAL_ERROR
      "DUCKDB_VERSION selects download mode; it cannot be combined with DUCKDB_ROOT or DUCKDB_PROVIDER"
  )
endif()
if(DUCKDB_PROVIDER AND NOT DUCKDB_PROVIDER STREQUAL "system")
  message(
    FATAL_ERROR
      "Unknown DUCKDB_PROVIDER \"${DUCKDB_PROVIDER}\"; the only supported value is \"system\""
  )
endif()

# A stale cache entry from a previous configure must not survive a knob change.
unset(DUCKDB_LIBRARY CACHE)

set(_duckdb_cpp_probe_needed FALSE)

if(DUCKDB_ROOT)
  # Accepted layouts: an unpacked libduckdb release zip (flat), an install
  # prefix (lib/), or a DuckDB build tree such as <checkout>/build/reldebug
  # (src/).
  find_library(
    DUCKDB_LIBRARY
    NAMES duckdb
    PATHS "${DUCKDB_ROOT}" "${DUCKDB_ROOT}/lib" "${DUCKDB_ROOT}/src"
    NO_DEFAULT_PATH)
  if(NOT DUCKDB_LIBRARY)
    message(
      FATAL_ERROR
        "DUCKDB_ROOT=${DUCKDB_ROOT}: no duckdb library found (searched ., ./lib, ./src). "
        "Expected an unpacked libduckdb release zip, an install prefix, or a DuckDB build tree."
    )
  endif()
  set(_duckdb_cpp_probe_needed TRUE)
  set(_duckdb_cpp_status "${DUCKDB_LIBRARY} (DUCKDB_ROOT)")
elseif(DUCKDB_PROVIDER STREQUAL "system")
  find_library(DUCKDB_LIBRARY NAMES duckdb)
  if(NOT DUCKDB_LIBRARY)
    message(
      FATAL_ERROR
        "DUCKDB_PROVIDER=system: no installed duckdb library found in the standard prefixes. "
        "Install one (e.g. brew install duckdb) or use DUCKDB_ROOT / download mode."
    )
  endif()
  set(_duckdb_cpp_probe_needed TRUE)
  set(_duckdb_cpp_status "${DUCKDB_LIBRARY} (system)")
else()
  # Download mode. The version was chosen by us or explicitly by the user, so
  # the floor is satisfied by construction and no probe is needed.
  set(_duckdb_cpp_version "${DUCKDB_CPP_DEFAULT_DUCKDB_VERSION}")
  if(DUCKDB_VERSION)
    set(_duckdb_cpp_version "${DUCKDB_VERSION}")
  endif()

  if(APPLE)
    set(_duckdb_cpp_platform "osx-universal")
  elseif(WIN32)
    if(CMAKE_SYSTEM_PROCESSOR MATCHES "ARM64|aarch64")
      set(_duckdb_cpp_platform "windows-arm64")
    else()
      set(_duckdb_cpp_platform "windows-amd64")
    endif()
  else()
    if(CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|arm64")
      set(_duckdb_cpp_platform "linux-arm64")
    else()
      set(_duckdb_cpp_platform "linux-amd64")
    endif()
  endif()

  set(_duckdb_cpp_base "https://github.com/duckdb/duckdb/releases/download")
  if(DUCKDB_DOWNLOAD_BASE_URL)
    set(_duckdb_cpp_base "${DUCKDB_DOWNLOAD_BASE_URL}")
  endif()
  if(_duckdb_cpp_version STREQUAL "nightly")
    set(_duckdb_cpp_url
        "${_duckdb_cpp_base}/nightly/libduckdb-${_duckdb_cpp_platform}.zip")
  else()
    set(_duckdb_cpp_url
        "${_duckdb_cpp_base}/v${_duckdb_cpp_version}/libduckdb-${_duckdb_cpp_platform}.zip"
    )
  endif()

  include(FetchContent)
  set(_duckdb_cpp_extract_ts "")
  if(CMAKE_VERSION VERSION_GREATER_EQUAL "3.24")
    set(_duckdb_cpp_extract_ts DOWNLOAD_EXTRACT_TIMESTAMP TRUE)
  endif()
  FetchContent_Declare(duckdb_lib URL "${_duckdb_cpp_url}"
                       ${_duckdb_cpp_extract_ts})
  FetchContent_MakeAvailable(duckdb_lib)

  find_library(
    DUCKDB_LIBRARY
    NAMES duckdb
    PATHS "${duckdb_lib_SOURCE_DIR}" "${duckdb_lib_SOURCE_DIR}/lib"
    NO_DEFAULT_PATH)
  if(NOT DUCKDB_LIBRARY)
    message(
      FATAL_ERROR
        "Downloaded ${_duckdb_cpp_url} but found no duckdb library in the archive"
    )
  endif()
  set(_duckdb_cpp_status
      "${_duckdb_cpp_version} (downloaded, ${_duckdb_cpp_platform})")
endif()

# For libraries we did not pick ourselves, verify the V2 C API is present.
# Once the C surface carries version metadata this becomes a real version
# comparison against the floor; until then a link probe catches pre-V2 libs.
if(_duckdb_cpp_probe_needed)
  include(CheckCXXSourceCompiles)
  set(CMAKE_REQUIRED_LIBRARIES "${DUCKDB_LIBRARY}")
  unset(DUCKDB_CPP_V2_PROBE CACHE)
  check_cxx_source_compiles(
    "extern \"C\" int duckdb_v2_library_version(char **, void *);
     int main() { char *v = 0; return duckdb_v2_library_version(&v, 0) ? 1 : 0; }"
    DUCKDB_CPP_V2_PROBE)
  unset(CMAKE_REQUIRED_LIBRARIES)
  if(NOT DUCKDB_CPP_V2_PROBE)
    message(
      FATAL_ERROR
        "${DUCKDB_LIBRARY} does not provide the DuckDB V2 C API (link probe failed). "
        "This package requires libduckdb >= ${DUCKDB_CPP_DEFAULT_DUCKDB_VERSION}."
    )
  endif()
endif()

message(STATUS "DuckDB: ${_duckdb_cpp_status}")

add_library(duckdb::duckdb UNKNOWN IMPORTED)
set_target_properties(duckdb::duckdb PROPERTIES IMPORTED_LOCATION
                                                "${DUCKDB_LIBRARY}")
add_library(duckdb_cpp_api STATIC "${DUCKDB_CPP_DIR}/duckdb_cpp.cpp")
target_include_directories(duckdb_cpp_api PUBLIC "${DUCKDB_CPP_DIR}")
target_compile_features(duckdb_cpp_api PUBLIC cxx_std_17)
target_link_libraries(duckdb_cpp_api PUBLIC duckdb::duckdb)
add_library(duckdb::cpp_api ALIAS duckdb_cpp_api)
