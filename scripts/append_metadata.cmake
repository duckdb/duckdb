cmake_minimum_required(VERSION 3.15...3.29)

# Usage: cmake -DEXTENSION=path/to/extension.duckdb_extension -DPLATFORM_FILE=README.md -DDUCKDB_VERSION=tag1 -DEXTENSION_VERSION=tag2 -P scripts/append_metadata.cmake
# Currently hardcoded to host up to 8 fields
# Example: ./scripts/append_metadata.sh file.duckdb_extension git_hash_duckdb_file git_hash_extension_file platform_file

set(EXTENSION "" CACHE PATH "Path to the extension where to add metadata")
set(NULL_FILE "" CACHE PATH "Path to file containing a single 0 byte")
set(META1 "4" CACHE STRING "Metadata field" FORCE)
set(PLATFORM_FILE "" CACHE PATH "Metadata field: path of file containing duckdb_platform")
set(VERSION_FIELD "" CACHE STRING "Metadata field: path of file containing duckdb_version")
set(EXTENSION_VERSION "" CACHE STRING "Metadata field: path of file containing extension_version")
set(ABI_TYPE "" CACHE STRING "Metadata field: the ABI type of the extension")
set(META6 "" CACHE STRING "Metadata field")
set(META7 "" CACHE STRING "Metadata field")
set(META8 "" CACHE STRING "Metadata field")
set(SIGNATURE_SIZE "256" CACHE STRING "Size in bytes of the trailing signature slot: 256 for RSA-2048, 384 for RSA-3072")

if(NOT SIGNATURE_SIZE STREQUAL "256" AND NOT SIGNATURE_SIZE STREQUAL "384")
  message(FATAL_ERROR "SIGNATURE_SIZE must be 256 (RSA-2048) or 384 (RSA-3072), got '${SIGNATURE_SIZE}'")
endif()

# "384" marker in reserved field index 6 (reader numbering = the METADATA7 slot) selects RSA-3072; empty/"256" keeps
# the legacy RSA-2048 layout byte-for-byte identical. The marker is inside the signed region.
# META7 is reserved for this marker: it may only be empty or equal to SIGNATURE_SIZE.
if(NOT META7 STREQUAL "" AND NOT META7 STREQUAL "${SIGNATURE_SIZE}")
  message(FATAL_ERROR "META7 is reserved for the signature-size marker; it must be empty or '${SIGNATURE_SIZE}', got '${META7}'")
endif()
if(SIGNATURE_SIZE STREQUAL "384")
  set(META7 "384")
endif()

# null.txt should contain exactly 1 byte of value \x00
file(READ "${NULL_FILE}" EMPTY_BYTE)

string(REPEAT "${EMPTY_BYTE}" 32 EMPTY_32)
string(REPEAT "${EMPTY_BYTE}" ${SIGNATURE_SIZE} EMPTY_SIGNATURE)

# WASM custom-section LEB128 lengths: outer = 1+16+2+8x32+SIGNATURE_SIZE (531->659), inner = 8x32+SIGNATURE_SIZE
# (512->640). Low bytes (147, 128) are invariant; only the shared high byte (= value >> 7, same bucket for both) grows.
math(EXPR SIGNATURE_LEB_HIGH "(256 + ${SIGNATURE_SIZE}) >> 7")

# 0 for custom section
string(APPEND CUSTOM_SECTION "${EMPTY_BYTE}")
# Outer section length (LEB128 low byte 147, high byte above), then 16 = length of the name.
string(ASCII 147 ${SIGNATURE_LEB_HIGH} 16 CUSTOM_SECTION_2)
string(APPEND CUSTOM_SECTION "${CUSTOM_SECTION_2}")

# the name of the WebAssembly custom section, 16 bytes
string(APPEND CUSTOM_SECTION "duckdb_signature")

# Inner footer size (LEB128): low byte 128, high byte (256 + SIGNATURE_SIZE) >> 7, for a grand total of 2 bytes.
string(ASCII 128 ${SIGNATURE_LEB_HIGH} CUSTOM_SECTION_3)
string(APPEND CUSTOM_SECTION "${CUSTOM_SECTION_3}")

# Second metadata-field is special, since content comes from a file
file(READ "${PLATFORM_FILE}" META2)

# Build METADATA variables by appending and then truncating empty strings
string(SUBSTRING "${META1}${EMPTY_32}" 0 32 METADATA1)
string(SUBSTRING "${META2}${EMPTY_32}" 0 32 METADATA2)
string(SUBSTRING "${VERSION_FIELD}${EMPTY_32}" 0 32 METADATA3)
string(SUBSTRING "${EXTENSION_VERSION}${EMPTY_32}" 0 32 METADATA4)
string(SUBSTRING "${ABI_TYPE}${EMPTY_32}" 0 32 METADATA5)
string(SUBSTRING "${META6}${EMPTY_32}" 0 32 METADATA6)
string(SUBSTRING "${META7}${EMPTY_32}" 0 32 METADATA7)
string(SUBSTRING "${META8}${EMPTY_32}" 0 32 METADATA8)

# Append metadata fields, backwards
string(APPEND CUSTOM_SECTION "${METADATA8}")
string(APPEND CUSTOM_SECTION "${METADATA7}")
string(APPEND CUSTOM_SECTION "${METADATA6}")
string(APPEND CUSTOM_SECTION "${METADATA5}")
string(APPEND CUSTOM_SECTION "${METADATA4}")
string(APPEND CUSTOM_SECTION "${METADATA3}")
string(APPEND CUSTOM_SECTION "${METADATA2}")
string(APPEND CUSTOM_SECTION "${METADATA1}")

# Append signature (yet to be computed)
string(APPEND CUSTOM_SECTION "${EMPTY_SIGNATURE}")

# Append generated custom section to the extension
file(APPEND "${EXTENSION}" "${CUSTOM_SECTION}")
