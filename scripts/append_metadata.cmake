cmake_minimum_required(VERSION 3.15...3.29)

# Usage: cmake -DEXTENSION=path/to/extension.duckdb_extension -DPLATFORM_FILE=README.md -DDUCKDB_VERSION=tag1 -DEXTENSION_VERSION=tag2 -P scripts/append_metadata.cmake
# Currently hardcoded to host up to 8 fields
# Example: ./scripts/append_metadata.sh file.duckdb_extension git_hash_duckdb_file git_hash_extension_file platfrom_file

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

# null.txt should contain exactly 1 byte of value \x00
file(READ "${NULL_FILE}" EMPTY_BYTE)

string(REPEAT "${EMPTY_BYTE}" 32 EMPTY_32)
string(REPEAT "${EMPTY_BYTE}" 256 EMPTY_256)

# 0 for custom section
string(APPEND CUSTOM_SECTION "${EMPTY_BYTE}")
# 213 in hex = 531 in decimal, total lenght of what follows (1 + 16 + 2 + 8x32 + 256)
# [1(continuation) + 0010011(payload) = \x93 -> 147, 0(continuation) + 10(payload) = \x04 -> 4]
# 10 in hex = 16 in decimal, lenght of name, 1 byte
string(ASCII 147 4 16 CUSTOM_SECTION_2)
string(APPEND CUSTOM_SECTION "${CUSTOM_SECTION_2}")

# the name of the WebAssembly custom section, 16 bytes
string(APPEND CUSTOM_SECTION "duckdb_signature")

# 1000 in hex, 512 in decimal
# [1(continuation) + 0000000(payload) = -> 128, 0(continuation) + 100(payload) -> 4],
# for a grand total of 2 bytes
string(ASCII 128 4 CUSTOM_SECTION_3)
string(APPEND CUSTOM_SECTION "${CUSTOM_SECTION_3}")

# Second metadata-field is special, since content comes from a file
file(READ "${PLATFORM_FILE}" META2)

# Build METADATAx variable by appending and then truncating empty strings
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
string(APPEND CUSTOM_SECTION "${EMPTY_256}")

# Append generated custom section to the extension
file(APPEND "${EXTENSION}" "${CUSTOM_SECTION}")
