cmake_minimum_required(VERSION 2.8.12...3.29)

project(JSONExtension)

include_directories(include ../common/include ../../third_party/zstd/include)

add_subdirectory(json_functions)

set(JSON_EXTENSION_FILES
    buffered_json_reader.cpp
    json_extension.cpp
    json_common.cpp
    json_enums.cpp
    json_functions.cpp
    json_scan.cpp
    json_serializer.cpp
    json_deserializer.cpp
    serialize_json.cpp
    json_functions/copy_json.cpp
    json_functions/json_array_length.cpp
    json_functions/json_contains.cpp
    json_functions/json_extract.cpp
    json_functions/json_keys.cpp
    json_functions/json_merge_patch.cpp
    json_functions/json_pretty.cpp
    json_functions/json_structure.cpp
    json_functions/json_transform.cpp
    json_functions/json_create.cpp
    json_functions/json_type.cpp
    json_functions/json_valid.cpp
    json_functions/json_serialize_sql.cpp
    json_functions/json_serialize_plan.cpp
    json_functions/read_json.cpp
    json_functions/read_json_objects.cpp
    ../common/zstd_file_system.cpp
    ../../third_party/zstd/decompress/zstd_ddict.cpp
    ../../third_party/zstd/decompress/huf_decompress.cpp
    ../../third_party/zstd/decompress/zstd_decompress.cpp
    ../../third_party/zstd/decompress/zstd_decompress_block.cpp
    ../../third_party/zstd/common/entropy_common.cpp
    ../../third_party/zstd/common/fse_decompress.cpp
    ../../third_party/zstd/common/zstd_common.cpp
    ../../third_party/zstd/common/error_private.cpp
    ../../third_party/zstd/common/xxhash.cpp
    ../../third_party/zstd/compress/fse_compress.cpp
    ../../third_party/zstd/compress/hist.cpp
    ../../third_party/zstd/compress/huf_compress.cpp
    ../../third_party/zstd/compress/zstd_compress.cpp
    ../../third_party/zstd/compress/zstd_compress_literals.cpp
    ../../third_party/zstd/compress/zstd_compress_sequences.cpp
    ../../third_party/zstd/compress/zstd_compress_superblock.cpp
    ../../third_party/zstd/compress/zstd_double_fast.cpp
    ../../third_party/zstd/compress/zstd_fast.cpp
    ../../third_party/zstd/compress/zstd_lazy.cpp
    ../../third_party/zstd/compress/zstd_ldm.cpp
    ../../third_party/zstd/compress/zstd_opt.cpp)

build_static_extension(json ${JSON_EXTENSION_FILES})
set(PARAMETERS "-warnings")
build_loadable_extension(json ${PARAMETERS} ${JSON_EXTENSION_FILES})
target_link_libraries(json_loadable_extension duckdb_yyjson)

install(
  TARGETS json_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
