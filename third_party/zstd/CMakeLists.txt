if(POLICY CMP0063)
    cmake_policy(SET CMP0063 NEW)
endif()

set(ZSTD_FILES
	compress/zstd_compress_superblock.c
	compress/zstdmt_compress.c
	compress/zstd_double_fast.c
	compress/zstd_fast.c
	compress/zstd_compress_sequences.c
	compress/zstd_ldm.c
	compress/hist.c
	compress/zstd_compress.c
	compress/zstd_lazy.c
	compress/zstd_compress_literals.c
	compress/huf_compress.c
	compress/zstd_opt.c
	compress/fse_compress.c
	decompress/zstd_ddict.c
	decompress/huf_decompress.c
	decompress/zstd_decompress.c
	decompress/huf_decompress_amd64.S
	decompress/zstd_decompress_block.c
	#legacy/zstd_v05.c
	#legacy/zstd_v01.c
	#legacy/zstd_v06.c
	#legacy/zstd_v02.c
	#legacy/zstd_v07.c
	#legacy/zstd_v03.c
	#legacy/zstd_v04.c
	common/entropy_common.c
	common/fse_decompress.c
	common/debug.c
	common/xxhash.c
	common/pool.c
	common/threading.c
	common/zstd_common.c
	common/error_private.c
	dict/cover.c
	dict/divsufsort.c
	dict/fastcover.c
	dict/zdict.c
	deprecated/zbuff_common.c
	deprecated/zbuff_decompress.c
	deprecated/zbuff_compress.c
)

add_library(duckdb_zstd STATIC ${ZSTD_FILES})

target_include_directories(
  duckdb_zstd
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>)
set_target_properties(duckdb_zstd PROPERTIES EXPORT_NAME duckdb_duckdb_zstd)

install(TARGETS duckdb_zstd
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

disable_target_warnings(duckdb_zstd)
