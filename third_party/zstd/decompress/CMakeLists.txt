add_library(zstd_decompress OBJECT huf_decompress.c zstd_ddict.c zstd_decompress.c zstd_decompress_block.c)
set(ZSTD_OBJECT_FILES
    ${ZSTD_OBJECT_FILES} $<TARGET_OBJECTS:zstd_decompress>
    PARENT_SCOPE)

disable_target_warnings(zstd_decompress)
