add_library(zstd_compress OBJECT fse_compress.c hist.c huf_compress.c zstd_compress.c zstd_compress_literals.c zstd_compress_sequences.c zstd_compress_superblock.c zstd_double_fast.c zstd_fast.c zstd_lazy.c zstd_ldm.c zstd_opt.c zstdmt_compress.c)
set(ZSTD_OBJECT_FILES
    ${ZSTD_OBJECT_FILES} $<TARGET_OBJECTS:zstd_compress>
    PARENT_SCOPE)

disable_target_warnings(zstd_compress)
