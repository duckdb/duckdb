add_library(zstd_common OBJECT debug.c entropy_common.c error_private.c fse_decompress.c pool.c threading.c xxhash.c zstd_common.c)
set(ZSTD_OBJECT_FILES
    ${ZSTD_OBJECT_FILES} $<TARGET_OBJECTS:zstd_common>
    PARENT_SCOPE)

disable_target_warnings(zstd_common)
