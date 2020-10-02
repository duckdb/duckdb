add_library(zstd_deprecated OBJECT zbuff_common.c zbuff_compress.c zbuff_decompress.c)
set(ZSTD_OBJECT_FILES
    ${ZSTD_OBJECT_FILES} $<TARGET_OBJECTS:zstd_deprecated>
    PARENT_SCOPE)

disable_target_warnings(zstd_deprecated)
