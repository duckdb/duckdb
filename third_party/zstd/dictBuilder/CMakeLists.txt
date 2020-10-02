add_library(zstd_dictbuilder OBJECT cover.c divsufsort.c fastcover.c zdict.c)
set(ZSTD_OBJECT_FILES
    ${ZSTD_OBJECT_FILES} $<TARGET_OBJECTS:zstd_dictbuilder>
    PARENT_SCOPE)

disable_target_warnings(zstd_dictbuilder)
