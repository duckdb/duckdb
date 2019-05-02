CMAKE_POLICY(SET CMP0063 NEW)

add_definitions(-DMINIZ_NO_ARCHIVE_APIS)
add_library(miniz STATIC miniz.c)
