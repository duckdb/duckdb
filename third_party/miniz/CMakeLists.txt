cmake_policy(SET CMP0063 NEW)

add_definitions(-DMINIZ_NO_ARCHIVE_APIS)
add_library(miniz STATIC miniz.c)

set_property(TARGET miniz APPEND_STRING PROPERTY LINK_FLAGS "-Wl,--exclude-libs,libgcc.a -Wl,--exclude-libs,libunwind.a")