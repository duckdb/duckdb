cmake_policy(SET CMP0063 NEW)
add_library(hyperloglog STATIC hyperloglog.c sds.c)

set_property(TARGET hyperloglog APPEND_STRING PROPERTY LINK_FLAGS "-Wl,--exclude-libs,libgcc.a -Wl,--exclude-libs,libunwind.a")