CMAKE_POLICY(SET CMP0063 NEW)
add_library(hyperloglog STATIC hyperloglog.c sds.c)
