cmake_policy(SET CMP0063 NEW)
add_library(hyperloglog STATIC hyperloglog.cpp sds.cpp)
