CMAKE_POLICY(SET CMP0063 NEW)
include_directories(include)

add_library(sqlite3 STATIC sqlite3.c)
