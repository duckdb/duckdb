if(POLICY CMP0063)
    cmake_policy(SET CMP0063 NEW)
endif()

include_directories(include)

add_library(sqlite3 STATIC sqlite3.c)
TARGET_LINK_LIBRARIES(sqlite3 pthread)

