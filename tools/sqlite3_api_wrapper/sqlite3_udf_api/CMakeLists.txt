# set(UDF_WRAPPER_FILES cast_sqlite.cpp)
include_directories(include)

add_library(sqlite3_udf_api OBJECT sqlite3_udf_wrapper.cpp cast_sqlite.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:sqlite3_udf_api>
    PARENT_SCOPE)
