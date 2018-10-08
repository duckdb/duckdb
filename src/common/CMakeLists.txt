
add_subdirectory(types)

add_library(duckdb_common OBJECT file_system.cpp internal_types.cpp printable.cpp serializer.cpp string_util.cpp symbols.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_common> PARENT_SCOPE)
