add_library_unity(duckdb_common_serializer
                  OBJECT
                  buffered_deserializer.cpp
                  buffered_file_reader.cpp
                  buffered_file_writer.cpp
                  buffered_serializer.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES}
                     $<TARGET_OBJECTS:duckdb_common_serializer> PARENT_SCOPE)
