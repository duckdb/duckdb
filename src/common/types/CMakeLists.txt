add_library(duckdb_common_types OBJECT
            chunk_collection.cpp
            data_chunk.cpp
            date.cpp
            hash.cpp
            hyperloglog.cpp
            null_value.cpp
            string_heap.cpp
            timestamp.cpp
            time.cpp
            value.cpp
            vector_buffer.cpp
            vector.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_common_types>
    PARENT_SCOPE)
