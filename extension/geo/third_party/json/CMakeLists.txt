add_library(duckdb_geo_json OBJECT json.cpp json_tokener.cpp printbuf.cpp json_object.cpp linkhash.cpp arraylist.cpp random_seed.cpp json_util.cpp)
set(GEO_LIBRARY_FILES
    ${GEO_LIBRARY_FILES} $<TARGET_OBJECTS:duckdb_geo_json>
    PARENT_SCOPE)
