add_library(duckdb_geo_json OBJECT json.cpp)
set(GEO_LIBRARY_FILES
    ${GEO_LIBRARY_FILES} $<TARGET_OBJECTS:duckdb_geo_json>
    PARENT_SCOPE)
