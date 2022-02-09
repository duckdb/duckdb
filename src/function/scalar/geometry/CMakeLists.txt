include_directories(../../../../third_party/postgis/include)
include_directories(../../../../third_party/postgis)

add_library_unity(
  duckdb_func_geometry
  OBJECT
  make_point.cpp
  geometry_from_text.cpp
  geometry_from_wkb.cpp
  geometry_as_text.cpp
  geometry_get_x.cpp
  geometry_centroid.cpp
  geometry_distance.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_func_geometry>
    PARENT_SCOPE)
