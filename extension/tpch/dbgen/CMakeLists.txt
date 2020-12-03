include_directories(include)


add_library(dbgen OBJECT
  dbgen.cpp
  dbgen_gunk.cpp
  bm_utils.cpp
  build.cpp
  permute.cpp
  rnd.cpp
  rng64.cpp
  speed_seed.cpp
  text.cpp)
set(DBGEN_OBJECT_FILES
   ${DBGEN_OBJECT_FILES} $<TARGET_OBJECTS:dbgen>
   PARENT_SCOPE)

disable_target_warnings(dbgen)
