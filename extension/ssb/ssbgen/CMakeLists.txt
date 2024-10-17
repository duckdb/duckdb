include_directories(include)

add_library(ssbgen OBJECT
    bcd2.c
    bm_utils.c
    build.c
    driver.cpp
    load_stub.cpp
    permute.c
    rnd.cpp
    print.cpp
    speed_seed.cpp
    text.c
    varsub.c
)

set(SSBGEN_OBJECT_FILES
   ${SSBGEN_OBJECT_FILES} $<TARGET_OBJECTS:ssbgen>
   PARENT_SCOPE)

disable_target_warnings(ssbgen)