include_directories(include)

add_library(ssbgen OBJECT
    bcd2.c
    bm_utils.c
    build.c
    driver.cpp
    load_stub.c
    permute.c
    print.c
    rnd.c
    speed_seed.c
    text.c
    varsub.c
)

set(SSBGEN_OBJECT_FILES
   ${SSBGEN_OBJECT_FILES} $<TARGET_OBJECTS:ssbgen>
   PARENT_SCOPE)

disable_target_warnings(ssbgen)