include_directories(include)

add_library(postgis_json OBJECT
            json.cpp
            json_object.cpp
            arraylist.cpp
            printbuf.cpp
            json_tokener.cpp
            linkhash.cpp
            json_util.cpp
            random_seed.cpp
            )

set(POSTGIS_OBJECT_FILES
    ${POSTGIS_OBJECT_FILES} $<TARGET_OBJECTS:postgis_json>
    PARENT_SCOPE)

target_include_directories(postgis_json PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>)

disable_target_warnings(postgis_json)