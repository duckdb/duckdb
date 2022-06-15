include_directories(../../../third_party/httplib/)

if(${EXIT_TIME_DESTRUCTORS_WARNING})
  set(CMAKE_CXX_FLAGS_DEBUG
      "${CMAKE_CXX_FLAGS_DEBUG} -Wno-exit-time-destructors")
endif()
add_library_unity(duckdb_main_extension OBJECT extension_helper.cpp
                  extension_install.cpp extension_load.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_main_extension>
    PARENT_SCOPE)

add_extension_definitions(duckdb_main_extension)
