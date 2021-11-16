if(POLICY CMP0063)
    cmake_policy(SET CMP0063 NEW)
endif()

add_library(lemire STATIC bitpacking.cpp bitpackingaligned.cpp bitpackingunaligned.cpp)

target_include_directories(
  lemire
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>)
set_target_properties(lemire PROPERTIES EXPORT_NAME duckdb_lemire)

install(TARGETS lemire
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

disable_target_warnings(lemire)