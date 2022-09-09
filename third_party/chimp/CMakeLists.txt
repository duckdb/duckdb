if(POLICY CMP0063)
    cmake_policy(SET CMP0063 NEW)
endif()

add_library(chimp
	STATIC
	chimp.cpp)

target_include_directories(chimp PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>)
set_target_properties(chimp PROPERTIES EXPORT_NAME chimp)

install(TARGETS chimp
        EXPORT "${DUCKDB_EXPORT_SET}"
        LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
        ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")

disable_target_warnings(chimp)
