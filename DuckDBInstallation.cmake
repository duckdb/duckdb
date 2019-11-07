# Offer the user the choice of overriding the installation directories
set(INSTALL_LIB_DIR lib CACHE PATH "Installation directory for libraries")
set(INSTALL_BIN_DIR bin CACHE PATH "Installation directory for executables")
set(INSTALL_INCLUDE_DIR include CACHE PATH "Installation directory for header files")
if(WIN32 AND NOT CYGWIN)
    set(DEF_INSTALL_CMAKE_DIR cmake)
else()
    set(DEF_INSTALL_CMAKE_DIR lib/cmake/DuckDB)
endif()

set(INSTALL_CMAKE_DIR ${DEF_INSTALL_CMAKE_DIR} CACHE PATH "Installation directory for CMake files")

# Make relative paths absolute
foreach(p LIB BIN INCLUDE CMAKE)
    set(var INSTALL_${p}_DIR)
    if(NOT IS_ABSOLUTE "${${var}}")
        set(${var} "${CMAKE_INSTALL_PREFIX}/${${var}}")
    endif()
endforeach()

# Install the libraries
set(DUCKDB_INSTALL_TARGETS duckdb duckdb_static pg_query hyperloglog re2 miniz)
install(TARGETS ${DUCKDB_INSTALL_TARGETS}
    EXPORT DuckDBTargets
    LIBRARY DESTINATION "${INSTALL_LIB_DIR}" COMPONENT shlib
    ARCHIVE DESTINATION "${INSTALL_LIB_DIR}" COMPONENT lib
    RUNTIME DESTINATION "${INSTALL_BIN_DIR}" COMPONENT bin)
install(
    DIRECTORY "${CMAKE_SOURCE_DIR}/src/include/duckdb"
    DESTINATION "${INSTALL_INCLUDE_DIR}"
    FILES_MATCHING PATTERN "*.hpp"
)
install(
    FILES
        "${CMAKE_SOURCE_DIR}/src/include/duckdb.hpp"
        "${CMAKE_SOURCE_DIR}/src/include/duckdb.h"
    DESTINATION "${INSTALL_INCLUDE_DIR}"
)

# Add libraries to the build-tree export set
export(TARGETS ${DUCKDB_INSTALL_TARGETS} FILE "${PROJECT_BINARY_DIR}/DuckDBTargets.cmake")

# Export the package for use from the build-tree
export(PACKAGE DuckDB)

# Create the DuckDBConfig.cmake and DuckDBConfigVersion files
file(RELATIVE_PATH REL_INCLUDE_DIR
    "${INSTALL_CMAKE_DIR}" "${INSTALL_INCLUDE_DIR}")
# ... for the build tree
set(CONF_INCLUDE_DIRS "${PROJECT_SOURCE_DIR}/src/include")
configure_file(DuckDBConfig.cmake.in "${PROJECT_BINARY_DIR}/DuckDBConfig.cmake" @ONLY)
# ... for the install tree
set(CONF_INCLUDE_DIRS "\${DuckDB_CMAKE_DIR}/${REL_INCLUDE_DIR}")
configure_file(DuckDBConfig.cmake.in
  "${PROJECT_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/DuckDBConfig.cmake" @ONLY)
# ... for both
configure_file(DuckDBConfigVersion.cmake.in
  "${PROJECT_BINARY_DIR}/DuckDBConfigVersion.cmake" @ONLY)

# Install the DuckDBConfig.cmake and DuckDBConfigVersion.cmake
install(FILES
  "${PROJECT_BINARY_DIR}${CMAKE_FILES_DIRECTORY}/DuckDBConfig.cmake"
  "${PROJECT_BINARY_DIR}/DuckDBConfigVersion.cmake"
  DESTINATION "${INSTALL_CMAKE_DIR}" COMPONENT dev)

# Install the export set for use with the install-tree
install(EXPORT DuckDBTargets DESTINATION "${INSTALL_CMAKE_DIR}" COMPONENT dev)
