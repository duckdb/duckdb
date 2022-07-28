# Define our host system
SET(CMAKE_SYSTEM_NAME Linux)
set(CMAKE_SYSTEM_PROCESSOR aarch64)
SET(CMAKE_SYSTEM_VERSION 1)

if(NOT DUCKDB_AARCH64_TOOLCHAIN_GCC_VERSION)
    set(DUCKDB_AARCH64_TOOLCHAIN_GCC_VERSION 8)
endif()

# Define the cross compiler locations
set(CMAKE_C_COMPILER "/usr/bin/aarch64-linux-gnu-gcc-${DUCKDB_AARCH64_TOOLCHAIN_GCC_VERSION}")
set(CMAKE_CXX_COMPILER "/usr/bin/aarch64-linux-gnu-g++-${DUCKDB_AARCH64_TOOLCHAIN_GCC_VERSION}")

# Define the sysroot path for the RaspberryPi distribution in our tools folder
SET(CMAKE_FIND_ROOT_PATH /usr/aarch64-linux-gnu)
# Use our definitions for compiler tools
SET(CMAKE_FIND_ROOT_PATH_MODE_PROGRAM NEVER)
# Search for libraries and headers in the target directories only
SET(CMAKE_FIND_ROOT_PATH_MODE_LIBRARY ONLY)
SET(CMAKE_FIND_ROOT_PATH_MODE_INCLUDE ONLY)

SET(DUCKDB_EXTRA_LINK_FLAGS -lstdc++ -lgcc -lm)