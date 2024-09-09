cmake_minimum_required(VERSION 2.8.12...3.29)

project(DemoCapiExtension)

include_directories(include)

# Configure extension: the minimum CAPI version where the extension still
# compiles should be chosen here
set(EXTENSION_NAME demo_capi)
set(CAPI_MAJOR_VERSION 0)
set(CAPI_MINOR_VERSION 0)
set(CAPI_PATCH_VERSION 1)

# Generate semver string
set(CAPI_VERSION
    v${CAPI_MAJOR_VERSION}.${CAPI_MINOR_VERSION}.${CAPI_PATCH_VERSION})

# Set the dev version to allow using the unstable API
# add_definitions(-DDUCKDB_EXTENSION_API_VERSION_DEV)

# Set the version definitions. These prevent compilation if a function is used
# that is added in a version > CAPI_VERSION
add_definitions(-DDUCKDB_EXTENSION_API_VERSION_MAJOR=${CAPI_MAJOR_VERSION})
add_definitions(-DDUCKDB_EXTENSION_API_VERSION_MINOR=${CAPI_MINOR_VERSION})
add_definitions(-DDUCKDB_EXTENSION_API_VERSION_PATCH=${CAPI_PATCH_VERSION})
add_definitions(-DDUCKDB_EXTENSION_NAME=${EXTENSION_NAME})

# Build the loadable extension adding its footer
build_loadable_extension_capi(${EXTENSION_NAME} ${CAPI_VERSION} add_numbers.cpp
                              capi_demo.cpp)
