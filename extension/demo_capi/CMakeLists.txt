cmake_minimum_required(VERSION 2.8.12...3.29)

project(DemoCapiExtension)

include_directories(include)

# Configure extension: the minimum CAPI version where the extension still
# compiles should be chosen here
set(EXTENSION_NAME demo_capi)

set(EXTENSION_FILES add_numbers.cpp capi_demo.cpp)

# This demo targets the V1 C API. There is no unstable flavor of it any more:
# everything that used to be unstable was stabilized into v1.5.6, and the
# unstable helpers now build against the V2 API instead.
set(CAPI_MAJOR 1)
set(CAPI_MINOR 5)
set(CAPI_PATCH 6)
build_loadable_extension_capi(${EXTENSION_NAME} ${CAPI_MAJOR} ${CAPI_MINOR}
                              ${CAPI_PATCH} ${EXTENSION_FILES})
build_static_extension_capi(${EXTENSION_NAME} ${CAPI_MAJOR} ${CAPI_MINOR}
                            ${CAPI_PATCH} ${EXTENSION_FILES})
