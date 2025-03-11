cmake_minimum_required(VERSION 2.8.12...3.29)

project(DemoCapiExtension)

include_directories(include)

# Configure extension: the minimum CAPI version where the extension still
# compiles should be chosen here
set(EXTENSION_NAME demo_capi)

set(EXTENSION_FILES add_numbers.cpp capi_demo.cpp)

option(
  USE_UNSTABLE_C_API
  "Use the unstable C Extension API (extension will be tied to exact DuckDB version)"
  TRUE)

if(USE_UNSTABLE_C_API)
  build_loadable_extension_capi_unstable(${EXTENSION_NAME} ${EXTENSION_FILES})
else()
  # Minimum supported DuckDB version
  set(CAPI_MAJOR 1)
  set(CAPI_MINOR 2)
  set(CAPI_PATCH 0)
  build_loadable_extension_capi(${EXTENSION_NAME} ${CAPI_MAJOR} ${CAPI_MINOR}
                                ${CAPI_PATCH} ${EXTENSION_FILES})
endif()
