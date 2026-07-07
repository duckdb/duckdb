set(PARAMETERS "-warnings")

set(DEBUG_FS_EXTENSION_FILES
    debug_fs/debug_fs_extension.cpp debug_fs/debug_file_system.cpp
    debug_fs/io_latency_model.cpp)
set(DEBUG_FS_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/debug_fs/include")

build_loadable_extension_directory(
  debug_fs
  "CPP"
  test/extension
  "default-version"
  ""
  ${PARAMETERS}
  ${DEBUG_FS_EXTENSION_FILES})
target_include_directories(debug_fs_loadable_extension
                           PRIVATE ${DEBUG_FS_INCLUDE_DIR})

add_library(test_debug_fs_extension OBJECT ${DEBUG_FS_EXTENSION_FILES})
target_include_directories(test_debug_fs_extension
                           PRIVATE ${DEBUG_FS_INCLUDE_DIR})
list(APPEND UNITTEST_OBJECT_FILES $<TARGET_OBJECTS:test_debug_fs_extension>)
set(UNITTEST_OBJECT_FILES
    ${UNITTEST_OBJECT_FILES}
    PARENT_SCOPE)

build_loadable_extension_directory(
  loadable_extension_demo
  "CPP"
  test/extension
  "default-version"
  ""
  ${PARAMETERS}
  loadable_extension_demo.cpp)

build_loadable_extension_directory(
  explicit_extension_schema
  "CPP"
  test/extension
  "default-version"
  ""
  ${PARAMETERS}
  explicit_extension_schema.cpp)

build_loadable_extension_directory(
  explicit_extension_schema_alias
  "CPP"
  test/extension
  "default-version"
  ""
  ${PARAMETERS}
  explicit_extension_schema_alias.cpp)

build_loadable_extension_directory(
  loadable_extension_demo_double
  "CPP"
  test/extension
  "default-version"
  ""
  ${PARAMETERS}
  loadable_extension_demo_alt.cpp)

if(NOT WIN32 AND NOT SUN)
  add_definitions(-DDUCKDB_BUILD_DIRECTORY="${PROJECT_BINARY_DIR}")

  build_loadable_extension_directory(
    loadable_extension_optimizer_demo
    "CPP"
    test/extension
    "default-version"
    ""
    ${PARAMETERS}
    ../extension/loadable_extension_optimizer_demo.cpp)

  if(${ENABLE_UNITTEST_CPP_TESTS} AND NOT (${ENABLE_THREAD_SANITIZER}))
    set(TEST_EXT_OBJECTS test_remote_optimizer.cpp)

    add_library_unity(test_extensions OBJECT ${TEST_EXT_OBJECTS})
    set(UNITTEST_OBJECT_FILES
        ${UNITTEST_OBJECT_FILES} $<TARGET_OBJECTS:test_extensions>
        PARENT_SCOPE)
  endif()
endif()
