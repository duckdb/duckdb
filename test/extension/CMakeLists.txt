build_loadable_extension_directory(loadable_extension_demo test/extension
                                   loadable_extension_demo.cpp)

build_loadable_extension_directory(
  loadable_extension_optimizer_demo test/extension
  loadable_extension_optimizer_demo.cpp)

set(TEST_EXTENSION_OBJECTS test_remote_optimizer.cpp)

add_definitions(-DDUCKDB_BUILD_DIRECTORY="${PROJECT_BINARY_DIR}")

add_library_unity(test_extension OBJECT ${TEST_EXTENSION_OBJECTS})
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_extension>
    PARENT_SCOPE)
