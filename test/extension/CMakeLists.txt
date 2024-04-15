set(PARAMETERS "-warnings")

build_loadable_extension_directory(
  loadable_extension_demo test/extension "default-version" ${PARAMETERS}
  loadable_extension_demo.cpp)

# Note: this is a slightly hacky method to be able to test extension updating in
# CI. This produces a binary that is actually not directly usable; the extension
# name we give it here is only the CMake target name and filename not the name
# the extension's symbols actually has. This means we can simply copy & rename
# this binary to "update" the binary to the default-version-2
build_loadable_extension_directory(
  loadable_extension_demo_2 test/extension "default-version-2" ${PARAMETERS}
  loadable_extension_demo.cpp)

if(NOT WIN32 AND NOT SUN)
  add_definitions(-DDUCKDB_BUILD_DIRECTORY="${PROJECT_BINARY_DIR}")

  build_loadable_extension_directory(
    loadable_extension_optimizer_demo test/extension "default-version"
    ${PARAMETERS} ../extension/loadable_extension_optimizer_demo.cpp)

  set(TEST_EXT_OBJECTS test_remote_optimizer.cpp)

  add_library_unity(test_extensions OBJECT ${TEST_EXT_OBJECTS})
  set(ALL_OBJECT_FILES
      ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_extensions>
      PARENT_SCOPE)
endif()
