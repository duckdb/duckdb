set(PARAMETERS "-warnings")

build_loadable_extension(loadable_extension_demo ${PARAMETERS}
                         loadable_extension_demo.cpp)

# why not windows?
if(NOT WIN32)
  add_definitions(-DDUCKDB_BUILD_DIRECTORY="${PROJECT_BINARY_DIR}")

  build_loadable_extension(loadable_extension_optimizer_demo ${PARAMETERS}
                           ../extension/loadable_extension_optimizer_demo.cpp)

  if(${ENABLE_UNITTEST_CPP_TESTS})
    set(TEST_EXT_OBJECTS test_remote_optimizer.cpp)

    add_library_unity(test_extensions OBJECT ${TEST_EXT_OBJECTS})
    set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_extensions>
        PARENT_SCOPE)
  endif()
endif()
