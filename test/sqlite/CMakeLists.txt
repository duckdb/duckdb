add_extension_definitions()

add_definitions(-DDUCKDB_ROOT_DIRECTORY="${PROJECT_SOURCE_DIR}"
                -DDUCKDB_BUILD_DIRECTORY="${PROJECT_BINARY_DIR}")

set(SQLITE_TEST_RUNNER_SOURCES result_helper.cpp sqllogic_command.cpp
                               sqllogic_test_runner.cpp sqllogic_parser.cpp)

add_library_unity(test_sqlite OBJECT ${SQLITE_TEST_RUNNER_SOURCES}
                  test_sqllogictest.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sqlite>
    PARENT_SCOPE)

add_executable(sql_extractor sql_extractor.cpp ${SQLITE_TEST_RUNNER_SOURCES})
target_link_libraries(sql_extractor duckdb test_helpers)
