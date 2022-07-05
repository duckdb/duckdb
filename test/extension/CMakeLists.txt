build_loadable_extension_directory(loadable_extension_demo test/extension
                                   loadable_extension_demo.cpp)

set(DUCKDB_TEST_ALIAS_UNITS test_alias_extension.cpp)

add_library(test_alias STATIC ${DUCKDB_TEST_ALIAS_UNITS})

if(NOT WIN32)
  target_link_libraries(test_alias duckdb)
endif()
