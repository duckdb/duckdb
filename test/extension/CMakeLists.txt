add_library(loadable_extension_demo SHARED loadable_extension_demo.cpp)
set_target_properties(loadable_extension_demo PROPERTIES PREFIX "")
target_link_libraries(loadable_extension_demo duckdb ${DUCKDB_EXTRA_LINK_FLAGS})
set_target_properties(loadable_extension_demo PROPERTIES SUFFIX ".so")
