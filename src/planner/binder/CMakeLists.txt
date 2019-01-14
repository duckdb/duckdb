
add_library(duckdb_binder OBJECT bind_alter_table.cpp bind_copy.cpp bind_create_index.cpp bind_create_table.cpp bind_create_view.cpp bind_delete.cpp bind_insert.cpp bind_select.cpp bind_select_node.cpp bind_setop_node.cpp bind_update.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_binder> PARENT_SCOPE)
