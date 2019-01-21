add_library(test_sql_storage OBJECT test_checkconstraint.cpp test_droptable.cpp test_shutdown.cpp test_storage.cpp test_store_alter.cpp test_views.cpp)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_storage> PARENT_SCOPE)
