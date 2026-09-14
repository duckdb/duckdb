add_library_unity(test_sql_export OBJECT bound_expression_sql_export.cpp
                  logical_plan_sql_export.cpp sql_export_verification.cpp)

set(UNITTEST_OBJECT_FILES
    ${UNITTEST_OBJECT_FILES} $<TARGET_OBJECTS:test_sql_export>
    PARENT_SCOPE)
