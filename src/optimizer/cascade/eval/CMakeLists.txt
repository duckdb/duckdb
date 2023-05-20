add_library_unity(
    eval
    OBJECT
    CConstExprEvaluatorDefault.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:eval>
    PARENT_SCOPE)