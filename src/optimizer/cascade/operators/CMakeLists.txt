add_library_unity(
    operators
    OBJECT
    CExpressionHandle.cpp
    CPattern.cpp
    CPatternLeaf.cpp
    Operator.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:operators>
    PARENT_SCOPE)