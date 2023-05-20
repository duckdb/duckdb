add_library_unity(
    xforms
    OBJECT
    CDecorrelator.cpp
    CXform.cpp
    CXformExploration.cpp
    CXformFactory.cpp
    CXformResult.cpp
    CXformUtils.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:xforms>
    PARENT_SCOPE)