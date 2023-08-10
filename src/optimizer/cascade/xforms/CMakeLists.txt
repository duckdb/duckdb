add_library_unity(
    xforms
    OBJECT
    CXform.cpp
    CXformExploration.cpp
    CXformFactory.cpp
    CXformFilterImplementation.cpp
    CXformGet2TableScan.cpp
    CXformImplementation.cpp
    CXformLogicalProj2PhysicalProj.cpp
    CXformOrderImplementation.cpp
    CXformResult.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:xforms>
    PARENT_SCOPE)
