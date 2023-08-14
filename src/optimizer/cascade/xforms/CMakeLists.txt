add_library_unity(
        xforms
        OBJECT
        CXform.cpp
        CXformDummyScanImplementation.cpp
        CXformExploration.cpp
        CXformFactory.cpp
        CXformFilterImplementation.cpp
        CXformGet2TableScan.cpp
        CXformImplementation.cpp
        CXformInnerJoin2HashJoin.cpp
        CXformJoinCommutativity.cpp
        CXformLogicalProj2PhysicalProj.cpp
        CXformOrderImplementation.cpp
        CXformResult.cpp)
set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:xforms>
        PARENT_SCOPE)
