add_library_unity(
    string
    OBJECT
    CStringStatic.cpp
    CWorkerPoolManager.cpp
    CWString.cpp
    CWStringBase.cpp
    CWStringConst.cpp
    CWStringDynamic.cpp
    CWStringStatic.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:string>
    PARENT_SCOPE)