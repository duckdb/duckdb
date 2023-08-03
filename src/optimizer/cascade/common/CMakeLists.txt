add_library_unity(
    common
    OBJECT
    CAutoTimer.cpp
    CHeapObject.cpp
    CStackDescriptor.cpp
    CStackObject.cpp
    CTimerUser.cpp
    CWallClock.cpp
    clibwrapper.cpp
    syslibwrapper.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:common>
    PARENT_SCOPE)
    