add_library_unity(
    task
    OBJECT
    CAutoTaskProxy.cpp
    CTask.cpp
    CTaskLocalStorage.cpp
    CTaskSchedulerFifo.cpp
    CWorker.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:task>
    PARENT_SCOPE)