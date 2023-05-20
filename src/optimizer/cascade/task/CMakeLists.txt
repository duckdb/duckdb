add_library_unity(
    task
    OBJECT
    CAutoSuspendAbort.cpp
    CAutoTaskProxy.cpp
    CAutoTraceFlag.cpp
    CTask.cpp
    CTaskContext.cpp
    CTaskLocalStorage.cpp
    CTaskSchedulerFifo.cpp
    CWorker.cpp
    ITask.cpp
    IWorker.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:task>
    PARENT_SCOPE)