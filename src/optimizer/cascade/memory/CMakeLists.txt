add_library_unity(
  memory
  OBJECT
  CAutoMemoryPool.cpp
  CMemoryPool.cpp
  CMemoryPoolManager.cpp
  CMemoryPoolTracker.cpp
  CMemoryVisitorPrint.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:memory>
    PARENT_SCOPE)