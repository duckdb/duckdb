add_library_unity(
  engine
  OBJECT
  CEngine.cpp
  CEnumeratorConfig.cpp
  CPartialPlan.cpp
  CStatisticsConfig.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:engine>
    PARENT_SCOPE)