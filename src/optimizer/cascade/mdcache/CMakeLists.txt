add_library_unity(
  mdcache
  OBJECT
  CMDAccessor.cpp
  CMDAccessorUtils.cpp
  CMDKey.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:mdcache>
    PARENT_SCOPE)