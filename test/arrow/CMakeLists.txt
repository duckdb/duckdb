add_library_unity(
  test_arrow_roundtrip
  OBJECT
  arrow_test_helper.cpp
  arrow_roundtrip.cpp
  arrow_move_children.cpp
  arrow_filter_pushdown.cpp
  arrow_union_type_ids.cpp)

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  target_compile_options(test_arrow_roundtrip
                         PRIVATE -Wno-error=maybe-uninitialized)
endif()

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_arrow_roundtrip>
    PARENT_SCOPE)
