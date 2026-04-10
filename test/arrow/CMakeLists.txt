add_library_unity(
  test_arrow_roundtrip OBJECT arrow_test_helper.cpp arrow_roundtrip.cpp
  arrow_move_children.cpp arrow_filter_pushdown.cpp)
# GCC 13's -Wmaybe-uninitialized has a known false positive inside libstdc++'s
# <regex> (used by arrow_filter_pushdown.cpp); the warning is sensitive to
# inlining decisions in the surrounding unity-build TU. test/CMakeLists.txt
# already demotes this warning for the `unittest` executable target, but the
# compile flag does not propagate back to OBJECT libraries that are built first
# and then linked in. Apply the same demotion here.
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  target_compile_options(test_arrow_roundtrip
                         PRIVATE -Wno-error=maybe-uninitialized)
endif()
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_arrow_roundtrip>
    PARENT_SCOPE)
