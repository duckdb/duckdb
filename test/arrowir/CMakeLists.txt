include_directories(../../extension/tpch/include)
include_directories(../../extension/)

include_directories(../../third_party/arrowir/format)
include_directories(../../third_party/arrowir/ir)
include_directories(../../third_party/flatbuffers)

add_extension_definitions()

add_library_unity(test_arrowir OBJECT test_arrowir.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:test_arrowir>
    PARENT_SCOPE)
