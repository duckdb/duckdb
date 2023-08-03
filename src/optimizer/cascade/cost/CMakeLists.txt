add_library_unity(
    cost
    OBJECT
    ICostModel.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:cost>
    PARENT_SCOPE)
    