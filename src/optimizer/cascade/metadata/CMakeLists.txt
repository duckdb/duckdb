add_library_unity(
    metadata
    OBJECT
    CColumnDescriptor.cpp
    CIndexDescriptor.cpp
    CName.cpp
    CPartConstraint.cpp
    CTableDescriptor.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:metadata>
    PARENT_SCOPE)