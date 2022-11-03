add_library(python_pandas OBJECT type.cpp, scan.cpp analyzer.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:python_pandas>
    PARENT_SCOPE)
