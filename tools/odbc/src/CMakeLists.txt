add_subdirectory(common)
add_subdirectory(connect)
add_subdirectory(statement)

add_library(
  odbc_src OBJECT
  driver.cpp
  prepared.cpp
  result.cpp
  api_info.cpp
  empty_stubs.cpp
  descriptor.cpp
  parameter_descriptor.cpp
  row_descriptor.cpp
  cursor.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:odbc_src>
    PARENT_SCOPE)
