add_subdirectory(base)
add_subdirectory(common)
add_subdirectory(cost)
add_subdirectory(engine)
add_subdirectory(eval)
add_subdirectory(io)
add_subdirectory(operators)
add_subdirectory(optimizer)
add_subdirectory(search)
add_subdirectory(string)
add_subdirectory(task)
add_subdirectory(xforms)

add_library_unity(
  cascade_optimizer
  OBJECT
  Cascade.cpp
  utils.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:cascade_optimizer>
    PARENT_SCOPE)