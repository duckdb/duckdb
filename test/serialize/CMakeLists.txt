include_directories(${CMAKE_CURRENT_SOURCE_DIR})

add_library_unity(
  test_serialization
  OBJECT
  serialization_test.cpp
  serialize_test_message.cpp
  serialize_test_message_writer.cpp)
set(UNITTEST_OBJECT_FILES
    ${UNITTEST_OBJECT_FILES} $<TARGET_OBJECTS:test_serialization>
    PARENT_SCOPE)
