add_library_unity(
  io
  OBJECT
  CFileDescriptor.cpp
  CFileWriter.cpp
  COstream.cpp
  COStreamBasic.cpp
  COStreamFile.cpp
  COstreamString.cpp
  ioutils.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:io>
    PARENT_SCOPE)