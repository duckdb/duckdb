include_directories(include)

add_library(
  numformat OBJECT
  calendar.cpp
  datetime.cxx
  localedata.cpp
  tdate.cxx
  ttime.cxx
  zforfind.cxx
  zformat.cxx
  zforscan.cxx)

set(NUMFORMAT_OBJECT_FILES
    ${NUMFORMAT_OBJECT_FILES} $<TARGET_OBJECTS:numformat>
    PARENT_SCOPE)

disable_target_warnings(numformat)
