add_library_unity(
  duckdb_core_functions_date
  OBJECT
  current.cpp
  age.cpp
  date_diff.cpp
  date_sub.cpp
  to_interval.cpp
  time_bucket.cpp
  date_trunc.cpp
  epoch.cpp
  date_part.cpp
  make_date.cpp)
set(CORE_FUNCTION_FILES
    ${CORE_FUNCTION_FILES} $<TARGET_OBJECTS:duckdb_core_functions_date>
    PARENT_SCOPE)

# https://learn.microsoft.com/en-us/cpp/build/reference/ob-inline-function-expansion?view=msvc-170
# Lower functions inlining, /Ob2 causes ICE with ARM64 toolchain (both
# cross-compile and native): error MSB6006: "CL.exe" exited with code -529706956
if(MSVC
   AND (CMAKE_VS_PLATFORM_NAME STREQUAL "ARM64")
   AND (CMAKE_BUILD_TYPE STREQUAL "Release" OR CMAKE_BUILD_TYPE STREQUAL
                                               "RelWithDebInfo"))
  set_source_files_properties(ub_duckdb_core_functions_date
                              PROPERTIES COMPILE_OPTIONS /Ob1)
endif()
