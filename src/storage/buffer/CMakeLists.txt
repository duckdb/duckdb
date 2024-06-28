include(CheckFunctionExists)
include(CheckCSourceRuns)

check_function_exists(sched_getcpu HAVE_SCHED_GETCPU)
if(HAVE_SCHED_GETCPU)
  add_definitions(-DDUCKDB_HAVE_SCHED_GETCPU)
endif()

check_c_source_runs(
  "
#include <stdint.h>
int main() {
  unsigned int dx;
  asm volatile(\"rdtscp\" : \"=d\"(dx) ::);
  return 0;
}
"
  HAVE_RDTSCP)
if(HAVE_RDTSCP)
  add_definitions(-DDUCKDB_HAVE_RDTSCP)
endif()

add_library_unity(
  duckdb_storage_buffer
  OBJECT
  buffer_handle.cpp
  block_handle.cpp
  block_manager.cpp
  buffer_pool.cpp
  buffer_pool_reservation.cpp)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_storage_buffer>
    PARENT_SCOPE)
