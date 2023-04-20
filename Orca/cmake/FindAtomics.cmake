# Copyright (c) 2015, Pivotal Software, Inc.

# CMake module to find compiler built-ins for atomic operations.

# Test for GCC-style builtins for fetch-add and compare-and-swap (clang and ICC
# also have these). C++11 and C11 do this in an elegant standardized way, but
# we can not be assured of a C++11 compiler.
include(CheckCXXSourceCompiles)
CHECK_CXX_SOURCE_COMPILES("
#include <stdint.h>

int main() {
  uint32_t value = 0;
  uint32_t increment = 10;
  uint32_t prev = __sync_fetch_and_add(&value, increment);

  return static_cast<int>(prev);
}
" GPOS_GCC_FETCH_ADD_32)

CHECK_CXX_SOURCE_COMPILES("
#include <stdint.h>

int main() {
  uint64_t value = 0;
  uint64_t increment = 10;
  uint64_t prev = __sync_fetch_and_add(&value, increment);

  return static_cast<int>(prev);
}
" GPOS_GCC_FETCH_ADD_64)

CHECK_CXX_SOURCE_COMPILES("
#include <stdint.h>

int main() {
  uint32_t value = 0;
  uint32_t expected_value = 0;
  uint32_t new_value = 10;
  bool success = __sync_bool_compare_and_swap(&value, expected_value, new_value);

  return success ? 0 : 1;
}
" GPOS_GCC_CAS_32)

CHECK_CXX_SOURCE_COMPILES("
#include <stdint.h>

int main() {
  uint64_t value = 0;
  uint64_t expected_value = 0;
  uint64_t new_value = 10;
  bool success = __sync_bool_compare_and_swap(&value, expected_value, new_value);

  return success ? 0 : 1;
}
" GPOS_GCC_CAS_64)
