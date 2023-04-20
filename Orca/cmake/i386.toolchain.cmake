set(GPOS_ARCH_BITS "32")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -m32" CACHE STRING "c++ flags")
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -m32" CACHE STRING "c flags")

set(FORCED_CMAKE_SYSTEM_PROCESSOR "i386")

# Older GCC versions give linking errors when using certain builtins without
# explicitly setting a 32-bit -march. Tell the root CMakeLists file to set that
# flag if needed.
set(ENABLE_OLD_GCC_32BIT_MARCH 1)
