add_library(avx2 OBJECT
            ../../../../src/function/scalar/operators/add.cpp
            ../../../../src/function/scalar/operators/arithmetic.cpp
            ../../../../src/function/scalar/operators/bitwise.cpp
            ../../../../src/function/scalar/operators/multiply.cpp
            ../../../../src/function/scalar/operators/subtract.cpp
            ../../../../src/function/scalar/operators/operators.cpp)

target_compile_definitions(avx2 PRIVATE IS_AVX2)
set_target_properties(avx2 PROPERTIES COMPILE_FLAGS "-mavx2")

include_directories(include)

set(SIMD_OBJECT_FILES ${SIMD_OBJECT_FILES} $<TARGET_OBJECTS:avx2> PARENT_SCOPE)