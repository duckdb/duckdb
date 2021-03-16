add_library(avx512f OBJECT
            ../../../../src/function/scalar/operators/add.cpp
            ../../../../src/function/scalar/operators/arithmetic.cpp
            ../../../../src/function/scalar/operators/bitwise.cpp
            ../../../../src/function/scalar/operators/multiply.cpp
            ../../../../src/function/scalar/operators/subtract.cpp
            ../../../../src/function/scalar/operators/operators.cpp)

target_compile_definitions(avx512f PRIVATE IS_AVX512f)
set_target_properties(avx512f PROPERTIES COMPILE_FLAGS "-mavx512f")

include_directories(include)

SET(SIMD_OBJECT_FILES ${SIMD_OBJECT_FILES} $<TARGET_OBJECTS:avx512f> PARENT_SCOPE)