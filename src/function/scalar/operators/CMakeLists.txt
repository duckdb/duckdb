if(${ARCH} STREQUAL "x86_64")
add_library(
        avx2
        OBJECT
        add.cpp
        arithmetic.cpp
        bitwise.cpp
        multiply.cpp
        subtract.cpp
        operators.cpp)

target_compile_definitions(avx2 PRIVATE IS_AVX2)
set_target_properties (avx2 PROPERTIES COMPILE_FLAGS "-mavx2")

    LIST(APPEND ALL_OBJECT_FILES
         ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:avx2>)

add_library(
        avx512
        OBJECT
        add.cpp
        arithmetic.cpp
        bitwise.cpp
        multiply.cpp
        subtract.cpp
        operators.cpp)

target_compile_definitions(avx512 PRIVATE IS_AVX512f)
set_target_properties (avx512 PROPERTIES COMPILE_FLAGS "-mavx512f")

LIST(APPEND ALL_OBJECT_FILES
     ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:avx512>)


elseif(${CMAKE_TARGET_ARCHITECTURES} STREQUAL "arm")
endif()


add_library(
        duckdb_func_ops
        OBJECT
        add.cpp
        arithmetic.cpp
        bitwise.cpp
        multiply.cpp
        subtract.cpp
        operators.cpp)

set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:duckdb_func_ops>
    PARENT_SCOPE)

