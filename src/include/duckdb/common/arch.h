#pragma once

#ifdef IS_AVX2
#define SET_ARCH(x)       x##_AVX2
#define SET_ARCH_FRONT(x) AVX2##x
#define INCLUDE(x) "avx2_functions.hpp"
#include INCLUDE()
#elif IS_AVX512f
#define SET_ARCH(x)       x##_AVX512f
#define SET_ARCH_FRONT(x) AVX512f##x
#define INCLUDE(x) "avx512f_functions.hpp"
#include INCLUDE()
#else
#define SET_ARCH(x)       x
#define SET_ARCH_FRONT(x) x
#endif