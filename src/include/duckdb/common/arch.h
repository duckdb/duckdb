#pragma once

#ifdef IS_AVX2
#define SET_ARCH(x)       x##_AVX2
#define SET_ARCH_FRONT(x) AVX2##x
#include "avx2_functions.hpp"
#elif IS_AVX512f
#define SET_ARCH(x)       x##_AVX512f
#define SET_ARCH_FRONT(x) AVX512f##x
#include "avx512f_functions.hpp"
#else
#define SET_ARCH(x)       x
#define SET_ARCH_FRONT(x) x
#include "duckdb/function/functions.hpp"
#endif