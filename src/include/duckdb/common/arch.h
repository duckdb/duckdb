#pragma once

#ifdef IS_AVX2
#define SET_ARCH(x) x ## _AVX2
#elif IS_AVX512f
#define SET_ARCH(x) x ## _AVX512f
#else
#define SET_ARCH(x) x
#endif