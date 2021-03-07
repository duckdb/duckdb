#pragma once

#ifdef AVX2
#define SET_ARCH(x) x ## _AVX2
#elif AVX512f
#define SET_ARCH(x) x ## _AVX512f
#else
#define SET_ARCH(x) x
#endif