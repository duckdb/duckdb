
/* SPDX-License-Identifier: MIT */
/* Copyright © 2022 Max Bachmann */
#pragma once

/* RAPIDFUZZ_LTO_HACK is used to differentiate functions between different
 * translation units to avoid warnings when using lto */
#ifndef RAPIDFUZZ_EXCLUDE_SIMD
#    if __AVX2__
#        define RAPIDFUZZ_SIMD
#        define RAPIDFUZZ_AVX2
#        define RAPIDFUZZ_LTO_HACK 0
#        include <rapidfuzz/details/simd_avx2.hpp>

#    elif (defined(_M_AMD64) || defined(_M_X64)) || defined(__SSE2__)
#        define RAPIDFUZZ_SIMD
#        define RAPIDFUZZ_SSE2
#        define RAPIDFUZZ_LTO_HACK 1
#        include <rapidfuzz/details/simd_sse2.hpp>
#    endif
#endif