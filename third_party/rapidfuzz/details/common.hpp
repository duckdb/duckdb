/* SPDX-License-Identifier: MIT */
/* Copyright © 2021 Max Bachmann */

#pragma once
#include <cstring>
#include <rapidfuzz/details/Range.hpp>
#include <rapidfuzz/details/SplittedSentenceView.hpp>
#include <rapidfuzz/details/intrinsics.hpp>
#include <rapidfuzz/details/type_traits.hpp>
#include <rapidfuzz/details/types.hpp>

#if defined(__APPLE__) && !defined(_LIBCPP_HAS_C11_FEATURES)
#    include <mm_malloc.h>
#endif

namespace duckdb_rapidfuzz::detail {

template <typename InputIt1, typename InputIt2, typename InputIt3>
struct DecomposedSet {
    SplittedSentenceView<InputIt1> difference_ab;
    SplittedSentenceView<InputIt2> difference_ba;
    SplittedSentenceView<InputIt3> intersection;
    DecomposedSet(SplittedSentenceView<InputIt1> diff_ab, SplittedSentenceView<InputIt2> diff_ba,
                  SplittedSentenceView<InputIt3> intersect)
        : difference_ab(std::move(diff_ab)),
          difference_ba(std::move(diff_ba)),
          intersection(std::move(intersect))
    {}
};

static inline size_t abs_diff(size_t a, size_t b)
{
    return a > b ? a - b : b - a;
}

template<typename TO, typename FROM>
TO opt_static_cast(const FROM &value)
{
    if constexpr (std::is_same_v<TO, FROM>)
        return value;
    else
        return static_cast<TO>(value);
}

/**
 * @defgroup Common Common
 * Common utilities shared among multiple functions
 * @{
 */

static inline double NormSim_to_NormDist(double score_cutoff, double imprecision = 0.00001)
{
    return std::min(1.0, 1.0 - score_cutoff + imprecision);
}

template <typename InputIt1, typename InputIt2>
DecomposedSet<InputIt1, InputIt2, InputIt1> set_decomposition(SplittedSentenceView<InputIt1> a,
                                                              SplittedSentenceView<InputIt2> b);

template <typename InputIt1, typename InputIt2>
StringAffix remove_common_affix(Range<InputIt1>& s1, Range<InputIt2>& s2);

template <typename InputIt1, typename InputIt2>
size_t remove_common_prefix(Range<InputIt1>& s1, Range<InputIt2>& s2);

template <typename InputIt1, typename InputIt2>
size_t remove_common_suffix(Range<InputIt1>& s1, Range<InputIt2>& s2);

template <typename InputIt, typename CharT = iter_value_t<InputIt>>
SplittedSentenceView<InputIt> sorted_split(InputIt first, InputIt last);

static inline void* rf_aligned_alloc(size_t alignment, size_t size)
{
#if defined(_WIN32)
    return _aligned_malloc(size, alignment);
#elif defined(__APPLE__) && !defined(_LIBCPP_HAS_C11_FEATURES)
    return _mm_malloc(size, alignment);
#elif defined(__ANDROID__) && __ANDROID_API__ > 16
    void* ptr = nullptr;
    return posix_memalign(&ptr, alignment, size) ? nullptr : ptr;
#else
    return aligned_alloc(alignment, size);
#endif
}

static inline void rf_aligned_free(void* ptr)
{
#if defined(_WIN32)
    _aligned_free(ptr);
#elif defined(__APPLE__) && !defined(_LIBCPP_HAS_C11_FEATURES)
    _mm_free(ptr);
#else
    free(ptr);
#endif
}

/**@}*/

} // namespace duckdb_rapidfuzz::detail

#include <rapidfuzz/details/common_impl.hpp>
