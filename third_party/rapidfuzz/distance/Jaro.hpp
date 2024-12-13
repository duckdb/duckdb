/* SPDX-License-Identifier: MIT */
/* Copyright © 2022-present Max Bachmann */

#pragma once

#include <rapidfuzz/details/Range.hpp>
#include <rapidfuzz/distance/Jaro_impl.hpp>
#include <stdlib.h>

namespace duckdb_rapidfuzz {

template <typename InputIt1, typename InputIt2>
double jaro_distance(InputIt1 first1, InputIt1 last1, InputIt2 first2, InputIt2 last2,
                     double score_cutoff = 1.0)
{
    return detail::Jaro::distance(first1, last1, first2, last2, score_cutoff, score_cutoff);
}

template <typename Sentence1, typename Sentence2>
double jaro_distance(const Sentence1& s1, const Sentence2& s2, double score_cutoff = 1.0)
{
    return detail::Jaro::distance(s1, s2, score_cutoff, score_cutoff);
}

template <typename InputIt1, typename InputIt2>
double jaro_similarity(InputIt1 first1, InputIt1 last1, InputIt2 first2, InputIt2 last2,
                       double score_cutoff = 0.0)
{
    return detail::Jaro::similarity(first1, last1, first2, last2, score_cutoff, score_cutoff);
}

template <typename Sentence1, typename Sentence2>
double jaro_similarity(const Sentence1& s1, const Sentence2& s2, double score_cutoff = 0.0)
{
    return detail::Jaro::similarity(s1, s2, score_cutoff, score_cutoff);
}

template <typename InputIt1, typename InputIt2>
double jaro_normalized_distance(InputIt1 first1, InputIt1 last1, InputIt2 first2, InputIt2 last2,
                                double score_cutoff = 1.0)
{
    return detail::Jaro::normalized_distance(first1, last1, first2, last2, score_cutoff, score_cutoff);
}

template <typename Sentence1, typename Sentence2>
double jaro_normalized_distance(const Sentence1& s1, const Sentence2& s2, double score_cutoff = 1.0)
{
    return detail::Jaro::normalized_distance(s1, s2, score_cutoff, score_cutoff);
}

template <typename InputIt1, typename InputIt2>
double jaro_normalized_similarity(InputIt1 first1, InputIt1 last1, InputIt2 first2, InputIt2 last2,
                                  double score_cutoff = 0.0)
{
    return detail::Jaro::normalized_similarity(first1, last1, first2, last2, score_cutoff, score_cutoff);
}

template <typename Sentence1, typename Sentence2>
double jaro_normalized_similarity(const Sentence1& s1, const Sentence2& s2, double score_cutoff = 0.0)
{
    return detail::Jaro::normalized_similarity(s1, s2, score_cutoff, score_cutoff);
}

#ifdef RAPIDFUZZ_SIMD
namespace experimental {
template <int MaxLen>
struct MultiJaro : public detail::MultiSimilarityBase<MultiJaro<MaxLen>, double, 0, 1> {

private:
    friend detail::MultiSimilarityBase<MultiJaro<MaxLen>, double, 0, 1>;
    friend detail::MultiNormalizedMetricBase<MultiJaro<MaxLen>, double>;

    static_assert(MaxLen == 8 || MaxLen == 16 || MaxLen == 32 || MaxLen == 64);

    using VecType = typename std::conditional_t<
        MaxLen == 8, uint8_t,
        typename std::conditional_t<MaxLen == 16, uint16_t,
                                    typename std::conditional_t<MaxLen == 32, uint32_t, uint64_t>>>;

    constexpr static size_t get_vec_size()
    {
#    ifdef RAPIDFUZZ_AVX2
        return detail::simd_avx2::native_simd<VecType>::size;
#    else
        return detail::simd_sse2::native_simd<VecType>::size;
#    endif
    }

    constexpr static size_t get_vec_alignment()
    {
#    ifdef RAPIDFUZZ_AVX2
        return detail::simd_avx2::native_simd<VecType>::alignment;
#    else
        return detail::simd_sse2::native_simd<VecType>::alignment;
#    endif
    }

    constexpr static size_t find_block_count(size_t count)
    {
        size_t vec_size = get_vec_size();
        size_t simd_vec_count = detail::ceil_div(count, vec_size);
        return detail::ceil_div(simd_vec_count * vec_size * MaxLen, 64);
    }

public:
    MultiJaro(size_t count) : input_count(count), PM(find_block_count(count) * 64)
    {
        /* align for avx2 so we can directly load into avx2 registers */
        str_lens_size = result_count();

        str_lens = static_cast<VecType*>(
            detail::rf_aligned_alloc(get_vec_alignment(), sizeof(VecType) * str_lens_size));
        std::fill(str_lens, str_lens + str_lens_size, VecType(0));
    }

    ~MultiJaro()
    {
        detail::rf_aligned_free(str_lens);
    }

    /**
     * @brief get minimum size required for result vectors passed into
     * - distance
     * - similarity
     * - normalized_distance
     * - normalized_similarity
     *
     * @return minimum vector size
     */
    size_t result_count() const
    {
        size_t vec_size = get_vec_size();
        size_t simd_vec_count = detail::ceil_div(input_count, vec_size);
        return simd_vec_count * vec_size;
    }

    template <typename Sentence1>
    void insert(const Sentence1& s1_)
    {
        insert(detail::to_begin(s1_), detail::to_end(s1_));
    }

    template <typename InputIt1>
    void insert(InputIt1 first1, InputIt1 last1)
    {
        auto len = std::distance(first1, last1);
        int block_pos = static_cast<int>((pos * MaxLen) % 64);
        auto block = (pos * MaxLen) / 64;
        assert(len <= MaxLen);

        if (pos >= input_count) throw std::invalid_argument("out of bounds insert");

        str_lens[pos] = static_cast<VecType>(len);
        for (; first1 != last1; ++first1) {
            PM.insert(block, *first1, block_pos);
            block_pos++;
        }
        pos++;
    }

private:
    template <typename InputIt2>
    void _similarity(double* scores, size_t score_count, const detail::Range<InputIt2>& s2,
                     double score_cutoff = 0.0) const
    {
        if (score_count < result_count())
            throw std::invalid_argument("scores has to have >= result_count() elements");

        detail::Range scores_(scores, scores + score_count);
        detail::jaro_similarity_simd<VecType>(scores_, PM, str_lens, str_lens_size, s2, score_cutoff);
    }

    template <typename InputIt2>
    double maximum([[maybe_unused]] size_t s1_idx, const detail::Range<InputIt2>&) const
    {
        return 1.0;
    }

    size_t get_input_count() const noexcept
    {
        return input_count;
    }

    size_t input_count;
    size_t pos = 0;
    detail::BlockPatternMatchVector PM;
    VecType* str_lens;
    size_t str_lens_size;
};

} /* namespace experimental */
#endif /* RAPIDFUZZ_SIMD */

template <typename CharT1>
struct CachedJaro : public detail::CachedSimilarityBase<CachedJaro<CharT1>, double, 0, 1> {
    template <typename Sentence1>
    explicit CachedJaro(const Sentence1& s1_) : CachedJaro(detail::to_begin(s1_), detail::to_end(s1_))
    {}

    template <typename InputIt1>
    CachedJaro(InputIt1 first1, InputIt1 last1) : s1(first1, last1), PM(detail::Range(first1, last1))
    {}

private:
    friend detail::CachedSimilarityBase<CachedJaro<CharT1>, double, 0, 1>;
    friend detail::CachedNormalizedMetricBase<CachedJaro<CharT1>>;

    template <typename InputIt2>
    double maximum(const detail::Range<InputIt2>&) const
    {
        return 1.0;
    }

    template <typename InputIt2>
    double _similarity(const detail::Range<InputIt2>& s2, double score_cutoff,
                       [[maybe_unused]] double score_hint) const
    {
        return detail::jaro_similarity(PM, detail::Range(s1), s2, score_cutoff);
    }

    std::vector<CharT1> s1;
    detail::BlockPatternMatchVector PM;
};

template <typename Sentence1>
explicit CachedJaro(const Sentence1& s1_) -> CachedJaro<char_type<Sentence1>>;

template <typename InputIt1>
CachedJaro(InputIt1 first1, InputIt1 last1) -> CachedJaro<iter_value_t<InputIt1>>;

} // namespace duckdb_rapidfuzz
