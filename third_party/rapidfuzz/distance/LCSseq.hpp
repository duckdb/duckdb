/* SPDX-License-Identifier: MIT */
/* Copyright © 2022-present Max Bachmann */

#pragma once
#include <rapidfuzz/distance/LCSseq_impl.hpp>

#include <algorithm>
#include <limits>

namespace duckdb_rapidfuzz {

template <typename InputIt1, typename InputIt2>
size_t lcs_seq_distance(InputIt1 first1, InputIt1 last1, InputIt2 first2, InputIt2 last2,
                        size_t score_cutoff = std::numeric_limits<size_t>::max())
{
    return detail::LCSseq::distance(first1, last1, first2, last2, score_cutoff, score_cutoff);
}

template <typename Sentence1, typename Sentence2>
size_t lcs_seq_distance(const Sentence1& s1, const Sentence2& s2,
                        size_t score_cutoff = std::numeric_limits<size_t>::max())
{
    return detail::LCSseq::distance(s1, s2, score_cutoff, score_cutoff);
}

template <typename InputIt1, typename InputIt2>
size_t lcs_seq_similarity(InputIt1 first1, InputIt1 last1, InputIt2 first2, InputIt2 last2,
                          size_t score_cutoff = 0)
{
    return detail::LCSseq::similarity(first1, last1, first2, last2, score_cutoff, score_cutoff);
}

template <typename Sentence1, typename Sentence2>
size_t lcs_seq_similarity(const Sentence1& s1, const Sentence2& s2, size_t score_cutoff = 0)
{
    return detail::LCSseq::similarity(s1, s2, score_cutoff, score_cutoff);
}

template <typename InputIt1, typename InputIt2>
double lcs_seq_normalized_distance(InputIt1 first1, InputIt1 last1, InputIt2 first2, InputIt2 last2,
                                   double score_cutoff = 1.0)
{
    return detail::LCSseq::normalized_distance(first1, last1, first2, last2, score_cutoff, score_cutoff);
}

template <typename Sentence1, typename Sentence2>
double lcs_seq_normalized_distance(const Sentence1& s1, const Sentence2& s2, double score_cutoff = 1.0)
{
    return detail::LCSseq::normalized_distance(s1, s2, score_cutoff, score_cutoff);
}

template <typename InputIt1, typename InputIt2>
double lcs_seq_normalized_similarity(InputIt1 first1, InputIt1 last1, InputIt2 first2, InputIt2 last2,
                                     double score_cutoff = 0.0)
{
    return detail::LCSseq::normalized_similarity(first1, last1, first2, last2, score_cutoff, score_cutoff);
}

template <typename Sentence1, typename Sentence2>
double lcs_seq_normalized_similarity(const Sentence1& s1, const Sentence2& s2, double score_cutoff = 0.0)
{
    return detail::LCSseq::normalized_similarity(s1, s2, score_cutoff, score_cutoff);
}

template <typename InputIt1, typename InputIt2>
Editops lcs_seq_editops(InputIt1 first1, InputIt1 last1, InputIt2 first2, InputIt2 last2)
{
    return detail::lcs_seq_editops(detail::Range(first1, last1), detail::Range(first2, last2));
}

template <typename Sentence1, typename Sentence2>
Editops lcs_seq_editops(const Sentence1& s1, const Sentence2& s2)
{
    return detail::lcs_seq_editops(detail::Range(s1), detail::Range(s2));
}

#ifdef RAPIDFUZZ_SIMD
namespace experimental {
template <int MaxLen>
struct MultiLCSseq : public detail::MultiSimilarityBase<MultiLCSseq<MaxLen>, size_t, 0,
                                                        std::numeric_limits<int64_t>::max()> {
private:
    friend detail::MultiSimilarityBase<MultiLCSseq<MaxLen>, size_t, 0, std::numeric_limits<int64_t>::max()>;
    friend detail::MultiNormalizedMetricBase<MultiLCSseq<MaxLen>, size_t>;

    constexpr static size_t get_vec_size()
    {
#    ifdef RAPIDFUZZ_AVX2
        using namespace detail::simd_avx2;
#    else
        using namespace detail::simd_sse2;
#    endif
        if constexpr (MaxLen <= 8)
            return native_simd<uint8_t>::size;
        else if constexpr (MaxLen <= 16)
            return native_simd<uint16_t>::size;
        else if constexpr (MaxLen <= 32)
            return native_simd<uint32_t>::size;
        else if constexpr (MaxLen <= 64)
            return native_simd<uint64_t>::size;

        static_assert(MaxLen <= 64);
    }

    constexpr static size_t find_block_count(size_t count)
    {
        size_t vec_size = get_vec_size();
        size_t simd_vec_count = detail::ceil_div(count, vec_size);
        return detail::ceil_div(simd_vec_count * vec_size * MaxLen, 64);
    }

public:
    MultiLCSseq(size_t count) : input_count(count), pos(0), PM(find_block_count(count) * 64)
    {
        str_lens.resize(result_count());
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

        str_lens[pos] = static_cast<size_t>(len);

        for (; first1 != last1; ++first1) {
            PM.insert(block, *first1, block_pos);
            block_pos++;
        }
        pos++;
    }

private:
    template <typename InputIt2>
    void _similarity(size_t* scores, size_t score_count, const detail::Range<InputIt2>& s2,
                     size_t score_cutoff = 0) const
    {
        if (score_count < result_count())
            throw std::invalid_argument("scores has to have >= result_count() elements");

        detail::Range scores_(scores, scores + score_count);
        if constexpr (MaxLen == 8)
            detail::lcs_simd<uint8_t>(scores_, PM, s2, score_cutoff);
        else if constexpr (MaxLen == 16)
            detail::lcs_simd<uint16_t>(scores_, PM, s2, score_cutoff);
        else if constexpr (MaxLen == 32)
            detail::lcs_simd<uint32_t>(scores_, PM, s2, score_cutoff);
        else if constexpr (MaxLen == 64)
            detail::lcs_simd<uint64_t>(scores_, PM, s2, score_cutoff);
    }

    template <typename InputIt2>
    size_t maximum(size_t s1_idx, const detail::Range<InputIt2>& s2) const
    {
        return std::max(str_lens[s1_idx], s2.size());
    }

    size_t get_input_count() const noexcept
    {
        return input_count;
    }

    size_t input_count;
    size_t pos;
    detail::BlockPatternMatchVector PM;
    std::vector<size_t> str_lens;
};
} /* namespace experimental */
#endif

template <typename CharT1>
struct CachedLCSseq
    : detail::CachedSimilarityBase<CachedLCSseq<CharT1>, size_t, 0, std::numeric_limits<int64_t>::max()> {
    template <typename Sentence1>
    explicit CachedLCSseq(const Sentence1& s1_) : CachedLCSseq(detail::to_begin(s1_), detail::to_end(s1_))
    {}

    template <typename InputIt1>
    CachedLCSseq(InputIt1 first1, InputIt1 last1) : s1(first1, last1), PM(detail::Range(first1, last1))
    {}

private:
    friend detail::CachedSimilarityBase<CachedLCSseq<CharT1>, size_t, 0, std::numeric_limits<int64_t>::max()>;
    friend detail::CachedNormalizedMetricBase<CachedLCSseq<CharT1>>;

    template <typename InputIt2>
    size_t maximum(const detail::Range<InputIt2>& s2) const
    {
        return std::max(s1.size(), s2.size());
    }

    template <typename InputIt2>
    size_t _similarity(const detail::Range<InputIt2>& s2, size_t score_cutoff,
                       [[maybe_unused]] size_t score_hint) const
    {
        return detail::lcs_seq_similarity(PM, detail::Range(s1), s2, score_cutoff);
    }

    std::vector<CharT1> s1;
    detail::BlockPatternMatchVector PM;
};

template <typename Sentence1>
explicit CachedLCSseq(const Sentence1& s1_) -> CachedLCSseq<char_type<Sentence1>>;

template <typename InputIt1>
CachedLCSseq(InputIt1 first1, InputIt1 last1) -> CachedLCSseq<iter_value_t<InputIt1>>;

} // namespace duckdb_rapidfuzz
