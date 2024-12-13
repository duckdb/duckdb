/* SPDX-License-Identifier: MIT */
/* Copyright © 2022-present Max Bachmann */

#pragma once

#include <limits>
#include <rapidfuzz/distance/Indel_impl.hpp>
#include <rapidfuzz/distance/LCSseq.hpp>

namespace duckdb_rapidfuzz {

template <typename InputIt1, typename InputIt2>
size_t indel_distance(InputIt1 first1, InputIt1 last1, InputIt2 first2, InputIt2 last2,
                      size_t score_cutoff = std::numeric_limits<size_t>::max())
{
    return detail::Indel::distance(first1, last1, first2, last2, score_cutoff, score_cutoff);
}

template <typename Sentence1, typename Sentence2>
size_t indel_distance(const Sentence1& s1, const Sentence2& s2,
                      size_t score_cutoff = std::numeric_limits<size_t>::max())
{
    return detail::Indel::distance(s1, s2, score_cutoff, score_cutoff);
}

template <typename InputIt1, typename InputIt2>
size_t indel_similarity(InputIt1 first1, InputIt1 last1, InputIt2 first2, InputIt2 last2,
                        size_t score_cutoff = 0.0)
{
    return detail::Indel::similarity(first1, last1, first2, last2, score_cutoff, score_cutoff);
}

template <typename Sentence1, typename Sentence2>
size_t indel_similarity(const Sentence1& s1, const Sentence2& s2, size_t score_cutoff = 0.0)
{
    return detail::Indel::similarity(s1, s2, score_cutoff, score_cutoff);
}

template <typename InputIt1, typename InputIt2>
double indel_normalized_distance(InputIt1 first1, InputIt1 last1, InputIt2 first2, InputIt2 last2,
                                 double score_cutoff = 1.0)
{
    return detail::Indel::normalized_distance(first1, last1, first2, last2, score_cutoff, score_cutoff);
}

template <typename Sentence1, typename Sentence2>
double indel_normalized_distance(const Sentence1& s1, const Sentence2& s2, double score_cutoff = 1.0)
{
    return detail::Indel::normalized_distance(s1, s2, score_cutoff, score_cutoff);
}

template <typename InputIt1, typename InputIt2>
double indel_normalized_similarity(InputIt1 first1, InputIt1 last1, InputIt2 first2, InputIt2 last2,
                                   double score_cutoff = 0.0)
{
    return detail::Indel::normalized_similarity(first1, last1, first2, last2, score_cutoff, score_cutoff);
}

template <typename Sentence1, typename Sentence2>
double indel_normalized_similarity(const Sentence1& s1, const Sentence2& s2, double score_cutoff = 0.0)
{
    return detail::Indel::normalized_similarity(s1, s2, score_cutoff, score_cutoff);
}

template <typename InputIt1, typename InputIt2>
Editops indel_editops(InputIt1 first1, InputIt1 last1, InputIt2 first2, InputIt2 last2)
{
    return lcs_seq_editops(first1, last1, first2, last2);
}

template <typename Sentence1, typename Sentence2>
Editops indel_editops(const Sentence1& s1, const Sentence2& s2)
{
    return lcs_seq_editops(s1, s2);
}

#ifdef RAPIDFUZZ_SIMD
namespace experimental {
template <int MaxLen>
struct MultiIndel
    : public detail::MultiDistanceBase<MultiIndel<MaxLen>, size_t, 0, std::numeric_limits<int64_t>::max()> {
private:
    friend detail::MultiDistanceBase<MultiIndel<MaxLen>, size_t, 0, std::numeric_limits<int64_t>::max()>;
    friend detail::MultiNormalizedMetricBase<MultiIndel<MaxLen>, size_t>;

public:
    MultiIndel(size_t count) : scorer(count)
    {}

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
        return scorer.result_count();
    }

    template <typename Sentence1>
    void insert(const Sentence1& s1_)
    {
        insert(detail::to_begin(s1_), detail::to_end(s1_));
    }

    template <typename InputIt1>
    void insert(InputIt1 first1, InputIt1 last1)
    {
        scorer.insert(first1, last1);
        str_lens.push_back(static_cast<size_t>(std::distance(first1, last1)));
    }

private:
    template <typename InputIt2>
    void _distance(size_t* scores, size_t score_count, const detail::Range<InputIt2>& s2,
                   size_t score_cutoff = std::numeric_limits<size_t>::max()) const
    {
        scorer.similarity(scores, score_count, s2);

        for (size_t i = 0; i < get_input_count(); ++i) {
            size_t maximum_ = maximum(i, s2);
            size_t dist = maximum_ - 2 * scores[i];
            scores[i] = (dist <= score_cutoff) ? dist : score_cutoff + 1;
        }
    }

    template <typename InputIt2>
    size_t maximum(size_t s1_idx, const detail::Range<InputIt2>& s2) const
    {
        return str_lens[s1_idx] + s2.size();
    }

    size_t get_input_count() const noexcept
    {
        return str_lens.size();
    }

    std::vector<size_t> str_lens;
    MultiLCSseq<MaxLen> scorer;
};
} /* namespace experimental */
#endif

template <typename CharT1>
struct CachedIndel
    : public detail::CachedDistanceBase<CachedIndel<CharT1>, size_t, 0, std::numeric_limits<int64_t>::max()> {
    template <typename Sentence1>
    explicit CachedIndel(const Sentence1& s1_) : CachedIndel(detail::to_begin(s1_), detail::to_end(s1_))
    {}

    template <typename InputIt1>
    CachedIndel(InputIt1 first1, InputIt1 last1)
        : s1_len(static_cast<size_t>(std::distance(first1, last1))), scorer(first1, last1)
    {}

private:
    friend detail::CachedDistanceBase<CachedIndel<CharT1>, size_t, 0, std::numeric_limits<int64_t>::max()>;
    friend detail::CachedNormalizedMetricBase<CachedIndel<CharT1>>;

    template <typename InputIt2>
    size_t maximum(const detail::Range<InputIt2>& s2) const
    {
        return s1_len + s2.size();
    }

    template <typename InputIt2>
    size_t _distance(const detail::Range<InputIt2>& s2, size_t score_cutoff, size_t score_hint) const
    {
        size_t maximum_ = maximum(s2);
        size_t lcs_cutoff = (maximum_ / 2 >= score_cutoff) ? maximum_ / 2 - score_cutoff : 0;
        size_t lcs_cutoff_hint = (maximum_ / 2 >= score_hint) ? maximum_ / 2 - score_hint : 0;
        size_t lcs_sim = scorer.similarity(s2, lcs_cutoff, lcs_cutoff_hint);
        size_t dist = maximum_ - 2 * lcs_sim;
        return (dist <= score_cutoff) ? dist : score_cutoff + 1;
    }

    size_t s1_len;
    CachedLCSseq<CharT1> scorer;
};

template <typename Sentence1>
explicit CachedIndel(const Sentence1& s1_) -> CachedIndel<char_type<Sentence1>>;

template <typename InputIt1>
CachedIndel(InputIt1 first1, InputIt1 last1) -> CachedIndel<iter_value_t<InputIt1>>;

} // namespace duckdb_rapidfuzz
