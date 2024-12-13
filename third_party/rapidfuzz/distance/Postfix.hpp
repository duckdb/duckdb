/* SPDX-License-Identifier: MIT */
/* Copyright © 2021 Max Bachmann */

#pragma once

#include <limits>
#include <rapidfuzz/details/common.hpp>
#include <rapidfuzz/distance/Postfix_impl.hpp>

namespace duckdb_rapidfuzz {

template <typename InputIt1, typename InputIt2>
size_t postfix_distance(InputIt1 first1, InputIt1 last1, InputIt2 first2, InputIt2 last2,
                        size_t score_cutoff = std::numeric_limits<size_t>::max())
{
    return detail::Postfix::distance(first1, last1, first2, last2, score_cutoff, score_cutoff);
}

template <typename Sentence1, typename Sentence2>
size_t postfix_distance(const Sentence1& s1, const Sentence2& s2,
                        size_t score_cutoff = std::numeric_limits<size_t>::max())
{
    return detail::Postfix::distance(s1, s2, score_cutoff, score_cutoff);
}

template <typename InputIt1, typename InputIt2>
size_t postfix_similarity(InputIt1 first1, InputIt1 last1, InputIt2 first2, InputIt2 last2,
                          size_t score_cutoff = 0)
{
    return detail::Postfix::similarity(first1, last1, first2, last2, score_cutoff, score_cutoff);
}

template <typename Sentence1, typename Sentence2>
size_t postfix_similarity(const Sentence1& s1, const Sentence2& s2, size_t score_cutoff = 0)
{
    return detail::Postfix::similarity(s1, s2, score_cutoff, score_cutoff);
}

template <typename InputIt1, typename InputIt2>
double postfix_normalized_distance(InputIt1 first1, InputIt1 last1, InputIt2 first2, InputIt2 last2,
                                   double score_cutoff = 1.0)
{
    return detail::Postfix::normalized_distance(first1, last1, first2, last2, score_cutoff, score_cutoff);
}

template <typename Sentence1, typename Sentence2>
double postfix_normalized_distance(const Sentence1& s1, const Sentence2& s2, double score_cutoff = 1.0)
{
    return detail::Postfix::normalized_distance(s1, s2, score_cutoff, score_cutoff);
}

template <typename InputIt1, typename InputIt2>
double postfix_normalized_similarity(InputIt1 first1, InputIt1 last1, InputIt2 first2, InputIt2 last2,
                                     double score_cutoff = 0.0)
{
    return detail::Postfix::normalized_similarity(first1, last1, first2, last2, score_cutoff, score_cutoff);
}

template <typename Sentence1, typename Sentence2>
double postfix_normalized_similarity(const Sentence1& s1, const Sentence2& s2, double score_cutoff = 0.0)
{
    return detail::Postfix::normalized_similarity(s1, s2, score_cutoff, score_cutoff);
}

template <typename CharT1>
struct CachedPostfix : public detail::CachedSimilarityBase<CachedPostfix<CharT1>, size_t, 0,
                                                           std::numeric_limits<int64_t>::max()> {
    template <typename Sentence1>
    explicit CachedPostfix(const Sentence1& s1_) : CachedPostfix(detail::to_begin(s1_), detail::to_end(s1_))
    {}

    template <typename InputIt1>
    CachedPostfix(InputIt1 first1, InputIt1 last1) : s1(first1, last1)
    {}

private:
    friend detail::CachedSimilarityBase<CachedPostfix<CharT1>, size_t, 0,
                                        std::numeric_limits<int64_t>::max()>;
    friend detail::CachedNormalizedMetricBase<CachedPostfix<CharT1>>;

    template <typename InputIt2>
    size_t maximum(const detail::Range<InputIt2>& s2) const
    {
        return std::max(s1.size(), s2.size());
    }

    template <typename InputIt2>
    size_t _similarity(detail::Range<InputIt2> s2, size_t score_cutoff,
                       [[maybe_unused]] size_t score_hint) const
    {
        return detail::Postfix::similarity(s1, s2, score_cutoff, score_hint);
    }

    std::vector<CharT1> s1;
};

template <typename Sentence1>
explicit CachedPostfix(const Sentence1& s1_) -> CachedPostfix<char_type<Sentence1>>;

template <typename InputIt1>
CachedPostfix(InputIt1 first1, InputIt1 last1) -> CachedPostfix<iter_value_t<InputIt1>>;

/**@}*/

} // namespace duckdb_rapidfuzz
