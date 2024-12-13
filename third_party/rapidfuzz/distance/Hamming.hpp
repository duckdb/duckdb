/* SPDX-License-Identifier: MIT */
/* Copyright © 2021 Max Bachmann */

#pragma once
#include <limits>
#include <rapidfuzz/details/common.hpp>
#include <rapidfuzz/distance/Hamming_impl.hpp>

namespace duckdb_rapidfuzz {

/**
 * @brief Calculates the Hamming distance between two strings.
 *
 * @details
 * Both strings require a similar length
 *
 *
 * @tparam Sentence1 This is a string that can be converted to
 * basic_string_view<char_type>
 * @tparam Sentence2 This is a string that can be converted to
 * basic_string_view<char_type>
 *
 * @param s1
 *   string to compare with s2 (for type info check Template parameters above)
 * @param s2
 *   string to compare with s1 (for type info check Template parameters above)
 * @param max
 *   Maximum Hamming distance between s1 and s2, that is
 *   considered as a result. If the distance is bigger than max,
 *   max + 1 is returned instead. Default is std::numeric_limits<size_t>::max(),
 *   which deactivates this behaviour.
 *
 * @return Hamming distance between s1 and s2
 */
template <typename InputIt1, typename InputIt2>
size_t hamming_distance(InputIt1 first1, InputIt1 last1, InputIt2 first2, InputIt2 last2, bool pad_ = true,
                        size_t score_cutoff = std::numeric_limits<size_t>::max())
{
    return detail::Hamming::distance(first1, last1, first2, last2, pad_, score_cutoff, score_cutoff);
}

template <typename Sentence1, typename Sentence2>
size_t hamming_distance(const Sentence1& s1, const Sentence2& s2, bool pad_ = true,
                        size_t score_cutoff = std::numeric_limits<size_t>::max())
{
    return detail::Hamming::distance(s1, s2, pad_, score_cutoff, score_cutoff);
}

template <typename InputIt1, typename InputIt2>
size_t hamming_similarity(InputIt1 first1, InputIt1 last1, InputIt2 first2, InputIt2 last2, bool pad_ = true,
                          size_t score_cutoff = 0)
{
    return detail::Hamming::similarity(first1, last1, first2, last2, pad_, score_cutoff, score_cutoff);
}

template <typename Sentence1, typename Sentence2>
size_t hamming_similarity(const Sentence1& s1, const Sentence2& s2, bool pad_ = true, size_t score_cutoff = 0)
{
    return detail::Hamming::similarity(s1, s2, pad_, score_cutoff, score_cutoff);
}

template <typename InputIt1, typename InputIt2>
double hamming_normalized_distance(InputIt1 first1, InputIt1 last1, InputIt2 first2, InputIt2 last2,
                                   bool pad_ = true, double score_cutoff = 1.0)
{
    return detail::Hamming::normalized_distance(first1, last1, first2, last2, pad_, score_cutoff,
                                                score_cutoff);
}

template <typename Sentence1, typename Sentence2>
double hamming_normalized_distance(const Sentence1& s1, const Sentence2& s2, bool pad_ = true,
                                   double score_cutoff = 1.0)
{
    return detail::Hamming::normalized_distance(s1, s2, pad_, score_cutoff, score_cutoff);
}

template <typename InputIt1, typename InputIt2>
Editops hamming_editops(InputIt1 first1, InputIt1 last1, InputIt2 first2, InputIt2 last2, bool pad_ = true,
                        size_t score_hint = std::numeric_limits<size_t>::max())
{
    return detail::hamming_editops(detail::Range(first1, last1), detail::Range(first2, last2), pad_,
                                   score_hint);
}

template <typename Sentence1, typename Sentence2>
Editops hamming_editops(const Sentence1& s1, const Sentence2& s2, bool pad_ = true,
                        size_t score_hint = std::numeric_limits<size_t>::max())
{
    return detail::hamming_editops(detail::Range(s1), detail::Range(s2), pad_, score_hint);
}

/**
 * @brief Calculates a normalized hamming similarity
 *
 * @details
 * Both string require a similar length
 *
 *
 * @tparam Sentence1 This is a string that can be converted to
 * basic_string_view<char_type>
 * @tparam Sentence2 This is a string that can be converted to
 * basic_string_view<char_type>
 *
 * @param s1
 *   string to compare with s2 (for type info check Template parameters above)
 * @param s2
 *   string to compare with s1 (for type info check Template parameters above)
 * @param score_cutoff
 *   Optional argument for a score threshold as a float between 0 and 1.0.
 *   For ratio < score_cutoff 0 is returned instead. Default is 0,
 *   which deactivates this behaviour.
 *
 * @return Normalized hamming distance between s1 and s2
 *   as a float between 0 and 1.0
 */
template <typename InputIt1, typename InputIt2>
double hamming_normalized_similarity(InputIt1 first1, InputIt1 last1, InputIt2 first2, InputIt2 last2,
                                     bool pad_ = true, double score_cutoff = 0.0)
{
    return detail::Hamming::normalized_similarity(first1, last1, first2, last2, pad_, score_cutoff,
                                                  score_cutoff);
}

template <typename Sentence1, typename Sentence2>
double hamming_normalized_similarity(const Sentence1& s1, const Sentence2& s2, bool pad_ = true,
                                     double score_cutoff = 0.0)
{
    return detail::Hamming::normalized_similarity(s1, s2, pad_, score_cutoff, score_cutoff);
}

template <typename CharT1>
struct CachedHamming : public detail::CachedDistanceBase<CachedHamming<CharT1>, size_t, 0,
                                                         std::numeric_limits<int64_t>::max()> {
    template <typename Sentence1>
    explicit CachedHamming(const Sentence1& s1_, bool pad_ = true)
        : CachedHamming(detail::to_begin(s1_), detail::to_end(s1_), pad_)
    {}

    template <typename InputIt1>
    CachedHamming(InputIt1 first1, InputIt1 last1, bool pad_ = true) : s1(first1, last1), pad(pad_)
    {}

private:
    friend detail::CachedDistanceBase<CachedHamming<CharT1>, size_t, 0, std::numeric_limits<int64_t>::max()>;
    friend detail::CachedNormalizedMetricBase<CachedHamming<CharT1>>;

    template <typename InputIt2>
    size_t maximum(const detail::Range<InputIt2>& s2) const
    {
        return std::max(s1.size(), s2.size());
    }

    template <typename InputIt2>
    size_t _distance(const detail::Range<InputIt2>& s2, size_t score_cutoff,
                     [[maybe_unused]] size_t score_hint) const
    {
        return detail::Hamming::distance(s1, s2, pad, score_cutoff, score_hint);
    }

    std::vector<CharT1> s1;
    bool pad;
};

template <typename Sentence1>
explicit CachedHamming(const Sentence1& s1_, bool pad_ = true) -> CachedHamming<char_type<Sentence1>>;

template <typename InputIt1>
CachedHamming(InputIt1 first1, InputIt1 last1, bool pad_ = true) -> CachedHamming<iter_value_t<InputIt1>>;

/**@}*/

} // namespace duckdb_rapidfuzz
