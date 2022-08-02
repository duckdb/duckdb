/* SPDX-License-Identifier: MIT */
/* Copyright © 2022 Max Bachmann */

#pragma once
#include "details/common.hpp"
#include "details/jaro_impl.hpp"

#include <stdexcept>

namespace duckdb_jaro_winkler {

/**
 * @defgroup jaro_winkler jaro_winkler
 * @{
 */

/**
 * @brief Calculates the jaro winkler similarity
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
 * @param prefix_weight
 *   Weight used for the common prefix of the two strings.
 *   Has to be between 0 and 0.25. Default is 0.1.
 * @param score_cutoff
 *   Optional argument for a score threshold as a float between 0 and 100.
 *   For similarity < score_cutoff 0 is returned instead. Default is 0,
 *   which deactivates this behaviour.
 *
 * @return jaro winkler similarity between s1 and s2
 *   as a float between 0 and 100
 */
template <typename InputIt1, typename InputIt2>
typename std::enable_if<
    common::is_iterator<InputIt1>::value && common::is_iterator<InputIt2>::value, double>::type
jaro_winkler_similarity(InputIt1 first1, InputIt1 last1, InputIt2 first2, InputIt2 last2,
                        double prefix_weight = 0.1, double score_cutoff = 0.0)
{
    if (prefix_weight < 0.0 || prefix_weight > 0.25) {
        throw std::invalid_argument("prefix_weight has to be between 0.0 and 0.25");
    }

    return detail::jaro_winkler_similarity(first1, last1, first2, last2, prefix_weight,
                                           score_cutoff);
}

template <typename S1, typename S2>
double jaro_winkler_similarity(const S1& s1, const S2& s2, double prefix_weight = 0.1,
                               double score_cutoff = 0.0)
{
    return jaro_winkler_similarity(std::begin(s1), std::end(s1), std::begin(s2), std::end(s2),
                                   prefix_weight, score_cutoff);
}

template <typename CharT1>
struct CachedJaroWinklerSimilarity {
    template <typename InputIt1>
    CachedJaroWinklerSimilarity(InputIt1 first1, InputIt1 last1, double prefix_weight_ = 0.1)
        : s1(first1, last1), PM(first1, last1), prefix_weight(prefix_weight_)
    {
        if (prefix_weight < 0.0 || prefix_weight > 0.25) {
            throw std::invalid_argument("prefix_weight has to be between 0.0 and 0.25");
        }
    }

    template <typename S1>
    CachedJaroWinklerSimilarity(const S1& s1_, double prefix_weight_ = 0.1)
        : CachedJaroWinklerSimilarity(std::begin(s1_), std::end(s1_), prefix_weight_)
    {}

    template <typename InputIt2>
    double similarity(InputIt2 first2, InputIt2 last2, double score_cutoff = 0) const
    {
        return detail::jaro_winkler_similarity(PM, std::begin(s1), std::end(s1), first2, last2,
                                               prefix_weight, score_cutoff);
    }

    template <typename S2>
    double similarity(const S2& s2, double score_cutoff = 0) const
    {
        return similarity(std::begin(s2), std::end(s2), score_cutoff);
    }

    template <typename InputIt2>
    double normalized_similarity(InputIt2 first2, InputIt2 last2, double score_cutoff = 0) const
    {
        return similarity(first2, last2, score_cutoff);
    }

    template <typename S2>
    double normalized_similarity(const S2& s2, double score_cutoff = 0) const
    {
        return similarity(s2, score_cutoff);
    }

private:
    std::basic_string<CharT1> s1;
    common::BlockPatternMatchVector PM;

    double prefix_weight;
};

/**
 * @brief Calculates the jaro similarity
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
 *   Optional argument for a score threshold as a float between 0 and 100.
 *   For similarity < score_cutoff 0 is returned instead. Default is 0,
 *   which deactivates this behaviour.
 *
 * @return jaro similarity between s1 and s2
 *   as a float between 0 and 100
 */
template <typename InputIt1, typename InputIt2>
double jaro_similarity(InputIt1 first1, InputIt1 last1, InputIt2 first2, InputIt2 last2,
                       double score_cutoff = 0.0)
{
    return detail::jaro_similarity(first1, last1, first2, last2, score_cutoff);
}

template <typename S1, typename S2>
double jaro_similarity(const S1& s1, const S2& s2, double score_cutoff = 0.0)
{
    return jaro_similarity(std::begin(s1), std::end(s1), std::begin(s2), std::end(s2),
                           score_cutoff);
}

template <typename CharT1>
struct CachedJaroSimilarity {
    template <typename InputIt1>
    CachedJaroSimilarity(InputIt1 first1, InputIt1 last1) : s1(first1, last1), PM(first1, last1)
    {}

    template <typename S1>
    CachedJaroSimilarity(const S1& s1_) : CachedJaroSimilarity(std::begin(s1_), std::end(s1_))
    {}

    template <typename InputIt2>
    double similarity(InputIt2 first2, InputIt2 last2, double score_cutoff = 0) const
    {
        return detail::jaro_similarity(PM, std::begin(s1), std::end(s1), first2, last2,
                                       score_cutoff);
    }

    template <typename S2>
    double similarity(const S2& s2, double score_cutoff = 0) const
    {
        return similarity(std::begin(s2), std::end(s2), score_cutoff);
    }

    template <typename InputIt2>
    double normalized_similarity(InputIt2 first2, InputIt2 last2, double score_cutoff = 0) const
    {
        return similarity(first2, last2, score_cutoff);
    }

    template <typename S2>
    double normalized_similarity(const S2& s2, double score_cutoff = 0) const
    {
        return similarity(s2, score_cutoff);
    }

private:
    std::basic_string<CharT1> s1;
    common::BlockPatternMatchVector PM;
};

/**@}*/

} // namespace duckdb_jaro_winkler
