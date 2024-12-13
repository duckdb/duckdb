/* SPDX-License-Identifier: MIT */
/* Copyright © 2022-present Max Bachmann */

#include <algorithm>
#include <rapidfuzz/distance/DamerauLevenshtein_impl.hpp>

namespace duckdb_rapidfuzz {
/* the API will require a change when adding custom weights */
namespace experimental {
/**
 * @brief Calculates the Damerau Levenshtein distance between two strings.
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
 *   Maximum Damerau Levenshtein distance between s1 and s2, that is
 *   considered as a result. If the distance is bigger than max,
 *   max + 1 is returned instead. Default is std::numeric_limits<size_t>::max(),
 *   which deactivates this behaviour.
 *
 * @return Damerau Levenshtein distance between s1 and s2
 */
template <typename InputIt1, typename InputIt2>
size_t damerau_levenshtein_distance(InputIt1 first1, InputIt1 last1, InputIt2 first2, InputIt2 last2,
                                    size_t score_cutoff = std::numeric_limits<size_t>::max())
{
    return detail::DamerauLevenshtein::distance(first1, last1, first2, last2, score_cutoff, score_cutoff);
}

template <typename Sentence1, typename Sentence2>
size_t damerau_levenshtein_distance(const Sentence1& s1, const Sentence2& s2,
                                    size_t score_cutoff = std::numeric_limits<size_t>::max())
{
    return detail::DamerauLevenshtein::distance(s1, s2, score_cutoff, score_cutoff);
}

template <typename InputIt1, typename InputIt2>
size_t damerau_levenshtein_similarity(InputIt1 first1, InputIt1 last1, InputIt2 first2, InputIt2 last2,
                                      size_t score_cutoff = 0)
{
    return detail::DamerauLevenshtein::similarity(first1, last1, first2, last2, score_cutoff, score_cutoff);
}

template <typename Sentence1, typename Sentence2>
size_t damerau_levenshtein_similarity(const Sentence1& s1, const Sentence2& s2, size_t score_cutoff = 0)
{
    return detail::DamerauLevenshtein::similarity(s1, s2, score_cutoff, score_cutoff);
}

template <typename InputIt1, typename InputIt2>
double damerau_levenshtein_normalized_distance(InputIt1 first1, InputIt1 last1, InputIt2 first2,
                                               InputIt2 last2, double score_cutoff = 1.0)
{
    return detail::DamerauLevenshtein::normalized_distance(first1, last1, first2, last2, score_cutoff,
                                                           score_cutoff);
}

template <typename Sentence1, typename Sentence2>
double damerau_levenshtein_normalized_distance(const Sentence1& s1, const Sentence2& s2,
                                               double score_cutoff = 1.0)
{
    return detail::DamerauLevenshtein::normalized_distance(s1, s2, score_cutoff, score_cutoff);
}

/**
 * @brief Calculates a normalized Damerau Levenshtein similarity
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
 * @return Normalized Damerau Levenshtein distance between s1 and s2
 *   as a float between 0 and 1.0
 */
template <typename InputIt1, typename InputIt2>
double damerau_levenshtein_normalized_similarity(InputIt1 first1, InputIt1 last1, InputIt2 first2,
                                                 InputIt2 last2, double score_cutoff = 0.0)
{
    return detail::DamerauLevenshtein::normalized_similarity(first1, last1, first2, last2, score_cutoff,
                                                             score_cutoff);
}

template <typename Sentence1, typename Sentence2>
double damerau_levenshtein_normalized_similarity(const Sentence1& s1, const Sentence2& s2,
                                                 double score_cutoff = 0.0)
{
    return detail::DamerauLevenshtein::normalized_similarity(s1, s2, score_cutoff, score_cutoff);
}

template <typename CharT1>
struct CachedDamerauLevenshtein : public detail::CachedDistanceBase<CachedDamerauLevenshtein<CharT1>, size_t,
                                                                    0, std::numeric_limits<int64_t>::max()> {
    template <typename Sentence1>
    explicit CachedDamerauLevenshtein(const Sentence1& s1_)
        : CachedDamerauLevenshtein(detail::to_begin(s1_), detail::to_end(s1_))
    {}

    template <typename InputIt1>
    CachedDamerauLevenshtein(InputIt1 first1, InputIt1 last1) : s1(first1, last1)
    {}

private:
    friend detail::CachedDistanceBase<CachedDamerauLevenshtein<CharT1>, size_t, 0,
                                      std::numeric_limits<int64_t>::max()>;
    friend detail::CachedNormalizedMetricBase<CachedDamerauLevenshtein<CharT1>>;

    template <typename InputIt2>
    size_t maximum(const detail::Range<InputIt2>& s2) const
    {
        return std::max(s1.size(), s2.size());
    }

    template <typename InputIt2>
    size_t _distance(const detail::Range<InputIt2>& s2, size_t score_cutoff,
                     [[maybe_unused]] size_t score_hint) const
    {
        return rapidfuzz::experimental::damerau_levenshtein_distance(s1, s2, score_cutoff);
    }

    std::vector<CharT1> s1;
};

template <typename Sentence1>
explicit CachedDamerauLevenshtein(const Sentence1& s1_) -> CachedDamerauLevenshtein<char_type<Sentence1>>;

template <typename InputIt1>
CachedDamerauLevenshtein(InputIt1 first1, InputIt1 last1) -> CachedDamerauLevenshtein<iter_value_t<InputIt1>>;

} // namespace experimental
} // namespace duckdb_rapidfuzz
