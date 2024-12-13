/* SPDX-License-Identifier: MIT */
/* Copyright © 2021-present Max Bachmann */
/* Copyright © 2011 Adam Cohen */

#include <limits>
#include <rapidfuzz/details/CharSet.hpp>

#include <algorithm>
#include <cmath>
#include <iterator>
#include <sys/types.h>
#include <vector>

namespace duckdb_rapidfuzz::fuzz {

/**********************************************
 *                  ratio
 *********************************************/

template <typename InputIt1, typename InputIt2>
double ratio(InputIt1 first1, InputIt1 last1, InputIt2 first2, InputIt2 last2, double score_cutoff)
{
    return ratio(detail::Range(first1, last1), detail::Range(first2, last2), score_cutoff);
}

template <typename Sentence1, typename Sentence2>
double ratio(const Sentence1& s1, const Sentence2& s2, const double score_cutoff)
{
    return indel_normalized_similarity(s1, s2, score_cutoff / 100) * 100;
}

template <typename CharT1>
template <typename InputIt2>
double CachedRatio<CharT1>::similarity(InputIt2 first2, InputIt2 last2, double score_cutoff,
                                       double score_hint) const
{
    return similarity(detail::Range(first2, last2), score_cutoff, score_hint);
}

template <typename CharT1>
template <typename Sentence2>
double CachedRatio<CharT1>::similarity(const Sentence2& s2, double score_cutoff, double score_hint) const
{
    return cached_indel.normalized_similarity(s2, score_cutoff / 100, score_hint / 100) * 100;
}

/**********************************************
 *              partial_ratio
 *********************************************/

namespace fuzz_detail {

static constexpr double norm_distance(size_t dist, size_t lensum, double score_cutoff = 0)
{
    double score =
        (lensum > 0) ? (100.0 - 100.0 * static_cast<double>(dist) / static_cast<double>(lensum)) : 100.0;

    return (score >= score_cutoff) ? score : 0;
}

static inline size_t score_cutoff_to_distance(double score_cutoff, size_t lensum)
{
    return static_cast<size_t>(std::ceil(static_cast<double>(lensum) * (1.0 - score_cutoff / 100)));
}

template <typename InputIt1, typename InputIt2, typename CachedCharT1>
ScoreAlignment<double>
partial_ratio_impl(const detail::Range<InputIt1>& s1, const detail::Range<InputIt2>& s2,
                   const CachedRatio<CachedCharT1>& cached_ratio,
                   const detail::CharSet<iter_value_t<InputIt1>>& s1_char_set, double score_cutoff)
{
    ScoreAlignment<double> res;
    size_t len1 = s1.size();
    size_t len2 = s2.size();
    res.src_start = 0;
    res.src_end = len1;
    res.dest_start = 0;
    res.dest_end = len1;

    if (len2 > len1) {
        size_t maximum = len1 * 2;
        double norm_cutoff_sim = rapidfuzz::detail::NormSim_to_NormDist(score_cutoff / 100);
        size_t cutoff_dist = static_cast<size_t>(std::ceil(static_cast<double>(maximum) * norm_cutoff_sim));
        size_t best_dist = std::numeric_limits<size_t>::max();
        std::vector<size_t> scores(len2 - len1, std::numeric_limits<size_t>::max());
        std::vector<std::pair<size_t, size_t>> windows = {{0, len2 - len1 - 1}};
        std::vector<std::pair<size_t, size_t>> new_windows;

        while (!windows.empty()) {
            for (const auto& window : windows) {
                auto subseq1_first = s2.begin() + static_cast<ptrdiff_t>(window.first);
                auto subseq2_first = s2.begin() + static_cast<ptrdiff_t>(window.second);
                detail::Range subseq1(subseq1_first, subseq1_first + static_cast<ptrdiff_t>(len1));
                detail::Range subseq2(subseq2_first, subseq2_first + static_cast<ptrdiff_t>(len1));

                if (scores[window.first] == std::numeric_limits<size_t>::max()) {
                    scores[window.first] = cached_ratio.cached_indel.distance(subseq1);
                    if (scores[window.first] < cutoff_dist) {
                        cutoff_dist = best_dist = scores[window.first];
                        res.dest_start = window.first;
                        res.dest_end = window.first + len1;
                        if (best_dist == 0) {
                            res.score = 100;
                            return res;
                        }
                    }
                }
                if (scores[window.second] == std::numeric_limits<size_t>::max()) {
                    scores[window.second] = cached_ratio.cached_indel.distance(subseq2);
                    if (scores[window.second] < cutoff_dist) {
                        cutoff_dist = best_dist = scores[window.second];
                        res.dest_start = window.second;
                        res.dest_end = window.second + len1;
                        if (best_dist == 0) {
                            res.score = 100;
                            return res;
                        }
                    }
                }

                size_t cell_diff = window.second - window.first;
                if (cell_diff == 1) continue;

                /* find the minimum score possible in the range first <-> last */
                size_t known_edits = detail::abs_diff(scores[window.first], scores[window.second]);
                /* half of the cells that are not needed for known_edits can lead to a better score */
                ptrdiff_t min_score =
                    static_cast<ptrdiff_t>(std::min(scores[window.first], scores[window.second])) -
                    static_cast<ptrdiff_t>(cell_diff + known_edits / 2);
                if (min_score < static_cast<ptrdiff_t>(cutoff_dist)) {
                    size_t center = cell_diff / 2;
                    new_windows.emplace_back(window.first, window.first + center);
                    new_windows.emplace_back(window.first + center, window.second);
                }
            }

            std::swap(windows, new_windows);
            new_windows.clear();
        }

        double score = 1.0 - (static_cast<double>(best_dist) / static_cast<double>(maximum));
        score *= 100;
        if (score >= score_cutoff) score_cutoff = res.score = score;
    }

    for (size_t i = 1; i < len1; ++i) {
        rapidfuzz::detail::Range subseq(s2.begin(), s2.begin() + static_cast<ptrdiff_t>(i));
        if (!s1_char_set.find(subseq.back())) continue;

        double ls_ratio = cached_ratio.similarity(subseq, score_cutoff);
        if (ls_ratio > res.score) {
            score_cutoff = res.score = ls_ratio;
            res.dest_start = 0;
            res.dest_end = i;
            if (res.score == 100.0) return res;
        }
    }

    for (size_t i = len2 - len1; i < len2; ++i) {
        rapidfuzz::detail::Range subseq(s2.begin() + static_cast<ptrdiff_t>(i), s2.end());
        if (!s1_char_set.find(subseq.front())) continue;

        double ls_ratio = cached_ratio.similarity(subseq, score_cutoff);
        if (ls_ratio > res.score) {
            score_cutoff = res.score = ls_ratio;
            res.dest_start = i;
            res.dest_end = len2;
            if (res.score == 100.0) return res;
        }
    }

    return res;
}

template <typename InputIt1, typename InputIt2, typename CharT1 = iter_value_t<InputIt1>>
ScoreAlignment<double> partial_ratio_impl(const detail::Range<InputIt1>& s1,
                                          const detail::Range<InputIt2>& s2, double score_cutoff)
{
    CachedRatio<CharT1> cached_ratio(s1);

    detail::CharSet<CharT1> s1_char_set;
    for (auto ch : s1)
        s1_char_set.insert(ch);

    return partial_ratio_impl(s1, s2, cached_ratio, s1_char_set, score_cutoff);
}

} // namespace fuzz_detail

template <typename InputIt1, typename InputIt2>
ScoreAlignment<double> partial_ratio_alignment(InputIt1 first1, InputIt1 last1, InputIt2 first2,
                                               InputIt2 last2, double score_cutoff)
{
    size_t len1 = static_cast<size_t>(std::distance(first1, last1));
    size_t len2 = static_cast<size_t>(std::distance(first2, last2));

    if (len1 > len2) {
        ScoreAlignment<double> result = partial_ratio_alignment(first2, last2, first1, last1, score_cutoff);
        std::swap(result.src_start, result.dest_start);
        std::swap(result.src_end, result.dest_end);
        return result;
    }

    if (score_cutoff > 100) return ScoreAlignment<double>(0, 0, len1, 0, len1);

    if (!len1 || !len2)
        return ScoreAlignment<double>(static_cast<double>(len1 == len2) * 100.0, 0, len1, 0, len1);

    auto s1 = detail::Range(first1, last1);
    auto s2 = detail::Range(first2, last2);

    auto alignment = fuzz_detail::partial_ratio_impl(s1, s2, score_cutoff);
    if (alignment.score != 100 && s1.size() == s2.size()) {
        score_cutoff = std::max(score_cutoff, alignment.score);
        auto alignment2 = fuzz_detail::partial_ratio_impl(s2, s1, score_cutoff);
        if (alignment2.score > alignment.score) {
            std::swap(alignment2.src_start, alignment2.dest_start);
            std::swap(alignment2.src_end, alignment2.dest_end);
            return alignment2;
        }
    }

    return alignment;
}

template <typename Sentence1, typename Sentence2>
ScoreAlignment<double> partial_ratio_alignment(const Sentence1& s1, const Sentence2& s2, double score_cutoff)
{
    return partial_ratio_alignment(detail::to_begin(s1), detail::to_end(s1), detail::to_begin(s2),
                                   detail::to_end(s2), score_cutoff);
}

template <typename InputIt1, typename InputIt2>
double partial_ratio(InputIt1 first1, InputIt1 last1, InputIt2 first2, InputIt2 last2, double score_cutoff)
{
    return partial_ratio_alignment(first1, last1, first2, last2, score_cutoff).score;
}

template <typename Sentence1, typename Sentence2>
double partial_ratio(const Sentence1& s1, const Sentence2& s2, double score_cutoff)
{
    return partial_ratio_alignment(s1, s2, score_cutoff).score;
}

template <typename CharT1>
template <typename InputIt1>
CachedPartialRatio<CharT1>::CachedPartialRatio(InputIt1 first1, InputIt1 last1)
    : s1(first1, last1), cached_ratio(first1, last1)
{
    for (const auto& ch : s1)
        s1_char_set.insert(ch);
}

template <typename CharT1>
template <typename InputIt2>
double CachedPartialRatio<CharT1>::similarity(InputIt2 first2, InputIt2 last2, double score_cutoff,
                                              [[maybe_unused]] double score_hint) const
{
    size_t len1 = s1.size();
    size_t len2 = static_cast<size_t>(std::distance(first2, last2));

    if (len1 > len2)
        return partial_ratio(detail::to_begin(s1), detail::to_end(s1), first2, last2, score_cutoff);

    if (score_cutoff > 100) return 0;

    if (!len1 || !len2) return static_cast<double>(len1 == len2) * 100.0;

    auto s1_ = detail::Range(s1);
    auto s2 = detail::Range(first2, last2);

    double score = fuzz_detail::partial_ratio_impl(s1_, s2, cached_ratio, s1_char_set, score_cutoff).score;
    if (score != 100 && s1_.size() == s2.size()) {
        score_cutoff = std::max(score_cutoff, score);
        double score2 = fuzz_detail::partial_ratio_impl(s2, s1_, score_cutoff).score;
        if (score2 > score) return score2;
    }

    return score;
}

template <typename CharT1>
template <typename Sentence2>
double CachedPartialRatio<CharT1>::similarity(const Sentence2& s2, double score_cutoff,
                                              [[maybe_unused]] double score_hint) const
{
    return similarity(detail::to_begin(s2), detail::to_end(s2), score_cutoff);
}

/**********************************************
 *             token_sort_ratio
 *********************************************/
template <typename InputIt1, typename InputIt2>
double token_sort_ratio(InputIt1 first1, InputIt1 last1, InputIt2 first2, InputIt2 last2, double score_cutoff)
{
    if (score_cutoff > 100) return 0;

    return ratio(detail::sorted_split(first1, last1).join(), detail::sorted_split(first2, last2).join(),
                 score_cutoff);
}

template <typename Sentence1, typename Sentence2>
double token_sort_ratio(const Sentence1& s1, const Sentence2& s2, double score_cutoff)
{
    return token_sort_ratio(detail::to_begin(s1), detail::to_end(s1), detail::to_begin(s2),
                            detail::to_end(s2), score_cutoff);
}

template <typename CharT1>
template <typename InputIt2>
double CachedTokenSortRatio<CharT1>::similarity(InputIt2 first2, InputIt2 last2, double score_cutoff,
                                                [[maybe_unused]] double score_hint) const
{
    if (score_cutoff > 100) return 0;

    return cached_ratio.similarity(detail::sorted_split(first2, last2).join(), score_cutoff);
}

template <typename CharT1>
template <typename Sentence2>
double CachedTokenSortRatio<CharT1>::similarity(const Sentence2& s2, double score_cutoff,
                                                [[maybe_unused]] double score_hint) const
{
    return similarity(detail::to_begin(s2), detail::to_end(s2), score_cutoff);
}

/**********************************************
 *          partial_token_sort_ratio
 *********************************************/

template <typename InputIt1, typename InputIt2>
double partial_token_sort_ratio(InputIt1 first1, InputIt1 last1, InputIt2 first2, InputIt2 last2,
                                double score_cutoff)
{
    if (score_cutoff > 100) return 0;

    return partial_ratio(detail::sorted_split(first1, last1).join(),
                         detail::sorted_split(first2, last2).join(), score_cutoff);
}

template <typename Sentence1, typename Sentence2>
double partial_token_sort_ratio(const Sentence1& s1, const Sentence2& s2, double score_cutoff)
{
    return partial_token_sort_ratio(detail::to_begin(s1), detail::to_end(s1), detail::to_begin(s2),
                                    detail::to_end(s2), score_cutoff);
}

template <typename CharT1>
template <typename InputIt2>
double CachedPartialTokenSortRatio<CharT1>::similarity(InputIt2 first2, InputIt2 last2, double score_cutoff,
                                                       [[maybe_unused]] double score_hint) const
{
    if (score_cutoff > 100) return 0;

    return cached_partial_ratio.similarity(detail::sorted_split(first2, last2).join(), score_cutoff);
}

template <typename CharT1>
template <typename Sentence2>
double CachedPartialTokenSortRatio<CharT1>::similarity(const Sentence2& s2, double score_cutoff,
                                                       [[maybe_unused]] double score_hint) const
{
    return similarity(detail::to_begin(s2), detail::to_end(s2), score_cutoff);
}

/**********************************************
 *               token_set_ratio
 *********************************************/

namespace fuzz_detail {
template <typename InputIt1, typename InputIt2>
double token_set_ratio(const rapidfuzz::detail::SplittedSentenceView<InputIt1>& tokens_a,
                       const rapidfuzz::detail::SplittedSentenceView<InputIt2>& tokens_b,
                       const double score_cutoff)
{
    /* in FuzzyWuzzy this returns 0. For sake of compatibility return 0 here as well
     * see https://github.com/rapidfuzz/RapidFuzz/issues/110 */
    if (tokens_a.empty() || tokens_b.empty()) return 0;

    auto decomposition = detail::set_decomposition(tokens_a, tokens_b);
    auto intersect = decomposition.intersection;
    auto diff_ab = decomposition.difference_ab;
    auto diff_ba = decomposition.difference_ba;

    // one sentence is part of the other one
    if (!intersect.empty() && (diff_ab.empty() || diff_ba.empty())) return 100;

    auto diff_ab_joined = diff_ab.join();
    auto diff_ba_joined = diff_ba.join();

    size_t ab_len = diff_ab_joined.size();
    size_t ba_len = diff_ba_joined.size();
    size_t sect_len = intersect.length();

    // string length sect+ab <-> sect and sect+ba <-> sect
    size_t sect_ab_len = sect_len + bool(sect_len) + ab_len;
    size_t sect_ba_len = sect_len + bool(sect_len) + ba_len;

    double result = 0;
    size_t cutoff_distance = score_cutoff_to_distance(score_cutoff, sect_ab_len + sect_ba_len);
    size_t dist = indel_distance(diff_ab_joined, diff_ba_joined, cutoff_distance);

    if (dist <= cutoff_distance) result = norm_distance(dist, sect_ab_len + sect_ba_len, score_cutoff);

    // exit early since the other ratios are 0
    if (!sect_len) return result;

    // levenshtein distance sect+ab <-> sect and sect+ba <-> sect
    // since only sect is similar in them the distance can be calculated based on
    // the length difference
    size_t sect_ab_dist = bool(sect_len) + ab_len;
    double sect_ab_ratio = norm_distance(sect_ab_dist, sect_len + sect_ab_len, score_cutoff);

    size_t sect_ba_dist = bool(sect_len) + ba_len;
    double sect_ba_ratio = norm_distance(sect_ba_dist, sect_len + sect_ba_len, score_cutoff);

    return std::max({result, sect_ab_ratio, sect_ba_ratio});
}
} // namespace fuzz_detail

template <typename InputIt1, typename InputIt2>
double token_set_ratio(InputIt1 first1, InputIt1 last1, InputIt2 first2, InputIt2 last2, double score_cutoff)
{
    if (score_cutoff > 100) return 0;

    return fuzz_detail::token_set_ratio(detail::sorted_split(first1, last1),
                                        detail::sorted_split(first2, last2), score_cutoff);
}

template <typename Sentence1, typename Sentence2>
double token_set_ratio(const Sentence1& s1, const Sentence2& s2, double score_cutoff)
{
    return token_set_ratio(detail::to_begin(s1), detail::to_end(s1), detail::to_begin(s2), detail::to_end(s2),
                           score_cutoff);
}

template <typename CharT1>
template <typename InputIt2>
double CachedTokenSetRatio<CharT1>::similarity(InputIt2 first2, InputIt2 last2, double score_cutoff,
                                               [[maybe_unused]] double score_hint) const
{
    if (score_cutoff > 100) return 0;

    return fuzz_detail::token_set_ratio(tokens_s1, detail::sorted_split(first2, last2), score_cutoff);
}

template <typename CharT1>
template <typename Sentence2>
double CachedTokenSetRatio<CharT1>::similarity(const Sentence2& s2, double score_cutoff,
                                               [[maybe_unused]] double score_hint) const
{
    return similarity(detail::to_begin(s2), detail::to_end(s2), score_cutoff);
}

/**********************************************
 *          partial_token_set_ratio
 *********************************************/

namespace fuzz_detail {
template <typename InputIt1, typename InputIt2>
double partial_token_set_ratio(const rapidfuzz::detail::SplittedSentenceView<InputIt1>& tokens_a,
                               const rapidfuzz::detail::SplittedSentenceView<InputIt2>& tokens_b,
                               const double score_cutoff)
{
    /* in FuzzyWuzzy this returns 0. For sake of compatibility return 0 here as well
     * see https://github.com/rapidfuzz/RapidFuzz/issues/110 */
    if (tokens_a.empty() || tokens_b.empty()) return 0;

    auto decomposition = detail::set_decomposition(tokens_a, tokens_b);

    // exit early when there is a common word in both sequences
    if (!decomposition.intersection.empty()) return 100;

    return partial_ratio(decomposition.difference_ab.join(), decomposition.difference_ba.join(),
                         score_cutoff);
}
} // namespace fuzz_detail

template <typename InputIt1, typename InputIt2>
double partial_token_set_ratio(InputIt1 first1, InputIt1 last1, InputIt2 first2, InputIt2 last2,
                               double score_cutoff)
{
    if (score_cutoff > 100) return 0;

    return fuzz_detail::partial_token_set_ratio(detail::sorted_split(first1, last1),
                                                detail::sorted_split(first2, last2), score_cutoff);
}

template <typename Sentence1, typename Sentence2>
double partial_token_set_ratio(const Sentence1& s1, const Sentence2& s2, double score_cutoff)
{
    return partial_token_set_ratio(detail::to_begin(s1), detail::to_end(s1), detail::to_begin(s2),
                                   detail::to_end(s2), score_cutoff);
}

template <typename CharT1>
template <typename InputIt2>
double CachedPartialTokenSetRatio<CharT1>::similarity(InputIt2 first2, InputIt2 last2, double score_cutoff,
                                                      [[maybe_unused]] double score_hint) const
{
    if (score_cutoff > 100) return 0;

    return fuzz_detail::partial_token_set_ratio(tokens_s1, detail::sorted_split(first2, last2), score_cutoff);
}

template <typename CharT1>
template <typename Sentence2>
double CachedPartialTokenSetRatio<CharT1>::similarity(const Sentence2& s2, double score_cutoff,
                                                      [[maybe_unused]] double score_hint) const
{
    return similarity(detail::to_begin(s2), detail::to_end(s2), score_cutoff);
}

/**********************************************
 *                token_ratio
 *********************************************/

template <typename InputIt1, typename InputIt2>
double token_ratio(InputIt1 first1, InputIt1 last1, InputIt2 first2, InputIt2 last2, double score_cutoff)
{
    if (score_cutoff > 100) return 0;

    auto tokens_a = detail::sorted_split(first1, last1);
    auto tokens_b = detail::sorted_split(first2, last2);

    auto decomposition = detail::set_decomposition(tokens_a, tokens_b);
    auto intersect = decomposition.intersection;
    auto diff_ab = decomposition.difference_ab;
    auto diff_ba = decomposition.difference_ba;

    if (!intersect.empty() && (diff_ab.empty() || diff_ba.empty())) return 100;

    auto diff_ab_joined = diff_ab.join();
    auto diff_ba_joined = diff_ba.join();

    size_t ab_len = diff_ab_joined.size();
    size_t ba_len = diff_ba_joined.size();
    size_t sect_len = intersect.length();

    double result = ratio(tokens_a.join(), tokens_b.join(), score_cutoff);

    // string length sect+ab <-> sect and sect+ba <-> sect
    size_t sect_ab_len = sect_len + bool(sect_len) + ab_len;
    size_t sect_ba_len = sect_len + bool(sect_len) + ba_len;

    size_t cutoff_distance = fuzz_detail::score_cutoff_to_distance(score_cutoff, sect_ab_len + sect_ba_len);
    size_t dist = indel_distance(diff_ab_joined, diff_ba_joined, cutoff_distance);
    if (dist <= cutoff_distance)
        result = std::max(result, fuzz_detail::norm_distance(dist, sect_ab_len + sect_ba_len, score_cutoff));

    // exit early since the other ratios are 0
    if (!sect_len) return result;

    // levenshtein distance sect+ab <-> sect and sect+ba <-> sect
    // since only sect is similar in them the distance can be calculated based on
    // the length difference
    size_t sect_ab_dist = bool(sect_len) + ab_len;
    double sect_ab_ratio = fuzz_detail::norm_distance(sect_ab_dist, sect_len + sect_ab_len, score_cutoff);

    size_t sect_ba_dist = bool(sect_len) + ba_len;
    double sect_ba_ratio = fuzz_detail::norm_distance(sect_ba_dist, sect_len + sect_ba_len, score_cutoff);

    return std::max({result, sect_ab_ratio, sect_ba_ratio});
}

template <typename Sentence1, typename Sentence2>
double token_ratio(const Sentence1& s1, const Sentence2& s2, double score_cutoff)
{
    return token_ratio(detail::to_begin(s1), detail::to_end(s1), detail::to_begin(s2), detail::to_end(s2),
                       score_cutoff);
}

namespace fuzz_detail {
template <typename CharT1, typename CachedCharT1, typename InputIt2>
double token_ratio(const rapidfuzz::detail::SplittedSentenceView<CharT1>& s1_tokens,
                   const CachedRatio<CachedCharT1>& cached_ratio_s1_sorted, InputIt2 first2, InputIt2 last2,
                   double score_cutoff)
{
    if (score_cutoff > 100) return 0;

    auto s2_tokens = detail::sorted_split(first2, last2);

    auto decomposition = detail::set_decomposition(s1_tokens, s2_tokens);
    auto intersect = decomposition.intersection;
    auto diff_ab = decomposition.difference_ab;
    auto diff_ba = decomposition.difference_ba;

    if (!intersect.empty() && (diff_ab.empty() || diff_ba.empty())) return 100;

    auto diff_ab_joined = diff_ab.join();
    auto diff_ba_joined = diff_ba.join();

    size_t ab_len = diff_ab_joined.size();
    size_t ba_len = diff_ba_joined.size();
    size_t sect_len = intersect.length();

    double result = cached_ratio_s1_sorted.similarity(s2_tokens.join(), score_cutoff);

    // string length sect+ab <-> sect and sect+ba <-> sect
    size_t sect_ab_len = sect_len + bool(sect_len) + ab_len;
    size_t sect_ba_len = sect_len + bool(sect_len) + ba_len;

    size_t cutoff_distance = score_cutoff_to_distance(score_cutoff, sect_ab_len + sect_ba_len);
    size_t dist = indel_distance(diff_ab_joined, diff_ba_joined, cutoff_distance);
    if (dist <= cutoff_distance)
        result = std::max(result, norm_distance(dist, sect_ab_len + sect_ba_len, score_cutoff));

    // exit early since the other ratios are 0
    if (!sect_len) return result;

    // levenshtein distance sect+ab <-> sect and sect+ba <-> sect
    // since only sect is similar in them the distance can be calculated based on
    // the length difference
    size_t sect_ab_dist = bool(sect_len) + ab_len;
    double sect_ab_ratio = norm_distance(sect_ab_dist, sect_len + sect_ab_len, score_cutoff);

    size_t sect_ba_dist = bool(sect_len) + ba_len;
    double sect_ba_ratio = norm_distance(sect_ba_dist, sect_len + sect_ba_len, score_cutoff);

    return std::max({result, sect_ab_ratio, sect_ba_ratio});
}

// todo this is a temporary solution until WRatio is properly implemented using other scorers
template <typename CharT1, typename InputIt1, typename InputIt2>
double token_ratio(const std::vector<CharT1>& s1_sorted,
                   const rapidfuzz::detail::SplittedSentenceView<InputIt1>& tokens_s1,
                   const detail::BlockPatternMatchVector& blockmap_s1_sorted, InputIt2 first2, InputIt2 last2,
                   double score_cutoff)
{
    if (score_cutoff > 100) return 0;

    auto tokens_b = detail::sorted_split(first2, last2);

    auto decomposition = detail::set_decomposition(tokens_s1, tokens_b);
    auto intersect = decomposition.intersection;
    auto diff_ab = decomposition.difference_ab;
    auto diff_ba = decomposition.difference_ba;

    if (!intersect.empty() && (diff_ab.empty() || diff_ba.empty())) return 100;

    auto diff_ab_joined = diff_ab.join();
    auto diff_ba_joined = diff_ba.join();

    size_t ab_len = diff_ab_joined.size();
    size_t ba_len = diff_ba_joined.size();
    size_t sect_len = intersect.length();

    double result = 0;
    auto s2_sorted = tokens_b.join();
    if (s1_sorted.size() < 65) {
        double norm_sim = detail::indel_normalized_similarity(blockmap_s1_sorted, detail::Range(s1_sorted),
                                                              detail::Range(s2_sorted), score_cutoff / 100);
        result = norm_sim * 100;
    }
    else {
        result = fuzz::ratio(s1_sorted, s2_sorted, score_cutoff);
    }

    // string length sect+ab <-> sect and sect+ba <-> sect
    size_t sect_ab_len = sect_len + bool(sect_len) + ab_len;
    size_t sect_ba_len = sect_len + bool(sect_len) + ba_len;

    size_t cutoff_distance = score_cutoff_to_distance(score_cutoff, sect_ab_len + sect_ba_len);
    size_t dist = indel_distance(diff_ab_joined, diff_ba_joined, cutoff_distance);
    if (dist <= cutoff_distance)
        result = std::max(result, norm_distance(dist, sect_ab_len + sect_ba_len, score_cutoff));

    // exit early since the other ratios are 0
    if (!sect_len) return result;

    // levenshtein distance sect+ab <-> sect and sect+ba <-> sect
    // since only sect is similar in them the distance can be calculated based on
    // the length difference
    size_t sect_ab_dist = bool(sect_len) + ab_len;
    double sect_ab_ratio = norm_distance(sect_ab_dist, sect_len + sect_ab_len, score_cutoff);

    size_t sect_ba_dist = bool(sect_len) + ba_len;
    double sect_ba_ratio = norm_distance(sect_ba_dist, sect_len + sect_ba_len, score_cutoff);

    return std::max({result, sect_ab_ratio, sect_ba_ratio});
}
} // namespace fuzz_detail

template <typename CharT1>
template <typename InputIt2>
double CachedTokenRatio<CharT1>::similarity(InputIt2 first2, InputIt2 last2, double score_cutoff,
                                            [[maybe_unused]] double score_hint) const
{
    return fuzz_detail::token_ratio(s1_tokens, cached_ratio_s1_sorted, first2, last2, score_cutoff);
}

template <typename CharT1>
template <typename Sentence2>
double CachedTokenRatio<CharT1>::similarity(const Sentence2& s2, double score_cutoff,
                                            [[maybe_unused]] double score_hint) const
{
    return similarity(detail::to_begin(s2), detail::to_end(s2), score_cutoff);
}

/**********************************************
 *            partial_token_ratio
 *********************************************/

template <typename InputIt1, typename InputIt2>
double partial_token_ratio(InputIt1 first1, InputIt1 last1, InputIt2 first2, InputIt2 last2,
                           double score_cutoff)
{
    if (score_cutoff > 100) return 0;

    auto tokens_a = detail::sorted_split(first1, last1);
    auto tokens_b = detail::sorted_split(first2, last2);

    auto decomposition = detail::set_decomposition(tokens_a, tokens_b);

    // exit early when there is a common word in both sequences
    if (!decomposition.intersection.empty()) return 100;

    auto diff_ab = decomposition.difference_ab;
    auto diff_ba = decomposition.difference_ba;

    double result = partial_ratio(tokens_a.join(), tokens_b.join(), score_cutoff);

    // do not calculate the same partial_ratio twice
    if (tokens_a.word_count() == diff_ab.word_count() && tokens_b.word_count() == diff_ba.word_count()) {
        return result;
    }

    score_cutoff = std::max(score_cutoff, result);
    return std::max(result, partial_ratio(diff_ab.join(), diff_ba.join(), score_cutoff));
}

template <typename Sentence1, typename Sentence2>
double partial_token_ratio(const Sentence1& s1, const Sentence2& s2, double score_cutoff)
{
    return partial_token_ratio(detail::to_begin(s1), detail::to_end(s1), detail::to_begin(s2),
                               detail::to_end(s2), score_cutoff);
}

namespace fuzz_detail {
template <typename CharT1, typename InputIt1, typename InputIt2>
double partial_token_ratio(const std::vector<CharT1>& s1_sorted,
                           const rapidfuzz::detail::SplittedSentenceView<InputIt1>& tokens_s1,
                           InputIt2 first2, InputIt2 last2, double score_cutoff)
{
    if (score_cutoff > 100) return 0;

    auto tokens_b = detail::sorted_split(first2, last2);

    auto decomposition = detail::set_decomposition(tokens_s1, tokens_b);

    // exit early when there is a common word in both sequences
    if (!decomposition.intersection.empty()) return 100;

    auto diff_ab = decomposition.difference_ab;
    auto diff_ba = decomposition.difference_ba;

    double result = partial_ratio(s1_sorted, tokens_b.join(), score_cutoff);

    // do not calculate the same partial_ratio twice
    if (tokens_s1.word_count() == diff_ab.word_count() && tokens_b.word_count() == diff_ba.word_count()) {
        return result;
    }

    score_cutoff = std::max(score_cutoff, result);
    return std::max(result, partial_ratio(diff_ab.join(), diff_ba.join(), score_cutoff));
}

} // namespace fuzz_detail

template <typename CharT1>
template <typename InputIt2>
double CachedPartialTokenRatio<CharT1>::similarity(InputIt2 first2, InputIt2 last2, double score_cutoff,
                                                   [[maybe_unused]] double score_hint) const
{
    return fuzz_detail::partial_token_ratio(s1_sorted, tokens_s1, first2, last2, score_cutoff);
}

template <typename CharT1>
template <typename Sentence2>
double CachedPartialTokenRatio<CharT1>::similarity(const Sentence2& s2, double score_cutoff,
                                                   [[maybe_unused]] double score_hint) const
{
    return similarity(detail::to_begin(s2), detail::to_end(s2), score_cutoff);
}

/**********************************************
 *                  WRatio
 *********************************************/

template <typename InputIt1, typename InputIt2>
double WRatio(InputIt1 first1, InputIt1 last1, InputIt2 first2, InputIt2 last2, double score_cutoff)
{
    if (score_cutoff > 100) return 0;

    constexpr double UNBASE_SCALE = 0.95;

    auto len1 = std::distance(first1, last1);
    auto len2 = std::distance(first2, last2);

    /* in FuzzyWuzzy this returns 0. For sake of compatibility return 0 here as well
     * see https://github.com/rapidfuzz/RapidFuzz/issues/110 */
    if (!len1 || !len2) return 0;

    double len_ratio = (len1 > len2) ? static_cast<double>(len1) / static_cast<double>(len2)
                                     : static_cast<double>(len2) / static_cast<double>(len1);

    double end_ratio = ratio(first1, last1, first2, last2, score_cutoff);

    if (len_ratio < 1.5) {
        score_cutoff = std::max(score_cutoff, end_ratio) / UNBASE_SCALE;
        return std::max(end_ratio, token_ratio(first1, last1, first2, last2, score_cutoff) * UNBASE_SCALE);
    }

    const double PARTIAL_SCALE = (len_ratio < 8.0) ? 0.9 : 0.6;

    score_cutoff = std::max(score_cutoff, end_ratio) / PARTIAL_SCALE;
    end_ratio =
        std::max(end_ratio, partial_ratio(first1, last1, first2, last2, score_cutoff) * PARTIAL_SCALE);

    score_cutoff = std::max(score_cutoff, end_ratio) / UNBASE_SCALE;
    return std::max(end_ratio, partial_token_ratio(first1, last1, first2, last2, score_cutoff) *
                                   UNBASE_SCALE * PARTIAL_SCALE);
}

template <typename Sentence1, typename Sentence2>
double WRatio(const Sentence1& s1, const Sentence2& s2, double score_cutoff)
{
    return WRatio(detail::to_begin(s1), detail::to_end(s1), detail::to_begin(s2), detail::to_end(s2),
                  score_cutoff);
}

template <typename Sentence1>
template <typename InputIt1>
CachedWRatio<Sentence1>::CachedWRatio(InputIt1 first1, InputIt1 last1)
    : s1(first1, last1),
      cached_partial_ratio(first1, last1),
      tokens_s1(detail::sorted_split(std::begin(s1), std::end(s1))),
      s1_sorted(tokens_s1.join()),
      blockmap_s1_sorted(detail::Range(s1_sorted))
{}

template <typename CharT1>
template <typename InputIt2>
double CachedWRatio<CharT1>::similarity(InputIt2 first2, InputIt2 last2, double score_cutoff,
                                        [[maybe_unused]] double score_hint) const
{
    if (score_cutoff > 100) return 0;

    constexpr double UNBASE_SCALE = 0.95;

    size_t len1 = s1.size();
    size_t len2 = static_cast<size_t>(std::distance(first2, last2));

    /* in FuzzyWuzzy this returns 0. For sake of compatibility return 0 here as well
     * see https://github.com/rapidfuzz/RapidFuzz/issues/110 */
    if (!len1 || !len2) return 0;

    double len_ratio = (len1 > len2) ? static_cast<double>(len1) / static_cast<double>(len2)
                                     : static_cast<double>(len2) / static_cast<double>(len1);

    double end_ratio = cached_partial_ratio.cached_ratio.similarity(first2, last2, score_cutoff);

    if (len_ratio < 1.5) {
        score_cutoff = std::max(score_cutoff, end_ratio) / UNBASE_SCALE;
        // use pre calculated values
        auto r =
            fuzz_detail::token_ratio(s1_sorted, tokens_s1, blockmap_s1_sorted, first2, last2, score_cutoff);
        return std::max(end_ratio, r * UNBASE_SCALE);
    }

    const double PARTIAL_SCALE = (len_ratio < 8.0) ? 0.9 : 0.6;

    score_cutoff = std::max(score_cutoff, end_ratio) / PARTIAL_SCALE;
    end_ratio =
        std::max(end_ratio, cached_partial_ratio.similarity(first2, last2, score_cutoff) * PARTIAL_SCALE);

    score_cutoff = std::max(score_cutoff, end_ratio) / UNBASE_SCALE;
    auto r = fuzz_detail::partial_token_ratio(s1_sorted, tokens_s1, first2, last2, score_cutoff);
    return std::max(end_ratio, r * UNBASE_SCALE * PARTIAL_SCALE);
}

template <typename CharT1>
template <typename Sentence2>
double CachedWRatio<CharT1>::similarity(const Sentence2& s2, double score_cutoff,
                                        [[maybe_unused]] double score_hint) const
{
    return similarity(detail::to_begin(s2), detail::to_end(s2), score_cutoff);
}

/**********************************************
 *                QRatio
 *********************************************/

template <typename InputIt1, typename InputIt2>
double QRatio(InputIt1 first1, InputIt1 last1, InputIt2 first2, InputIt2 last2, double score_cutoff)
{
    ptrdiff_t len1 = std::distance(first1, last1);
    ptrdiff_t len2 = std::distance(first2, last2);

    /* in FuzzyWuzzy this returns 0. For sake of compatibility return 0 here as well
     * see https://github.com/rapidfuzz/RapidFuzz/issues/110 */
    if (!len1 || !len2) return 0;

    return ratio(first1, last1, first2, last2, score_cutoff);
}

template <typename Sentence1, typename Sentence2>
double QRatio(const Sentence1& s1, const Sentence2& s2, double score_cutoff)
{
    return QRatio(detail::to_begin(s1), detail::to_end(s1), detail::to_begin(s2), detail::to_end(s2),
                  score_cutoff);
}

template <typename CharT1>
template <typename InputIt2>
double CachedQRatio<CharT1>::similarity(InputIt2 first2, InputIt2 last2, double score_cutoff,
                                        [[maybe_unused]] double score_hint) const
{
    auto len2 = std::distance(first2, last2);

    /* in FuzzyWuzzy this returns 0. For sake of compatibility return 0 here as well
     * see https://github.com/rapidfuzz/RapidFuzz/issues/110 */
    if (s1.empty() || !len2) return 0;

    return cached_ratio.similarity(first2, last2, score_cutoff);
}

template <typename CharT1>
template <typename Sentence2>
double CachedQRatio<CharT1>::similarity(const Sentence2& s2, double score_cutoff,
                                        [[maybe_unused]] double score_hint) const
{
    return similarity(detail::to_begin(s2), detail::to_end(s2), score_cutoff);
}

} // namespace duckdb_rapidfuzz::fuzz
