/* SPDX-License-Identifier: MIT */
/* Copyright © 2022-present Max Bachmann */

#pragma once

#include <rapidfuzz/details/Range.hpp>
#include <rapidfuzz/distance/JaroWinkler_impl.hpp>

namespace duckdb_rapidfuzz {

template <typename InputIt1, typename InputIt2,
          typename = std::enable_if_t<!std::is_same_v<InputIt2, double>>>
double jaro_winkler_distance(InputIt1 first1, InputIt1 last1, InputIt2 first2, InputIt2 last2,
                             double prefix_weight = 0.1, double score_cutoff = 1.0)
{
    return detail::JaroWinkler::distance(first1, last1, first2, last2, prefix_weight, score_cutoff,
                                         score_cutoff);
}

template <typename Sentence1, typename Sentence2>
double jaro_winkler_distance(const Sentence1& s1, const Sentence2& s2, double prefix_weight = 0.1,
                             double score_cutoff = 1.0)
{
    return detail::JaroWinkler::distance(s1, s2, prefix_weight, score_cutoff, score_cutoff);
}

template <typename InputIt1, typename InputIt2,
          typename = std::enable_if_t<!std::is_same_v<InputIt2, double>>>
double jaro_winkler_similarity(InputIt1 first1, InputIt1 last1, InputIt2 first2, InputIt2 last2,
                               double prefix_weight = 0.1, double score_cutoff = 0.0)
{
    return detail::JaroWinkler::similarity(first1, last1, first2, last2, prefix_weight, score_cutoff,
                                           score_cutoff);
}

template <typename Sentence1, typename Sentence2>
double jaro_winkler_similarity(const Sentence1& s1, const Sentence2& s2, double prefix_weight = 0.1,
                               double score_cutoff = 0.0)
{
    return detail::JaroWinkler::similarity(s1, s2, prefix_weight, score_cutoff, score_cutoff);
}

template <typename InputIt1, typename InputIt2,
          typename = std::enable_if_t<!std::is_same_v<InputIt2, double>>>
double jaro_winkler_normalized_distance(InputIt1 first1, InputIt1 last1, InputIt2 first2, InputIt2 last2,
                                        double prefix_weight = 0.1, double score_cutoff = 1.0)
{
    return detail::JaroWinkler::normalized_distance(first1, last1, first2, last2, prefix_weight, score_cutoff,
                                                    score_cutoff);
}

template <typename Sentence1, typename Sentence2>
double jaro_winkler_normalized_distance(const Sentence1& s1, const Sentence2& s2, double prefix_weight = 0.1,
                                        double score_cutoff = 1.0)
{
    return detail::JaroWinkler::normalized_distance(s1, s2, prefix_weight, score_cutoff, score_cutoff);
}

template <typename InputIt1, typename InputIt2,
          typename = std::enable_if_t<!std::is_same_v<InputIt2, double>>>
double jaro_winkler_normalized_similarity(InputIt1 first1, InputIt1 last1, InputIt2 first2, InputIt2 last2,
                                          double prefix_weight = 0.1, double score_cutoff = 0.0)
{
    return detail::JaroWinkler::normalized_similarity(first1, last1, first2, last2, prefix_weight,
                                                      score_cutoff, score_cutoff);
}

template <typename Sentence1, typename Sentence2>
double jaro_winkler_normalized_similarity(const Sentence1& s1, const Sentence2& s2,
                                          double prefix_weight = 0.1, double score_cutoff = 0.0)
{
    return detail::JaroWinkler::normalized_similarity(s1, s2, prefix_weight, score_cutoff, score_cutoff);
}

#ifdef RAPIDFUZZ_SIMD
namespace experimental {
template <int MaxLen>
struct MultiJaroWinkler : public detail::MultiSimilarityBase<MultiJaroWinkler<MaxLen>, double, 0, 1> {

private:
    friend detail::MultiSimilarityBase<MultiJaroWinkler<MaxLen>, double, 0, 1>;
    friend detail::MultiNormalizedMetricBase<MultiJaroWinkler<MaxLen>, double>;

public:
    MultiJaroWinkler(size_t count, double prefix_weight_ = 0.1) : scorer(count), prefix_weight(prefix_weight_)
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
        size_t len = static_cast<size_t>(std::distance(first1, last1));
        std::array<uint64_t, 4> prefix;
        for (size_t i = 0; i < std::min(len, size_t(4)); ++i)
            prefix[i] = static_cast<uint64_t>(first1[static_cast<ptrdiff_t>(i)]);

        str_lens.push_back(len);
        prefixes.push_back(prefix);
    }

private:
    template <typename InputIt2>
    void _similarity(double* scores, size_t score_count, const detail::Range<InputIt2>& s2,
                     double score_cutoff = 0.0) const
    {
        if (score_count < result_count())
            throw std::invalid_argument("scores has to have >= result_count() elements");

        scorer.similarity(scores, score_count, s2, std::min(0.7, score_cutoff));

        for (size_t i = 0; i < get_input_count(); ++i) {
            if (scores[i] > 0.7) {
                size_t min_len = std::min(s2.size(), str_lens[i]);
                size_t max_prefix = std::min(min_len, size_t(4));
                size_t prefix = 0;
                for (; prefix < max_prefix; ++prefix)
                    if (static_cast<uint64_t>(s2[prefix]) != prefixes[i][prefix]) break;

                scores[i] += static_cast<double>(prefix) * prefix_weight * (1.0 - scores[i]);
                scores[i] = std::min(scores[i], 1.0);
            }

            if (scores[i] < score_cutoff) scores[i] = 0.0;
        }
    }

    template <typename InputIt2>
    double maximum([[maybe_unused]] size_t s1_idx, const detail::Range<InputIt2>&) const
    {
        return 1.0;
    }

    size_t get_input_count() const noexcept
    {
        return str_lens.size();
    }

    std::vector<size_t> str_lens;
    // todo this could lead to incorrect results when comparing uint64_t with int64_t
    std::vector<std::array<uint64_t, 4>> prefixes;
    MultiJaro<MaxLen> scorer;
    double prefix_weight;
};

} /* namespace experimental */
#endif /* RAPIDFUZZ_SIMD */

template <typename CharT1>
struct CachedJaroWinkler : public detail::CachedSimilarityBase<CachedJaroWinkler<CharT1>, double, 0, 1> {
    template <typename Sentence1>
    explicit CachedJaroWinkler(const Sentence1& s1_, double _prefix_weight = 0.1)
        : CachedJaroWinkler(detail::to_begin(s1_), detail::to_end(s1_), _prefix_weight)
    {}

    template <typename InputIt1>
    CachedJaroWinkler(InputIt1 first1, InputIt1 last1, double _prefix_weight = 0.1)
        : prefix_weight(_prefix_weight), s1(first1, last1), PM(detail::Range(first1, last1))
    {}

private:
    friend detail::CachedSimilarityBase<CachedJaroWinkler<CharT1>, double, 0, 1>;
    friend detail::CachedNormalizedMetricBase<CachedJaroWinkler<CharT1>>;

    template <typename InputIt2>
    double maximum(const detail::Range<InputIt2>&) const
    {
        return 1.0;
    }

    template <typename InputIt2>
    double _similarity(const detail::Range<InputIt2>& s2, double score_cutoff,
                       [[maybe_unused]] double score_hint) const
    {
        return detail::jaro_winkler_similarity(PM, detail::Range(s1), s2, prefix_weight, score_cutoff);
    }

    double prefix_weight;
    std::vector<CharT1> s1;
    detail::BlockPatternMatchVector PM;
};

template <typename Sentence1>
explicit CachedJaroWinkler(const Sentence1& s1_,
                           double _prefix_weight = 0.1) -> CachedJaroWinkler<char_type<Sentence1>>;

template <typename InputIt1>
CachedJaroWinkler(InputIt1 first1, InputIt1 last1,
                  double _prefix_weight = 0.1) -> CachedJaroWinkler<iter_value_t<InputIt1>>;

} // namespace duckdb_rapidfuzz
