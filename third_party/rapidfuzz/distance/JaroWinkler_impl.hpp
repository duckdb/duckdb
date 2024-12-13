/* SPDX-License-Identifier: MIT */
/* Copyright © 2022-present Max Bachmann */

#include <rapidfuzz/distance/Jaro.hpp>

namespace duckdb_rapidfuzz::detail {

template <typename InputIt1, typename InputIt2>
double jaro_winkler_similarity(const Range<InputIt1>& P, const Range<InputIt2>& T, double prefix_weight,
                               double score_cutoff)
{
    size_t P_len = P.size();
    size_t T_len = T.size();
    size_t min_len = std::min(P_len, T_len);
    size_t prefix = 0;
    size_t max_prefix = std::min(min_len, size_t(4));

    for (; prefix < max_prefix; ++prefix)
        if (T[prefix] != P[prefix]) break;

    double jaro_score_cutoff = score_cutoff;
    if (jaro_score_cutoff > 0.7) {
        double prefix_sim = static_cast<double>(prefix) * prefix_weight;

        if (prefix_sim >= 1.0)
            jaro_score_cutoff = 0.7;
        else
            jaro_score_cutoff = std::max(0.7, (prefix_sim - jaro_score_cutoff) / (prefix_sim - 1.0));
    }

    double Sim = jaro_similarity(P, T, jaro_score_cutoff);
    if (Sim > 0.7) {
        Sim += static_cast<double>(prefix) * prefix_weight * (1.0 - Sim);
        Sim = std::min(Sim, 1.0);
    }

    return (Sim >= score_cutoff) ? Sim : 0;
}

template <typename InputIt1, typename InputIt2>
double jaro_winkler_similarity(const BlockPatternMatchVector& PM, const Range<InputIt1>& P,
                               const Range<InputIt2>& T, double prefix_weight, double score_cutoff)
{
    size_t P_len = P.size();
    size_t T_len = T.size();
    size_t min_len = std::min(P_len, T_len);
    size_t prefix = 0;
    size_t max_prefix = std::min(min_len, size_t(4));

    for (; prefix < max_prefix; ++prefix)
        if (T[prefix] != P[prefix]) break;

    double jaro_score_cutoff = score_cutoff;
    if (jaro_score_cutoff > 0.7) {
        double prefix_sim = static_cast<double>(prefix) * prefix_weight;

        if (prefix_sim >= 1.0)
            jaro_score_cutoff = 0.7;
        else
            jaro_score_cutoff = std::max(0.7, (prefix_sim - jaro_score_cutoff) / (prefix_sim - 1.0));
    }

    double Sim = jaro_similarity(PM, P, T, jaro_score_cutoff);
    if (Sim > 0.7) {
        Sim += static_cast<double>(prefix) * prefix_weight * (1.0 - Sim);
        Sim = std::min(Sim, 1.0);
    }

    return (Sim >= score_cutoff) ? Sim : 0;
}

class JaroWinkler : public SimilarityBase<JaroWinkler, double, 0, 1, double> {
    friend SimilarityBase<JaroWinkler, double, 0, 1, double>;
    friend NormalizedMetricBase<JaroWinkler, double>;

    template <typename InputIt1, typename InputIt2>
    static double maximum(const Range<InputIt1>&, const Range<InputIt2>&, double) noexcept
    {
        return 1.0;
    }

    template <typename InputIt1, typename InputIt2>
    static double _similarity(const Range<InputIt1>& s1, const Range<InputIt2>& s2, double prefix_weight,
                              double score_cutoff, [[maybe_unused]] double score_hint)
    {
        return jaro_winkler_similarity(s1, s2, prefix_weight, score_cutoff);
    }
};

} // namespace duckdb_rapidfuzz::detail
