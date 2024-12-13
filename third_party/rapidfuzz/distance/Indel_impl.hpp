/* SPDX-License-Identifier: MIT */
/* Copyright © 2022-present Max Bachmann */

#include <rapidfuzz/details/PatternMatchVector.hpp>
#include <rapidfuzz/details/Range.hpp>
#include <rapidfuzz/details/common.hpp>
#include <rapidfuzz/details/distance.hpp>
#include <rapidfuzz/details/intrinsics.hpp>
#include <rapidfuzz/distance/LCSseq.hpp>

namespace duckdb_rapidfuzz::detail {

template <typename InputIt1, typename InputIt2>
size_t indel_distance(const BlockPatternMatchVector& block, const Range<InputIt1>& s1,
                      const Range<InputIt2>& s2, size_t score_cutoff)
{
    size_t maximum = s1.size() + s2.size();
    size_t lcs_cutoff = (maximum / 2 >= score_cutoff) ? maximum / 2 - score_cutoff : 0;
    size_t lcs_sim = lcs_seq_similarity(block, s1, s2, lcs_cutoff);
    size_t dist = maximum - 2 * lcs_sim;
    return (dist <= score_cutoff) ? dist : score_cutoff + 1;
}

template <typename InputIt1, typename InputIt2>
double indel_normalized_distance(const BlockPatternMatchVector& block, const Range<InputIt1>& s1,
                                 const Range<InputIt2>& s2, double score_cutoff)
{
    size_t maximum = s1.size() + s2.size();
    size_t cutoff_distance = static_cast<size_t>(std::ceil(static_cast<double>(maximum) * score_cutoff));
    size_t dist = indel_distance(block, s1, s2, cutoff_distance);
    double norm_dist = (maximum) ? static_cast<double>(dist) / static_cast<double>(maximum) : 0.0;
    return (norm_dist <= score_cutoff) ? norm_dist : 1.0;
}

template <typename InputIt1, typename InputIt2>
double indel_normalized_similarity(const BlockPatternMatchVector& block, const Range<InputIt1>& s1,
                                   const Range<InputIt2>& s2, double score_cutoff)
{
    double cutoff_score = NormSim_to_NormDist(score_cutoff);
    double norm_dist = indel_normalized_distance(block, s1, s2, cutoff_score);
    double norm_sim = 1.0 - norm_dist;
    return (norm_sim >= score_cutoff) ? norm_sim : 0.0;
}

class Indel : public DistanceBase<Indel, size_t, 0, std::numeric_limits<int64_t>::max()> {
    friend DistanceBase<Indel, size_t, 0, std::numeric_limits<int64_t>::max()>;
    friend NormalizedMetricBase<Indel>;

    template <typename InputIt1, typename InputIt2>
    static size_t maximum(const Range<InputIt1>& s1, const Range<InputIt2>& s2)
    {
        return s1.size() + s2.size();
    }

    template <typename InputIt1, typename InputIt2>
    static size_t _distance(const Range<InputIt1>& s1, const Range<InputIt2>& s2, size_t score_cutoff,
                            size_t score_hint)
    {
        size_t maximum = Indel::maximum(s1, s2);
        size_t lcs_cutoff = (maximum / 2 >= score_cutoff) ? maximum / 2 - score_cutoff : 0;
        size_t lcs_hint = (maximum / 2 >= score_hint) ? maximum / 2 - score_hint : 0;
        size_t lcs_sim = LCSseq::similarity(s1, s2, lcs_cutoff, lcs_hint);
        size_t dist = maximum - 2 * lcs_sim;
        return (dist <= score_cutoff) ? dist : score_cutoff + 1;
    }
};

} // namespace duckdb_rapidfuzz::detail
