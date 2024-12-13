/* SPDX-License-Identifier: MIT */
/* Copyright © 2021 Max Bachmann */

#pragma once
#include <rapidfuzz/details/Range.hpp>
#include <rapidfuzz/details/common.hpp>
#include <rapidfuzz/details/distance.hpp>

namespace duckdb_rapidfuzz::detail {

class Prefix : public SimilarityBase<Prefix, size_t, 0, std::numeric_limits<int64_t>::max()> {
    friend SimilarityBase<Prefix, size_t, 0, std::numeric_limits<int64_t>::max()>;
    friend NormalizedMetricBase<Prefix>;

    template <typename InputIt1, typename InputIt2>
    static size_t maximum(const Range<InputIt1>& s1, const Range<InputIt2>& s2)
    {
        return std::max(s1.size(), s2.size());
    }

    template <typename InputIt1, typename InputIt2>
    static size_t _similarity(Range<InputIt1> s1, Range<InputIt2> s2, size_t score_cutoff,
                              [[maybe_unused]] size_t score_hint)
    {
        size_t dist = remove_common_prefix(s1, s2);
        return (dist >= score_cutoff) ? dist : 0;
    }
};

} // namespace duckdb_rapidfuzz::detail
