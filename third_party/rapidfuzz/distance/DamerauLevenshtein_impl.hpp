/* SPDX-License-Identifier: MIT */
/* Copyright © 2022-present Max Bachmann */

#include <cassert>
#include <cstddef>
#include <limits>
#include <numeric>
#include <rapidfuzz/details/GrowingHashmap.hpp>
#include <rapidfuzz/details/Matrix.hpp>
#include <rapidfuzz/details/Range.hpp>
#include <rapidfuzz/details/common.hpp>
#include <rapidfuzz/details/distance.hpp>

namespace duckdb_rapidfuzz::detail {

template <typename IntType>
struct RowId {
    IntType val = -1;
    friend bool operator==(const RowId& lhs, const RowId& rhs)
    {
        return lhs.val == rhs.val;
    }

    friend bool operator!=(const RowId& lhs, const RowId& rhs)
    {
        return !(lhs == rhs);
    }
};

/*
 * based on the paper
 * "Linear space string correction algorithm using the Damerau-Levenshtein distance"
 * from Chunchun Zhao and Sartaj Sahni
 */
template <typename IntType, typename InputIt1, typename InputIt2>
size_t damerau_levenshtein_distance_zhao(const Range<InputIt1>& s1, const Range<InputIt2>& s2, size_t max)
{
    // todo check types
    IntType len1 = static_cast<IntType>(s1.size());
    IntType len2 = static_cast<IntType>(s2.size());
    IntType maxVal = static_cast<IntType>(std::max(len1, len2) + 1);
    assert(std::numeric_limits<IntType>::max() > maxVal);

    HybridGrowingHashmap<typename Range<InputIt1>::value_type, RowId<IntType>> last_row_id;
    size_t size = s2.size() + 2;
    assume(size != 0);
    std::vector<IntType> FR_arr(size, maxVal);
    std::vector<IntType> R1_arr(size, maxVal);
    std::vector<IntType> R_arr(size);
    R_arr[0] = maxVal;
    std::iota(R_arr.begin() + 1, R_arr.end(), IntType(0));

    IntType* R = &R_arr[1];
    IntType* R1 = &R1_arr[1];
    IntType* FR = &FR_arr[1];

    auto iter_s1 = s1.begin();
    for (IntType i = 1; i <= len1; i++) {
        std::swap(R, R1);
        IntType last_col_id = -1;
        IntType last_i2l1 = R[0];
        R[0] = i;
        IntType T = maxVal;

        auto iter_s2 = s2.begin();
        for (IntType j = 1; j <= len2; j++) {
            int64_t diag = R1[j - 1] + static_cast<IntType>(*iter_s1 != *iter_s2);
            int64_t left = R[j - 1] + 1;
            int64_t up = R1[j] + 1;
            int64_t temp = std::min({diag, left, up});

            if (*iter_s1 == *iter_s2) {
                last_col_id = j;   // last occurence of s1_i
                FR[j] = R1[j - 2]; // save H_k-1,j-2
                T = last_i2l1;     // save H_i-2,l-1
            }
            else {
                int64_t k = last_row_id.get(static_cast<uint64_t>(*iter_s2)).val;
                int64_t l = last_col_id;

                if ((j - l) == 1) {
                    int64_t transpose = FR[j] + (i - k);
                    temp = std::min(temp, transpose);
                }
                else if ((i - k) == 1) {
                    int64_t transpose = T + (j - l);
                    temp = std::min(temp, transpose);
                }
            }

            last_i2l1 = R[j];
            R[j] = static_cast<IntType>(temp);
            iter_s2++;
        }
        last_row_id[*iter_s1].val = i;
        iter_s1++;
    }

    size_t dist = static_cast<size_t>(R[s2.size()]);
    return (dist <= max) ? dist : max + 1;
}

template <typename InputIt1, typename InputIt2>
size_t damerau_levenshtein_distance(Range<InputIt1> s1, Range<InputIt2> s2, size_t max)
{
    size_t min_edits = abs_diff(s1.size(), s2.size());
    if (min_edits > max) return max + 1;

    /* common affix does not effect Levenshtein distance */
    remove_common_affix(s1, s2);

    size_t maxVal = std::max(s1.size(), s2.size()) + 1;
    if (std::numeric_limits<int16_t>::max() > maxVal)
        return damerau_levenshtein_distance_zhao<int16_t>(s1, s2, max);
    else if (std::numeric_limits<int32_t>::max() > maxVal)
        return damerau_levenshtein_distance_zhao<int32_t>(s1, s2, max);
    else
        return damerau_levenshtein_distance_zhao<int64_t>(s1, s2, max);
}

class DamerauLevenshtein
    : public DistanceBase<DamerauLevenshtein, size_t, 0, std::numeric_limits<int64_t>::max()> {
    friend DistanceBase<DamerauLevenshtein, size_t, 0, std::numeric_limits<int64_t>::max()>;
    friend NormalizedMetricBase<DamerauLevenshtein>;

    template <typename InputIt1, typename InputIt2>
    static size_t maximum(const Range<InputIt1>& s1, const Range<InputIt2>& s2)
    {
        return std::max(s1.size(), s2.size());
    }

    template <typename InputIt1, typename InputIt2>
    static size_t _distance(const Range<InputIt1>& s1, const Range<InputIt2>& s2, size_t score_cutoff,
                            [[maybe_unused]] size_t score_hint)
    {
        return damerau_levenshtein_distance(s1, s2, score_cutoff);
    }
};

} // namespace duckdb_rapidfuzz::detail