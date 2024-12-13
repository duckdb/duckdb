
/* SPDX-License-Identifier: MIT */
/* Copyright © 2022-present Max Bachmann */

#pragma once
#include <cstdint>
#include <rapidfuzz/details/PatternMatchVector.hpp>
#include <rapidfuzz/details/Range.hpp>
#include <rapidfuzz/details/common.hpp>
#include <rapidfuzz/details/distance.hpp>
#include <rapidfuzz/details/simd.hpp>

namespace duckdb_rapidfuzz::detail {

/**
 * @brief Bitparallel implementation of the OSA distance.
 *
 * This implementation requires the first string to have a length <= 64.
 * The algorithm used is described @cite hyrro_2002 and has a time complexity
 * of O(N). Comments and variable names in the implementation follow the
 * paper. This implementation is used internally when the strings are short enough
 *
 * @tparam CharT1 This is the char type of the first sentence
 * @tparam CharT2 This is the char type of the second sentence
 *
 * @param s1
 *   string to compare with s2 (for type info check Template parameters above)
 * @param s2
 *   string to compare with s1 (for type info check Template parameters above)
 *
 * @return returns the OSA distance between s1 and s2
 */
template <typename PM_Vec, typename InputIt1, typename InputIt2>
size_t osa_hyrroe2003(const PM_Vec& PM, const Range<InputIt1>& s1, const Range<InputIt2>& s2, size_t max)
{
    /* VP is set to 1^m. Shifting by bitwidth would be undefined behavior */
    uint64_t VP = ~UINT64_C(0);
    uint64_t VN = 0;
    uint64_t D0 = 0;
    uint64_t PM_j_old = 0;
    size_t currDist = s1.size();
    assert(s1.size() != 0);

    /* mask used when computing D[m,j] in the paper 10^(m-1) */
    uint64_t mask = UINT64_C(1) << (s1.size() - 1);

    /* Searching */
    for (const auto& ch : s2) {
        /* Step 1: Computing D0 */
        uint64_t PM_j = PM.get(0, ch);
        uint64_t TR = (((~D0) & PM_j) << 1) & PM_j_old;
        D0 = (((PM_j & VP) + VP) ^ VP) | PM_j | VN;
        D0 = D0 | TR;

        /* Step 2: Computing HP and HN */
        uint64_t HP = VN | ~(D0 | VP);
        uint64_t HN = D0 & VP;

        /* Step 3: Computing the value D[m,j] */
        currDist += bool(HP & mask);
        currDist -= bool(HN & mask);

        /* Step 4: Computing Vp and VN */
        HP = (HP << 1) | 1;
        HN = (HN << 1);

        VP = HN | ~(D0 | HP);
        VN = HP & D0;
        PM_j_old = PM_j;
    }

    return (currDist <= max) ? currDist : max + 1;
}

#ifdef RAPIDFUZZ_SIMD
template <typename VecType, typename InputIt, int _lto_hack = RAPIDFUZZ_LTO_HACK>
void osa_hyrroe2003_simd(Range<size_t*> scores, const detail::BlockPatternMatchVector& block,
                         const std::vector<size_t>& s1_lengths, const Range<InputIt>& s2,
                         size_t score_cutoff) noexcept
{
#    ifdef RAPIDFUZZ_AVX2
    using namespace simd_avx2;
#    else
    using namespace simd_sse2;
#    endif
    static constexpr size_t alignment = native_simd<VecType>::alignment;
    static constexpr size_t vec_width = native_simd<VecType>::size;
    static constexpr size_t vecs = native_simd<uint64_t>::size;
    assert(block.size() % vecs == 0);

    native_simd<VecType> zero(VecType(0));
    native_simd<VecType> one(1);
    size_t result_index = 0;

    for (size_t cur_vec = 0; cur_vec < block.size(); cur_vec += vecs) {
        /* VP is set to 1^m */
        native_simd<VecType> VP(static_cast<VecType>(-1));
        native_simd<VecType> VN(VecType(0));
        native_simd<VecType> D0(VecType(0));
        native_simd<VecType> PM_j_old(VecType(0));

        alignas(alignment) std::array<VecType, vec_width> currDist_;
        unroll<int, vec_width>(
            [&](auto i) { currDist_[i] = static_cast<VecType>(s1_lengths[result_index + i]); });
        native_simd<VecType> currDist(reinterpret_cast<uint64_t*>(currDist_.data()));
        /* mask used when computing D[m,j] in the paper 10^(m-1) */
        alignas(alignment) std::array<VecType, vec_width> mask_;
        unroll<int, vec_width>([&](auto i) {
            if (s1_lengths[result_index + i] == 0)
                mask_[i] = 0;
            else
                mask_[i] = static_cast<VecType>(UINT64_C(1) << (s1_lengths[result_index + i] - 1));
        });
        native_simd<VecType> mask(reinterpret_cast<uint64_t*>(mask_.data()));

        for (const auto& ch : s2) {
            /* Step 1: Computing D0 */
            alignas(alignment) std::array<uint64_t, vecs> stored;
            unroll<int, vecs>([&](auto i) { stored[i] = block.get(cur_vec + i, ch); });

            native_simd<VecType> PM_j(stored.data());
            auto TR = (andnot(PM_j, D0) << 1) & PM_j_old;
            D0 = (((PM_j & VP) + VP) ^ VP) | PM_j | VN;
            D0 = D0 | TR;

            /* Step 2: Computing HP and HN */
            auto HP = VN | ~(D0 | VP);
            auto HN = D0 & VP;

            /* Step 3: Computing the value D[m,j] */
            currDist += andnot(one, (HP & mask) == zero);
            currDist -= andnot(one, (HN & mask) == zero);

            /* Step 4: Computing Vp and VN */
            HP = (HP << 1) | one;
            HN = (HN << 1);

            VP = HN | ~(D0 | HP);
            VN = HP & D0;
            PM_j_old = PM_j;
        }

        alignas(alignment) std::array<VecType, vec_width> distances;
        currDist.store(distances.data());

        unroll<int, vec_width>([&](auto i) {
            size_t score = 0;
            /* strings of length 0 are not handled correctly */
            if (s1_lengths[result_index] == 0) {
                score = s2.size();
            }
            /* calculate score under consideration of wraparounds in parallel counter */
            else {
                if constexpr (std::numeric_limits<VecType>::max() < std::numeric_limits<size_t>::max()) {
                    size_t min_dist = abs_diff(s1_lengths[result_index], s2.size());
                    size_t wraparound_score = static_cast<size_t>(std::numeric_limits<VecType>::max()) + 1;

                    score = (min_dist / wraparound_score) * wraparound_score;
                    VecType remainder = static_cast<VecType>(min_dist % wraparound_score);

                    if (distances[i] < remainder) score += wraparound_score;
                }

                score += distances[i];
            }
            scores[result_index] = (score <= score_cutoff) ? score : score_cutoff + 1;
            result_index++;
        });
    }
}
#endif

template <typename InputIt1, typename InputIt2>
size_t osa_hyrroe2003_block(const BlockPatternMatchVector& PM, const Range<InputIt1>& s1,
                            const Range<InputIt2>& s2, size_t max = std::numeric_limits<size_t>::max())
{
    struct Row {
        uint64_t VP;
        uint64_t VN;
        uint64_t D0;
        uint64_t PM;

        Row() : VP(~UINT64_C(0)), VN(0), D0(0), PM(0)
        {}
    };

    size_t word_size = sizeof(uint64_t) * 8;
    size_t words = PM.size();
    uint64_t Last = UINT64_C(1) << ((s1.size() - 1) % word_size);

    size_t currDist = s1.size();
    std::vector<Row> old_vecs(words + 1);
    std::vector<Row> new_vecs(words + 1);

    /* Searching */
    auto iter_s2 = s2.begin();
    for (size_t row = 0; row < s2.size(); ++iter_s2, ++row) {
        uint64_t HP_carry = 1;
        uint64_t HN_carry = 0;

        for (size_t word = 0; word < words; word++) {
            /* retrieve bit vectors from last iterations */
            uint64_t VN = old_vecs[word + 1].VN;
            uint64_t VP = old_vecs[word + 1].VP;
            uint64_t D0 = old_vecs[word + 1].D0;
            /* D0 last word */
            uint64_t D0_last = old_vecs[word].D0;

            /* PM of last char same word */
            uint64_t PM_j_old = old_vecs[word + 1].PM;
            /* PM of last word */
            uint64_t PM_last = new_vecs[word].PM;

            uint64_t PM_j = PM.get(word, *iter_s2);
            uint64_t X = PM_j;
            uint64_t TR = ((((~D0) & X) << 1) | (((~D0_last) & PM_last) >> 63)) & PM_j_old;

            X |= HN_carry;
            D0 = (((X & VP) + VP) ^ VP) | X | VN | TR;

            uint64_t HP = VN | ~(D0 | VP);
            uint64_t HN = D0 & VP;

            if (word == words - 1) {
                currDist += bool(HP & Last);
                currDist -= bool(HN & Last);
            }

            uint64_t HP_carry_temp = HP_carry;
            HP_carry = HP >> 63;
            HP = (HP << 1) | HP_carry_temp;
            uint64_t HN_carry_temp = HN_carry;
            HN_carry = HN >> 63;
            HN = (HN << 1) | HN_carry_temp;

            new_vecs[word + 1].VP = HN | ~(D0 | HP);
            new_vecs[word + 1].VN = HP & D0;
            new_vecs[word + 1].D0 = D0;
            new_vecs[word + 1].PM = PM_j;
        }

        std::swap(new_vecs, old_vecs);
    }

    return (currDist <= max) ? currDist : max + 1;
}

class OSA : public DistanceBase<OSA, size_t, 0, std::numeric_limits<int64_t>::max()> {
    friend DistanceBase<OSA, size_t, 0, std::numeric_limits<int64_t>::max()>;
    friend NormalizedMetricBase<OSA>;

    template <typename InputIt1, typename InputIt2>
    static size_t maximum(const Range<InputIt1>& s1, const Range<InputIt2>& s2)
    {
        return std::max(s1.size(), s2.size());
    }

    template <typename InputIt1, typename InputIt2>
    static size_t _distance(Range<InputIt1> s1, Range<InputIt2> s2, size_t score_cutoff, size_t score_hint)
    {
        if (s2.size() < s1.size()) return _distance(s2, s1, score_cutoff, score_hint);

        remove_common_affix(s1, s2);
        if (s1.empty())
            return (s2.size() <= score_cutoff) ? s2.size() : score_cutoff + 1;
        else if (s1.size() < 64)
            return osa_hyrroe2003(PatternMatchVector(s1), s1, s2, score_cutoff);
        else
            return osa_hyrroe2003_block(BlockPatternMatchVector(s1), s1, s2, score_cutoff);
    }
};

} // namespace duckdb_rapidfuzz::detail