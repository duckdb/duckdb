/* SPDX-License-Identifier: MIT */
/* Copyright © 2022-present Max Bachmann */

#include <cstddef>
#include <cstdint>
#include <limits>
#include <rapidfuzz/details/GrowingHashmap.hpp>
#include <rapidfuzz/details/Matrix.hpp>
#include <rapidfuzz/details/PatternMatchVector.hpp>
#include <rapidfuzz/details/common.hpp>
#include <rapidfuzz/details/distance.hpp>
#include <rapidfuzz/details/intrinsics.hpp>
#include <rapidfuzz/details/type_traits.hpp>
#include <rapidfuzz/distance/Indel.hpp>
#include <sys/types.h>

namespace duckdb_rapidfuzz::detail {

struct LevenshteinRow {
    uint64_t VP;
    uint64_t VN;

    LevenshteinRow() : VP(~UINT64_C(0)), VN(0)
    {}

    LevenshteinRow(uint64_t VP_, uint64_t VN_) : VP(VP_), VN(VN_)
    {}
};

template <bool RecordMatrix, bool RecordBitRow>
struct LevenshteinResult;

template <>
struct LevenshteinResult<true, false> {
    ShiftedBitMatrix<uint64_t> VP;
    ShiftedBitMatrix<uint64_t> VN;

    size_t dist;
};

template <>
struct LevenshteinResult<false, true> {
    size_t first_block;
    size_t last_block;
    size_t prev_score;
    std::vector<LevenshteinRow> vecs;

    size_t dist;
};

template <>
struct LevenshteinResult<false, false> {
    size_t dist;
};

template <typename InputIt1, typename InputIt2>
size_t generalized_levenshtein_wagner_fischer(const Range<InputIt1>& s1, const Range<InputIt2>& s2,
                                              LevenshteinWeightTable weights, size_t max)
{
    size_t cache_size = s1.size() + 1;
    std::vector<size_t> cache(cache_size);
    assume(cache_size != 0);

    for (size_t i = 0; i < cache_size; ++i)
        cache[i] = i * weights.delete_cost;

    for (const auto& ch2 : s2) {
        auto cache_iter = cache.begin();
        size_t temp = *cache_iter;
        *cache_iter += weights.insert_cost;

        for (const auto& ch1 : s1) {
            if (ch1 != ch2)
                temp = std::min({*cache_iter + weights.delete_cost, *(cache_iter + 1) + weights.insert_cost,
                                 temp + weights.replace_cost});
            ++cache_iter;
            std::swap(*cache_iter, temp);
        }
    }

    size_t dist = cache.back();
    return (dist <= max) ? dist : max + 1;
}

/**
 * @brief calculates the maximum possible Levenshtein distance based on
 * string lengths and weights
 */
static inline size_t levenshtein_maximum(size_t len1, size_t len2, LevenshteinWeightTable weights)
{
    size_t max_dist = len1 * weights.delete_cost + len2 * weights.insert_cost;

    if (len1 >= len2)
        max_dist = std::min(max_dist, len2 * weights.replace_cost + (len1 - len2) * weights.delete_cost);
    else
        max_dist = std::min(max_dist, len1 * weights.replace_cost + (len2 - len1) * weights.insert_cost);

    return max_dist;
}

/**
 * @brief calculates the minimal possible Levenshtein distance based on
 * string lengths and weights
 */
template <typename InputIt1, typename InputIt2>
size_t levenshtein_min_distance(const Range<InputIt1>& s1, const Range<InputIt2>& s2,
                                LevenshteinWeightTable weights)
{
    if (s1.size() > s2.size())
        return (s1.size() - s2.size()) * weights.delete_cost;
    else
        return (s2.size() - s1.size()) * weights.insert_cost;
}

template <typename InputIt1, typename InputIt2>
size_t generalized_levenshtein_distance(Range<InputIt1> s1, Range<InputIt2> s2,
                                        LevenshteinWeightTable weights, size_t max)
{
    size_t min_edits = levenshtein_min_distance(s1, s2, weights);
    if (min_edits > max) return max + 1;

    /* common affix does not effect Levenshtein distance */
    remove_common_affix(s1, s2);

    return generalized_levenshtein_wagner_fischer(s1, s2, weights, max);
}

/*
 * An encoded mbleven model table.
 *
 * Each 8-bit integer represents an edit sequence, with using two
 * bits for a single operation.
 *
 * Each Row of 8 integers represent all possible combinations
 * of edit sequences for a gived maximum edit distance and length
 * difference between the two strings, that is below the maximum
 * edit distance
 *
 *   01 = DELETE, 10 = INSERT, 11 = SUBSTITUTE
 *
 * For example, 3F -> 0b111111 means three substitutions
 */
static constexpr std::array<std::array<uint8_t, 7>, 9> levenshtein_mbleven2018_matrix = {{
    /* max edit distance 1 */
    {0x03}, /* len_diff 0 */
    {0x01}, /* len_diff 1 */
    /* max edit distance 2 */
    {0x0F, 0x09, 0x06}, /* len_diff 0 */
    {0x0D, 0x07},       /* len_diff 1 */
    {0x05},             /* len_diff 2 */
    /* max edit distance 3 */
    {0x3F, 0x27, 0x2D, 0x39, 0x36, 0x1E, 0x1B}, /* len_diff 0 */
    {0x3D, 0x37, 0x1F, 0x25, 0x19, 0x16},       /* len_diff 1 */
    {0x35, 0x1D, 0x17},                         /* len_diff 2 */
    {0x15},                                     /* len_diff 3 */
}};

template <typename InputIt1, typename InputIt2>
size_t levenshtein_mbleven2018(const Range<InputIt1>& s1, const Range<InputIt2>& s2, size_t max)
{
    size_t len1 = s1.size();
    size_t len2 = s2.size();
    assert(len1 > 0);
    assert(len2 > 0);
    assert(*s1.begin() != *s2.begin());
    assert(*std::prev(s1.end()) != *std::prev(s2.end()));

    if (len1 < len2) return levenshtein_mbleven2018(s2, s1, max);

    size_t len_diff = len1 - len2;

    if (max == 1) return max + static_cast<size_t>(len_diff == 1 || len1 != 1);

    size_t ops_index = (max + max * max) / 2 + len_diff - 1;
    auto& possible_ops = levenshtein_mbleven2018_matrix[ops_index];
    size_t dist = max + 1;

    for (uint8_t ops : possible_ops) {
        auto iter_s1 = s1.begin();
        auto iter_s2 = s2.begin();
        size_t cur_dist = 0;

        if (!ops) break;

        while (iter_s1 != s1.end() && iter_s2 != s2.end()) {
            if (*iter_s1 != *iter_s2) {
                cur_dist++;
                if (!ops) break;
                if (ops & 1) iter_s1++;
                if (ops & 2) iter_s2++;
#if defined(__GNUC__) && !defined(__clang__) && !defined(__ICC) && __GNUC__ < 10
#    pragma GCC diagnostic push
#    pragma GCC diagnostic ignored "-Wconversion"
#endif
                ops >>= 2;
#if defined(__GNUC__) && !defined(__clang__) && !defined(__ICC) && __GNUC__ < 10
#    pragma GCC diagnostic pop
#endif
            }
            else {
                iter_s1++;
                iter_s2++;
            }
        }
        cur_dist += static_cast<size_t>(std::distance(iter_s1, s1.end()) + std::distance(iter_s2, s2.end()));
        dist = std::min(dist, cur_dist);
    }

    return (dist <= max) ? dist : max + 1;
}

/**
 * @brief Bitparallel implementation of the Levenshtein distance.
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
 * @return returns the levenshtein distance between s1 and s2
 */
template <bool RecordMatrix, bool RecordBitRow, typename PM_Vec, typename InputIt1, typename InputIt2>
auto levenshtein_hyrroe2003(const PM_Vec& PM, const Range<InputIt1>& s1, const Range<InputIt2>& s2,
                            size_t max = std::numeric_limits<size_t>::max())
    -> LevenshteinResult<RecordMatrix, RecordBitRow>
{
    assert(s1.size() != 0);

    /* VP is set to 1^m. Shifting by bitwidth would be undefined behavior */
    uint64_t VP = ~UINT64_C(0);
    uint64_t VN = 0;

    LevenshteinResult<RecordMatrix, RecordBitRow> res;
    res.dist = s1.size();
    if constexpr (RecordMatrix) {
        res.VP = ShiftedBitMatrix<uint64_t>(s2.size(), 1, ~UINT64_C(0));
        res.VN = ShiftedBitMatrix<uint64_t>(s2.size(), 1, 0);
    }

    /* mask used when computing D[m,j] in the paper 10^(m-1) */
    uint64_t mask = UINT64_C(1) << (s1.size() - 1);

    /* Searching */
    auto iter_s2 = s2.begin();
    for (size_t i = 0; iter_s2 != s2.end(); ++iter_s2, ++i) {
        /* Step 1: Computing D0 */
        uint64_t PM_j = PM.get(0, *iter_s2);
        uint64_t X = PM_j;
        uint64_t D0 = (((X & VP) + VP) ^ VP) | X | VN;

        /* Step 2: Computing HP and HN */
        uint64_t HP = VN | ~(D0 | VP);
        uint64_t HN = D0 & VP;

        /* Step 3: Computing the value D[m,j] */
        res.dist += bool(HP & mask);
        res.dist -= bool(HN & mask);

        /* Step 4: Computing Vp and VN */
        HP = (HP << 1) | 1;
        HN = (HN << 1);

        VP = HN | ~(D0 | HP);
        VN = HP & D0;

        if constexpr (RecordMatrix) {
            res.VP[i][0] = VP;
            res.VN[i][0] = VN;
        }
    }

    if (res.dist > max) res.dist = max + 1;

    if constexpr (RecordBitRow) {
        res.first_block = 0;
        res.last_block = 0;
        res.prev_score = s2.size();
        res.vecs.emplace_back(VP, VN);
    }

    return res;
}

#ifdef RAPIDFUZZ_SIMD
template <typename VecType, typename InputIt, int _lto_hack = RAPIDFUZZ_LTO_HACK>
void levenshtein_hyrroe2003_simd(Range<size_t*> scores, const detail::BlockPatternMatchVector& block,
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

            native_simd<VecType> X(stored.data());
            auto D0 = (((X & VP) + VP) ^ VP) | X | VN;

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
size_t levenshtein_hyrroe2003_small_band(const BlockPatternMatchVector& PM, const Range<InputIt1>& s1,
                                         const Range<InputIt2>& s2, size_t max)
{
    /* VP is set to 1^m. */
    uint64_t VP = ~UINT64_C(0) << (64 - max - 1);
    uint64_t VN = 0;

    const auto words = PM.size();
    size_t currDist = max;
    uint64_t diagonal_mask = UINT64_C(1) << 63;
    uint64_t horizontal_mask = UINT64_C(1) << 62;
    ptrdiff_t start_pos = static_cast<ptrdiff_t>(max) + 1 - 64;

    /* score can decrease along the horizontal, but not along the diagonal */
    size_t break_score = 2 * max + s2.size() - s1.size();

    /* Searching */
    size_t i = 0;
    if (s1.size() > max) {
        for (; i < s1.size() - max; ++i, ++start_pos) {
            /* Step 1: Computing D0 */
            uint64_t PM_j = 0;
            if (start_pos < 0) {
                PM_j = PM.get(0, s2[i]) << (-start_pos);
            }
            else {
                size_t word = static_cast<size_t>(start_pos) / 64;
                size_t word_pos = static_cast<size_t>(start_pos) % 64;

                PM_j = PM.get(word, s2[i]) >> word_pos;

                if (word + 1 < words && word_pos != 0) PM_j |= PM.get(word + 1, s2[i]) << (64 - word_pos);
            }
            uint64_t X = PM_j;
            uint64_t D0 = (((X & VP) + VP) ^ VP) | X | VN;

            /* Step 2: Computing HP and HN */
            uint64_t HP = VN | ~(D0 | VP);
            uint64_t HN = D0 & VP;

            /* Step 3: Computing the value D[m,j] */
            currDist += !bool(D0 & diagonal_mask);

            if (currDist > break_score) return max + 1;

            /* Step 4: Computing Vp and VN */
            VP = HN | ~((D0 >> 1) | HP);
            VN = (D0 >> 1) & HP;
        }
    }

    for (; i < s2.size(); ++i, ++start_pos) {
        /* Step 1: Computing D0 */
        uint64_t PM_j = 0;
        if (start_pos < 0) {
            PM_j = PM.get(0, s2[i]) << (-start_pos);
        }
        else {
            size_t word = static_cast<size_t>(start_pos) / 64;
            size_t word_pos = static_cast<size_t>(start_pos) % 64;

            PM_j = PM.get(word, s2[i]) >> word_pos;

            if (word + 1 < words && word_pos != 0) PM_j |= PM.get(word + 1, s2[i]) << (64 - word_pos);
        }
        uint64_t X = PM_j;
        uint64_t D0 = (((X & VP) + VP) ^ VP) | X | VN;

        /* Step 2: Computing HP and HN */
        uint64_t HP = VN | ~(D0 | VP);
        uint64_t HN = D0 & VP;

        /* Step 3: Computing the value D[m,j] */
        currDist += bool(HP & horizontal_mask);
        currDist -= bool(HN & horizontal_mask);
        horizontal_mask >>= 1;

        if (currDist > break_score) return max + 1;

        /* Step 4: Computing Vp and VN */
        VP = HN | ~((D0 >> 1) | HP);
        VN = (D0 >> 1) & HP;
    }

    return (currDist <= max) ? currDist : max + 1;
}

template <bool RecordMatrix, typename InputIt1, typename InputIt2>
auto levenshtein_hyrroe2003_small_band(const Range<InputIt1>& s1, const Range<InputIt2>& s2,
                                       size_t max) -> LevenshteinResult<RecordMatrix, false>
{
    assert(max <= s1.size());
    assert(max <= s2.size());
    assert(s2.size() >= s1.size() - max);

    /* VP is set to 1^m. Shifting by bitwidth would be undefined behavior */
    uint64_t VP = ~UINT64_C(0) << (64 - max - 1);
    uint64_t VN = 0;

    LevenshteinResult<RecordMatrix, false> res;
    res.dist = max;
    if constexpr (RecordMatrix) {
        res.VP = ShiftedBitMatrix<uint64_t>(s2.size(), 1, ~UINT64_C(0));
        res.VN = ShiftedBitMatrix<uint64_t>(s2.size(), 1, 0);

        ptrdiff_t start_offset = static_cast<ptrdiff_t>(max) + 2 - 64;
        for (size_t i = 0; i < s2.size(); ++i) {
            res.VP.set_offset(i, start_offset + static_cast<ptrdiff_t>(i));
            res.VN.set_offset(i, start_offset + static_cast<ptrdiff_t>(i));
        }
    }

    uint64_t diagonal_mask = UINT64_C(1) << 63;
    uint64_t horizontal_mask = UINT64_C(1) << 62;

    /* score can decrease along the horizontal, but not along the diagonal */
    size_t break_score = 2 * max + s2.size() - (s1.size());
    HybridGrowingHashmap<typename Range<InputIt1>::value_type, std::pair<ptrdiff_t, uint64_t>> PM;

    auto iter_s1 = s1.begin();
    for (ptrdiff_t j = -static_cast<ptrdiff_t>(max); j < 0; ++iter_s1, ++j) {
        auto& x = PM[*iter_s1];
        x.second = shr64(x.second, j - x.first) | (UINT64_C(1) << 63);
        x.first = j;
    }

    /* Searching */
    size_t i = 0;
    auto iter_s2 = s2.begin();
    for (; i < s1.size() - max; ++iter_s2, ++iter_s1, ++i) {
        /* Step 1: Computing D0 */
        /* update bitmasks online */
        uint64_t PM_j = 0;
        {
            auto& x = PM[*iter_s1];
            x.second = shr64(x.second, static_cast<ptrdiff_t>(i) - x.first) | (UINT64_C(1) << 63);
            x.first = static_cast<ptrdiff_t>(i);
        }
        {
            auto x = PM.get(*iter_s2);
            PM_j = shr64(x.second, static_cast<ptrdiff_t>(i) - x.first);
        }

        uint64_t X = PM_j;
        uint64_t D0 = (((X & VP) + VP) ^ VP) | X | VN;

        /* Step 2: Computing HP and HN */
        uint64_t HP = VN | ~(D0 | VP);
        uint64_t HN = D0 & VP;

        /* Step 3: Computing the value D[m,j] */
        res.dist += !bool(D0 & diagonal_mask);

        if (res.dist > break_score) {
            res.dist = max + 1;
            return res;
        }

        /* Step 4: Computing Vp and VN */
        VP = HN | ~((D0 >> 1) | HP);
        VN = (D0 >> 1) & HP;

        if constexpr (RecordMatrix) {
            res.VP[i][0] = VP;
            res.VN[i][0] = VN;
        }
    }

    for (; i < s2.size(); ++iter_s2, ++i) {
        /* Step 1: Computing D0 */
        /* update bitmasks online */
        uint64_t PM_j = 0;
        if (iter_s1 != s1.end()) {
            auto& x = PM[*iter_s1];
            x.second = shr64(x.second, static_cast<ptrdiff_t>(i) - x.first) | (UINT64_C(1) << 63);
            x.first = static_cast<ptrdiff_t>(i);
            ++iter_s1;
        }
        {
            auto x = PM.get(*iter_s2);
            PM_j = shr64(x.second, static_cast<ptrdiff_t>(i) - x.first);
        }

        uint64_t X = PM_j;
        uint64_t D0 = (((X & VP) + VP) ^ VP) | X | VN;

        /* Step 2: Computing HP and HN */
        uint64_t HP = VN | ~(D0 | VP);
        uint64_t HN = D0 & VP;

        /* Step 3: Computing the value D[m,j] */
        res.dist += bool(HP & horizontal_mask);
        res.dist -= bool(HN & horizontal_mask);
        horizontal_mask >>= 1;

        if (res.dist > break_score) {
            res.dist = max + 1;
            return res;
        }

        /* Step 4: Computing Vp and VN */
        VP = HN | ~((D0 >> 1) | HP);
        VN = (D0 >> 1) & HP;

        if constexpr (RecordMatrix) {
            res.VP[i][0] = VP;
            res.VN[i][0] = VN;
        }
    }

    if (res.dist > max) res.dist = max + 1;

    return res;
}

/**
 * @param stop_row specifies the row to record when using RecordBitRow
 */
template <bool RecordMatrix, bool RecordBitRow, typename InputIt1, typename InputIt2>
auto levenshtein_hyrroe2003_block(const BlockPatternMatchVector& PM, const Range<InputIt1>& s1,
                                  const Range<InputIt2>& s2, size_t max = std::numeric_limits<size_t>::max(),
                                  size_t stop_row = std::numeric_limits<size_t>::max())
    -> LevenshteinResult<RecordMatrix, RecordBitRow>
{
    LevenshteinResult<RecordMatrix, RecordBitRow> res;
    if (max < abs_diff(s1.size(), s2.size())) {
        res.dist = max + 1;
        return res;
    }

    size_t word_size = sizeof(uint64_t) * 8;
    size_t words = PM.size();
    std::vector<LevenshteinRow> vecs(words);
    std::vector<size_t> scores(words);
    uint64_t Last = UINT64_C(1) << ((s1.size() - 1) % word_size);

    for (size_t i = 0; i < words - 1; ++i)
        scores[i] = (i + 1) * word_size;

    scores[words - 1] = s1.size();

    if constexpr (RecordMatrix) {
        size_t full_band = std::min(s1.size(), 2 * max + 1);
        size_t full_band_words = std::min(words, full_band / word_size + 2);
        res.VP = ShiftedBitMatrix<uint64_t>(s2.size(), full_band_words, ~UINT64_C(0));
        res.VN = ShiftedBitMatrix<uint64_t>(s2.size(), full_band_words, 0);
    }

    if constexpr (RecordBitRow) {
        res.first_block = 0;
        res.last_block = 0;
        res.prev_score = 0;
    }

    max = std::min(max, std::max(s1.size(), s2.size()));

    /* first_block is the index of the first block in Ukkonen band. */
    size_t first_block = 0;
    /* last_block is the index of the last block in Ukkonen band. */
    size_t last_block =
        std::min(words, ceil_div(std::min(max, (max + s1.size() - s2.size()) / 2) + 1, word_size)) - 1;

    /* Searching */
    auto iter_s2 = s2.begin();
    for (size_t row = 0; row < s2.size(); ++iter_s2, ++row) {
        uint64_t HP_carry = 1;
        uint64_t HN_carry = 0;

        if constexpr (RecordMatrix) {
            res.VP.set_offset(row, static_cast<ptrdiff_t>(first_block * word_size));
            res.VN.set_offset(row, static_cast<ptrdiff_t>(first_block * word_size));
        }

        auto advance_block = [&](size_t word) {
            /* Step 1: Computing D0 */
            uint64_t PM_j = PM.get(word, *iter_s2);
            uint64_t VN = vecs[word].VN;
            uint64_t VP = vecs[word].VP;

            uint64_t X = PM_j | HN_carry;
            uint64_t D0 = (((X & VP) + VP) ^ VP) | X | VN;

            /* Step 2: Computing HP and HN */
            uint64_t HP = VN | ~(D0 | VP);
            uint64_t HN = D0 & VP;

            uint64_t HP_carry_temp = HP_carry;
            uint64_t HN_carry_temp = HN_carry;
            if (word < words - 1) {
                HP_carry = HP >> 63;
                HN_carry = HN >> 63;
            }
            else {
                HP_carry = bool(HP & Last);
                HN_carry = bool(HN & Last);
            }

            /* Step 4: Computing Vp and VN */
            HP = (HP << 1) | HP_carry_temp;
            HN = (HN << 1) | HN_carry_temp;

            vecs[word].VP = HN | ~(D0 | HP);
            vecs[word].VN = HP & D0;

            if constexpr (RecordMatrix) {
                res.VP[row][word - first_block] = vecs[word].VP;
                res.VN[row][word - first_block] = vecs[word].VN;
            }

            return static_cast<int64_t>(HP_carry) - static_cast<int64_t>(HN_carry);
        };

        auto get_row_num = [&](size_t word) {
            if (word + 1 == words) return s1.size() - 1;
            return (word + 1) * word_size - 1;
        };

        for (size_t word = first_block; word <= last_block /* - 1*/; word++) {
            /* Step 3: Computing the value D[m,j] */
            scores[word] = static_cast<size_t>(static_cast<ptrdiff_t>(scores[word]) + advance_block(word));
        }

        max = static_cast<size_t>(
            std::min(static_cast<ptrdiff_t>(max),
                     static_cast<ptrdiff_t>(scores[last_block]) +
                         std::max(static_cast<ptrdiff_t>(s2.size()) - static_cast<ptrdiff_t>(row) - 1,
                                  static_cast<ptrdiff_t>(s1.size()) -
                                      (static_cast<ptrdiff_t>((1 + last_block) * word_size - 1) - 1))));

        /*---------- Adjust number of blocks according to Ukkonen ----------*/
        // todo on the last word instead of word_size often s1.size() % 64 should be used

        /* Band adjustment: last_block */
        /*  If block is not beneath band, calculate next block. Only next because others are certainly beneath
         * band. */
        if (last_block + 1 < words) {
            ptrdiff_t cond = static_cast<ptrdiff_t>(max + 2 * word_size + row + s1.size()) -
                             static_cast<ptrdiff_t>(scores[last_block] + 2 + s2.size());
            if (static_cast<ptrdiff_t>(get_row_num(last_block)) < cond) {
                last_block++;
                vecs[last_block].VP = ~UINT64_C(0);
                vecs[last_block].VN = 0;

                size_t chars_in_block = (last_block + 1 == words) ? ((s1.size() - 1) % word_size + 1) : 64;
                scores[last_block] = scores[last_block - 1] + chars_in_block -
                                     opt_static_cast<size_t>(HP_carry) + opt_static_cast<size_t>(HN_carry);
                // todo probably wrong types
                scores[last_block] = static_cast<size_t>(static_cast<ptrdiff_t>(scores[last_block]) +
                                                         advance_block(last_block));
            }
        }

        for (; last_block >= first_block; --last_block) {
            /* in band if score <= k where score >= score_last - word_size + 1 */
            bool in_band_cond1 = scores[last_block] < max + word_size;

            /* in band if row <= max - score - len2 + len1 + i
             * if the condition is met for the first cell in the block, it
             * is met for all other cells in the blocks as well
             *
             * this uses a more loose condition similar to edlib:
             * https://github.com/Martinsos/edlib
             */
            ptrdiff_t cond = static_cast<ptrdiff_t>(max + 2 * word_size + row + s1.size() + 1) -
                             static_cast<ptrdiff_t>(scores[last_block] + 2 + s2.size());
            bool in_band_cond2 = static_cast<ptrdiff_t>(get_row_num(last_block)) <= cond;

            if (in_band_cond1 && in_band_cond2) break;
        }

        /* Band adjustment: first_block */
        for (; first_block <= last_block; ++first_block) {
            /* in band if score <= k where score >= score_last - word_size + 1 */
            bool in_band_cond1 = scores[first_block] < max + word_size;

            /* in band if row >= score - max - len2 + len1 + i
             * if this condition is met for the last cell in the block, it
             * is met for all other cells in the blocks as well
             */
            ptrdiff_t cond = static_cast<ptrdiff_t>(scores[first_block] + s1.size() + row) -
                             static_cast<ptrdiff_t>(max + s2.size());
            bool in_band_cond2 = static_cast<ptrdiff_t>(get_row_num(first_block)) >= cond;

            if (in_band_cond1 && in_band_cond2) break;
        }

        /* distance is larger than max, so band stops to exist */
        if (last_block < first_block) {
            res.dist = max + 1;
            return res;
        }

        if constexpr (RecordBitRow) {
            if (row == stop_row) {
                if (first_block == 0)
                    res.prev_score = stop_row + 1;
                else {
                    /* count backwards to find score at last position in previous block */
                    size_t relevant_bits = std::min((first_block + 1) * 64, s1.size()) % 64;
                    uint64_t mask = ~UINT64_C(0);
                    if (relevant_bits) mask >>= 64 - relevant_bits;

                    res.prev_score = scores[first_block] + popcount(vecs[first_block].VN & mask) -
                                     popcount(vecs[first_block].VP & mask);
                }

                res.first_block = first_block;
                res.last_block = last_block;
                res.vecs = std::move(vecs);

                /* unknown so make sure it is <= max */
                res.dist = 0;
                return res;
            }
        }
    }

    res.dist = scores[words - 1];

    if (res.dist > max) res.dist = max + 1;

    return res;
}

template <typename InputIt1, typename InputIt2>
size_t uniform_levenshtein_distance(const BlockPatternMatchVector& block, Range<InputIt1> s1,
                                    Range<InputIt2> s2, size_t score_cutoff, size_t score_hint)
{
    /* upper bound */
    score_cutoff = std::min(score_cutoff, std::max(s1.size(), s2.size()));
    if (score_hint < 31) score_hint = 31;

    // when no differences are allowed a direct comparision is sufficient
    if (score_cutoff == 0) return !std::equal(s1.begin(), s1.end(), s2.begin(), s2.end());

    if (score_cutoff < abs_diff(s1.size(), s2.size())) return score_cutoff + 1;

    // important to catch, since this causes block to be empty -> raises exception on access
    if (s1.empty()) return (s2.size() <= score_cutoff) ? s2.size() : score_cutoff + 1;

    /* do this first, since we can not remove any affix in encoded form
     * todo actually we could at least remove the common prefix and just shift the band
     */
    if (score_cutoff >= 4) {
        // todo could safe up to 25% even without max when ignoring irrelevant paths
        // in the upper and lower corner
        size_t full_band = std::min(s1.size(), 2 * score_cutoff + 1);

        if (s1.size() < 65)
            return levenshtein_hyrroe2003<false, false>(block, s1, s2, score_cutoff).dist;
        else if (full_band <= 64)
            return levenshtein_hyrroe2003_small_band(block, s1, s2, score_cutoff);

        while (score_hint < score_cutoff) {
            full_band = std::min(s1.size(), 2 * score_hint + 1);

            size_t score;
            if (full_band <= 64)
                score = levenshtein_hyrroe2003_small_band(block, s1, s2, score_hint);
            else
                score = levenshtein_hyrroe2003_block<false, false>(block, s1, s2, score_hint).dist;

            if (score <= score_hint) return score;

            if (std::numeric_limits<size_t>::max() / 2 < score_hint) break;

            score_hint *= 2;
        }

        return levenshtein_hyrroe2003_block<false, false>(block, s1, s2, score_cutoff).dist;
    }

    /* common affix does not effect Levenshtein distance */
    remove_common_affix(s1, s2);
    if (s1.empty() || s2.empty()) return s1.size() + s2.size();

    return levenshtein_mbleven2018(s1, s2, score_cutoff);
}

template <typename InputIt1, typename InputIt2>
size_t uniform_levenshtein_distance(Range<InputIt1> s1, Range<InputIt2> s2, size_t score_cutoff,
                                    size_t score_hint)
{
    /* Swapping the strings so the second string is shorter */
    if (s1.size() < s2.size()) return uniform_levenshtein_distance(s2, s1, score_cutoff, score_hint);

    /* upper bound */
    score_cutoff = std::min(score_cutoff, std::max(s1.size(), s2.size()));
    if (score_hint < 31) score_hint = 31;

    // when no differences are allowed a direct comparision is sufficient
    if (score_cutoff == 0) return !std::equal(s1.begin(), s1.end(), s2.begin(), s2.end());

    // at least length difference insertions/deletions required
    if (score_cutoff < (s1.size() - s2.size())) return score_cutoff + 1;

    /* common affix does not effect Levenshtein distance */
    remove_common_affix(s1, s2);
    if (s1.empty() || s2.empty()) return s1.size() + s2.size();

    if (score_cutoff < 4) return levenshtein_mbleven2018(s1, s2, score_cutoff);

    // todo could safe up to 25% even without score_cutoff when ignoring irrelevant paths
    // in the upper and lower corner
    size_t full_band = std::min(s1.size(), 2 * score_cutoff + 1);

    /* when the short strings has less then 65 elements Hyyrös' algorithm can be used */
    if (s2.size() < 65)
        return levenshtein_hyrroe2003<false, false>(PatternMatchVector(s2), s2, s1, score_cutoff).dist;
    else if (full_band <= 64)
        return levenshtein_hyrroe2003_small_band<false>(s1, s2, score_cutoff).dist;
    else {
        BlockPatternMatchVector PM(s1);
        while (score_hint < score_cutoff) {
            // todo use small band implementation if possible
            size_t score = levenshtein_hyrroe2003_block<false, false>(PM, s1, s2, score_hint).dist;

            if (score <= score_hint) return score;

            if (std::numeric_limits<size_t>::max() / 2 < score_hint) break;

            score_hint *= 2;
        }

        return levenshtein_hyrroe2003_block<false, false>(PM, s1, s2, score_cutoff).dist;
    }
}

/**
 * @brief recover alignment from bitparallel Levenshtein matrix
 */
template <typename InputIt1, typename InputIt2>
void recover_alignment(Editops& editops, const Range<InputIt1>& s1, const Range<InputIt2>& s2,
                       const LevenshteinResult<true, false>& matrix, size_t src_pos, size_t dest_pos,
                       size_t editop_pos)
{
    size_t dist = matrix.dist;
    size_t col = s1.size();
    size_t row = s2.size();

    while (row && col) {
        /* Deletion */
        if (matrix.VP.test_bit(row - 1, col - 1)) {
            assert(dist > 0);
            dist--;
            col--;
            editops[editop_pos + dist].type = EditType::Delete;
            editops[editop_pos + dist].src_pos = col + src_pos;
            editops[editop_pos + dist].dest_pos = row + dest_pos;
        }
        else {
            row--;

            /* Insertion */
            if (row && matrix.VN.test_bit(row - 1, col - 1)) {
                assert(dist > 0);
                dist--;
                editops[editop_pos + dist].type = EditType::Insert;
                editops[editop_pos + dist].src_pos = col + src_pos;
                editops[editop_pos + dist].dest_pos = row + dest_pos;
            }
            /* Match/Mismatch */
            else {
                col--;

                /* Replace (Matches are not recorded) */
                if (s1[col] != s2[row]) {
                    assert(dist > 0);
                    dist--;
                    editops[editop_pos + dist].type = EditType::Replace;
                    editops[editop_pos + dist].src_pos = col + src_pos;
                    editops[editop_pos + dist].dest_pos = row + dest_pos;
                }
            }
        }
    }

    while (col) {
        dist--;
        col--;
        editops[editop_pos + dist].type = EditType::Delete;
        editops[editop_pos + dist].src_pos = col + src_pos;
        editops[editop_pos + dist].dest_pos = row + dest_pos;
    }

    while (row) {
        dist--;
        row--;
        editops[editop_pos + dist].type = EditType::Insert;
        editops[editop_pos + dist].src_pos = col + src_pos;
        editops[editop_pos + dist].dest_pos = row + dest_pos;
    }
}

template <typename InputIt1, typename InputIt2>
void levenshtein_align(Editops& editops, const Range<InputIt1>& s1, const Range<InputIt2>& s2,
                       size_t max = std::numeric_limits<size_t>::max(), size_t src_pos = 0,
                       size_t dest_pos = 0, size_t editop_pos = 0)
{
    /* upper bound */
    max = std::min(max, std::max(s1.size(), s2.size()));
    size_t full_band = std::min(s1.size(), 2 * max + 1);

    LevenshteinResult<true, false> matrix;
    if (s1.empty() || s2.empty())
        matrix.dist = s1.size() + s2.size();
    else if (s1.size() <= 64)
        matrix = levenshtein_hyrroe2003<true, false>(PatternMatchVector(s1), s1, s2);
    else if (full_band <= 64)
        matrix = levenshtein_hyrroe2003_small_band<true>(s1, s2, max);
    else
        matrix = levenshtein_hyrroe2003_block<true, false>(BlockPatternMatchVector(s1), s1, s2, max);

    assert(matrix.dist <= max);
    if (matrix.dist != 0) {
        if (editops.size() == 0) editops.resize(matrix.dist);

        recover_alignment(editops, s1, s2, matrix, src_pos, dest_pos, editop_pos);
    }
}

template <typename InputIt1, typename InputIt2>
LevenshteinResult<false, true> levenshtein_row(const Range<InputIt1>& s1, const Range<InputIt2>& s2,
                                               size_t max, size_t stop_row)
{
    return levenshtein_hyrroe2003_block<false, true>(BlockPatternMatchVector(s1), s1, s2, max, stop_row);
}

template <typename InputIt1, typename InputIt2>
size_t levenshtein_distance(const Range<InputIt1>& s1, const Range<InputIt2>& s2,
                            LevenshteinWeightTable weights = {1, 1, 1},
                            size_t score_cutoff = std::numeric_limits<size_t>::max(),
                            size_t score_hint = std::numeric_limits<size_t>::max())
{
    if (weights.insert_cost == weights.delete_cost) {
        /* when insertions + deletions operations are free there can not be any edit distance */
        if (weights.insert_cost == 0) return 0;

        /* uniform Levenshtein multiplied with the common factor */
        if (weights.insert_cost == weights.replace_cost) {
            // score_cutoff can make use of the common divisor of the three weights
            size_t new_score_cutoff = ceil_div(score_cutoff, weights.insert_cost);
            size_t new_score_hint = ceil_div(score_hint, weights.insert_cost);
            size_t distance = uniform_levenshtein_distance(s1, s2, new_score_cutoff, new_score_hint);
            distance *= weights.insert_cost;
            return (distance <= score_cutoff) ? distance : score_cutoff + 1;
        }
        /*
         * when replace_cost >= insert_cost + delete_cost no substitutions are performed
         * therefore this can be implemented as InDel distance multiplied with the common factor
         */
        else if (weights.replace_cost >= weights.insert_cost + weights.delete_cost) {
            // score_cutoff can make use of the common divisor of the three weights
            size_t new_score_cutoff = ceil_div(score_cutoff, weights.insert_cost);
            size_t distance = rapidfuzz::indel_distance(s1, s2, new_score_cutoff);
            distance *= weights.insert_cost;
            return (distance <= score_cutoff) ? distance : score_cutoff + 1;
        }
    }

    return generalized_levenshtein_distance(s1, s2, weights, score_cutoff);
}
struct HirschbergPos {
    size_t left_score;
    size_t right_score;
    size_t s1_mid;
    size_t s2_mid;
};

template <typename InputIt1, typename InputIt2>
HirschbergPos find_hirschberg_pos(const Range<InputIt1>& s1, const Range<InputIt2>& s2,
                                  size_t max = std::numeric_limits<size_t>::max())
{
    assert(s1.size() > 1);
    assert(s2.size() > 1);

    HirschbergPos hpos = {};
    size_t left_size = s2.size() / 2;
    size_t right_size = s2.size() - left_size;
    hpos.s2_mid = left_size;
    size_t s1_len = s1.size();
    size_t best_score = std::numeric_limits<size_t>::max();
    size_t right_first_pos = 0;
    size_t right_last_pos = 0;
    // todo: we could avoid this allocation by counting up the right score twice
    // not sure whats faster though
    std::vector<size_t> right_scores;
    {
        auto right_row = levenshtein_row(s1.reversed(), s2.reversed(), max, right_size - 1);
        if (right_row.dist > max) return find_hirschberg_pos(s1, s2, max * 2);

        right_first_pos = right_row.first_block * 64;
        right_last_pos = std::min(s1_len, right_row.last_block * 64 + 64);

        right_scores.resize(right_last_pos - right_first_pos + 1, 0);
        assume(right_scores.size() != 0);
        right_scores[0] = right_row.prev_score;

        for (size_t i = right_first_pos; i < right_last_pos; ++i) {
            size_t col_pos = i % 64;
            size_t col_word = i / 64;
            uint64_t col_mask = UINT64_C(1) << col_pos;

            right_scores[i - right_first_pos + 1] = right_scores[i - right_first_pos];
            right_scores[i - right_first_pos + 1] -= bool(right_row.vecs[col_word].VN & col_mask);
            right_scores[i - right_first_pos + 1] += bool(right_row.vecs[col_word].VP & col_mask);
        }
    }

    auto left_row = levenshtein_row(s1, s2, max, left_size - 1);
    if (left_row.dist > max) return find_hirschberg_pos(s1, s2, max * 2);

    auto left_first_pos = left_row.first_block * 64;
    auto left_last_pos = std::min(s1_len, left_row.last_block * 64 + 64);

    size_t left_score = left_row.prev_score;
    // take boundary into account
    if (s1_len >= left_first_pos + right_first_pos) {
        size_t right_index = s1_len - left_first_pos - right_first_pos;
        if (right_index < right_scores.size()) {
            best_score = right_scores[right_index] + left_score;
            hpos.left_score = left_score;
            hpos.right_score = right_scores[right_index];
            hpos.s1_mid = left_first_pos;
        }
    }

    for (size_t i = left_first_pos; i < left_last_pos; ++i) {
        size_t col_pos = i % 64;
        size_t col_word = i / 64;
        uint64_t col_mask = UINT64_C(1) << col_pos;

        left_score -= bool(left_row.vecs[col_word].VN & col_mask);
        left_score += bool(left_row.vecs[col_word].VP & col_mask);

        if (s1_len < i + 1 + right_first_pos) continue;

        size_t right_index = s1_len - i - 1 - right_first_pos;
        if (right_index >= right_scores.size()) continue;

        if (right_scores[right_index] + left_score < best_score) {
            best_score = right_scores[right_index] + left_score;
            hpos.left_score = left_score;
            hpos.right_score = right_scores[right_index];
            hpos.s1_mid = i + 1;
        }
    }

    assert(hpos.left_score >= 0);
    assert(hpos.right_score >= 0);

    if (hpos.left_score + hpos.right_score > max)
        return find_hirschberg_pos(s1, s2, max * 2);
    else {
        assert(levenshtein_distance(s1, s2) == hpos.left_score + hpos.right_score);
        return hpos;
    }
}

template <typename InputIt1, typename InputIt2>
void levenshtein_align_hirschberg(Editops& editops, Range<InputIt1> s1, Range<InputIt2> s2,
                                  size_t src_pos = 0, size_t dest_pos = 0, size_t editop_pos = 0,
                                  size_t max = std::numeric_limits<size_t>::max())
{
    /* prefix and suffix are no-ops, which do not need to be added to the editops */
    StringAffix affix = remove_common_affix(s1, s2);
    src_pos += affix.prefix_len;
    dest_pos += affix.prefix_len;

    max = std::min(max, std::max(s1.size(), s2.size()));
    size_t full_band = std::min(s1.size(), 2 * max + 1);

    size_t matrix_size = 2 * full_band * s2.size() / 8;
    if (matrix_size < 1024 * 1024 || s1.size() < 65 || s2.size() < 10) {
        levenshtein_align(editops, s1, s2, max, src_pos, dest_pos, editop_pos);
    }
    /* Hirschbergs algorithm */
    else {
        auto hpos = find_hirschberg_pos(s1, s2, max);

        if (editops.size() == 0) editops.resize(hpos.left_score + hpos.right_score);

        levenshtein_align_hirschberg(editops, s1.subseq(0, hpos.s1_mid), s2.subseq(0, hpos.s2_mid), src_pos,
                                     dest_pos, editop_pos, hpos.left_score);
        levenshtein_align_hirschberg(editops, s1.subseq(hpos.s1_mid), s2.subseq(hpos.s2_mid),
                                     src_pos + hpos.s1_mid, dest_pos + hpos.s2_mid,
                                     editop_pos + hpos.left_score, hpos.right_score);
    }
}

class Levenshtein : public DistanceBase<Levenshtein, size_t, 0, std::numeric_limits<int64_t>::max(),
                                        LevenshteinWeightTable> {
    friend DistanceBase<Levenshtein, size_t, 0, std::numeric_limits<int64_t>::max(), LevenshteinWeightTable>;
    friend NormalizedMetricBase<Levenshtein, LevenshteinWeightTable>;

    template <typename InputIt1, typename InputIt2>
    static size_t maximum(const Range<InputIt1>& s1, const Range<InputIt2>& s2,
                          LevenshteinWeightTable weights)
    {
        return levenshtein_maximum(s1.size(), s2.size(), weights);
    }

    template <typename InputIt1, typename InputIt2>
    static size_t _distance(const Range<InputIt1>& s1, const Range<InputIt2>& s2,
                            LevenshteinWeightTable weights, size_t score_cutoff, size_t score_hint)
    {
        return levenshtein_distance(s1, s2, weights, score_cutoff, score_hint);
    }
};

template <typename InputIt1, typename InputIt2>
Editops levenshtein_editops(const Range<InputIt1>& s1, const Range<InputIt2>& s2, size_t score_hint)
{
    Editops editops;
    if (score_hint < 31) score_hint = 31;

    size_t score_cutoff = std::max(s1.size(), s2.size());
    /* score_hint currently leads to calculating the levenshtein distance twice
     * 1) to find the real distance
     * 2) to find the alignment
     * this is only worth it when at least 50% of the runtime could be saved
     * todo: maybe there is a way to join these two calculations in the future
     * so it is worth it in more cases
     */
    if (std::numeric_limits<size_t>::max() / 2 > score_hint && 2 * score_hint < score_cutoff)
        score_cutoff = Levenshtein::distance(s1, s2, {1, 1, 1}, score_cutoff, score_hint);

    levenshtein_align_hirschberg(editops, s1, s2, 0, 0, 0, score_cutoff);

    editops.set_src_len(s1.size());
    editops.set_dest_len(s2.size());
    return editops;
}

} // namespace duckdb_rapidfuzz::detail
