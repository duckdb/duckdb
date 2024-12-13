/* SPDX-License-Identifier: MIT */
/* Copyright © 2022-present Max Bachmann */

#include <limits>
#include <rapidfuzz/details/Matrix.hpp>
#include <rapidfuzz/details/PatternMatchVector.hpp>
#include <rapidfuzz/details/common.hpp>
#include <rapidfuzz/details/distance.hpp>
#include <rapidfuzz/details/intrinsics.hpp>
#include <rapidfuzz/details/simd.hpp>

#include <algorithm>
#include <array>
#include <rapidfuzz/details/types.hpp>

namespace duckdb_rapidfuzz::detail {

template <bool RecordMatrix>
struct LCSseqResult;

template <>
struct LCSseqResult<true> {
    ShiftedBitMatrix<uint64_t> S;

    size_t sim;
};

template <>
struct LCSseqResult<false> {
    size_t sim;
};

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
 *   0x1 = 01 = DELETE,
 *   0x2 = 10 = INSERT
 *
 * 0x5 -> DEL + DEL
 * 0x6 -> DEL + INS
 * 0x9 -> INS + DEL
 * 0xA -> INS + INS
 */
static constexpr std::array<std::array<uint8_t, 6>, 14> lcs_seq_mbleven2018_matrix = {{
    /* max edit distance 1 */
    {0},
    /* case does not occur */ /* len_diff 0 */
    {0x01},                   /* len_diff 1 */
    /* max edit distance 2 */
    {0x09, 0x06}, /* len_diff 0 */
    {0x01},       /* len_diff 1 */
    {0x05},       /* len_diff 2 */
    /* max edit distance 3 */
    {0x09, 0x06},       /* len_diff 0 */
    {0x25, 0x19, 0x16}, /* len_diff 1 */
    {0x05},             /* len_diff 2 */
    {0x15},             /* len_diff 3 */
    /* max edit distance 4 */
    {0x96, 0x66, 0x5A, 0x99, 0x69, 0xA5}, /* len_diff 0 */
    {0x25, 0x19, 0x16},                   /* len_diff 1 */
    {0x65, 0x56, 0x95, 0x59},             /* len_diff 2 */
    {0x15},                               /* len_diff 3 */
    {0x55},                               /* len_diff 4 */
}};

template <typename InputIt1, typename InputIt2>
size_t lcs_seq_mbleven2018(const Range<InputIt1>& s1, const Range<InputIt2>& s2, size_t score_cutoff)
{
    auto len1 = s1.size();
    auto len2 = s2.size();
    assert(len1 != 0);
    assert(len2 != 0);

    if (len1 < len2) return lcs_seq_mbleven2018(s2, s1, score_cutoff);

    auto len_diff = len1 - len2;
    size_t max_misses = len1 + len2 - 2 * score_cutoff;
    size_t ops_index = (max_misses + max_misses * max_misses) / 2 + len_diff - 1;
    auto& possible_ops = lcs_seq_mbleven2018_matrix[ops_index];
    size_t max_len = 0;

    for (uint8_t ops : possible_ops) {
        auto iter_s1 = s1.begin();
        auto iter_s2 = s2.begin();
        size_t cur_len = 0;

        if (!ops) break;

        while (iter_s1 != s1.end() && iter_s2 != s2.end()) {
            if (*iter_s1 != *iter_s2) {
                if (!ops) break;
                if (ops & 1)
                    iter_s1++;
                else if (ops & 2)
                    iter_s2++;
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
                cur_len++;
                iter_s1++;
                iter_s2++;
            }
        }

        max_len = std::max(max_len, cur_len);
    }

    return (max_len >= score_cutoff) ? max_len : 0;
}

#ifdef RAPIDFUZZ_SIMD
template <typename VecType, typename InputIt, int _lto_hack = RAPIDFUZZ_LTO_HACK>
void lcs_simd(Range<size_t*> scores, const BlockPatternMatchVector& block, const Range<InputIt>& s2,
              size_t score_cutoff) noexcept
{
#    ifdef RAPIDFUZZ_AVX2
    using namespace simd_avx2;
#    else
    using namespace simd_sse2;
#    endif
    auto score_iter = scores.begin();
    static constexpr size_t alignment = native_simd<VecType>::alignment;
    static constexpr size_t vecs = native_simd<uint64_t>::size;
    assert(block.size() % vecs == 0);

    static constexpr size_t interleaveCount = 3;

    size_t cur_vec = 0;
    for (; cur_vec + interleaveCount * vecs <= block.size(); cur_vec += interleaveCount * vecs) {
        std::array<native_simd<VecType>, interleaveCount> S;
        unroll<int, interleaveCount>([&](auto j) { S[j] = static_cast<VecType>(-1); });

        for (const auto& ch : s2) {
            unroll<int, interleaveCount>([&](auto j) {
                alignas(32) std::array<uint64_t, vecs> stored;
                unroll<int, vecs>([&](auto i) { stored[i] = block.get(cur_vec + j * vecs + i, ch); });

                native_simd<VecType> Matches(stored.data());
                native_simd<VecType> u = S[j] & Matches;
                S[j] = (S[j] + u) | (S[j] - u);
            });
        }

        unroll<int, interleaveCount>([&](auto j) {
            auto counts = popcount(~S[j]);
            unroll<int, counts.size()>([&](auto i) {
                *score_iter = (counts[i] >= score_cutoff) ? static_cast<size_t>(counts[i]) : 0;
                score_iter++;
            });
        });
    }

    for (; cur_vec < block.size(); cur_vec += vecs) {
        native_simd<VecType> S = static_cast<VecType>(-1);

        for (const auto& ch : s2) {
            alignas(alignment) std::array<uint64_t, vecs> stored;
            unroll<int, vecs>([&](auto i) { stored[i] = block.get(cur_vec + i, ch); });

            native_simd<VecType> Matches(stored.data());
            native_simd<VecType> u = S & Matches;
            S = (S + u) | (S - u);
        }

        auto counts = popcount(~S);
        unroll<int, counts.size()>([&](auto i) {
            *score_iter = (counts[i] >= score_cutoff) ? static_cast<size_t>(counts[i]) : 0;
            score_iter++;
        });
    }
}

#endif

template <size_t N, bool RecordMatrix, typename PMV, typename InputIt1, typename InputIt2>
auto lcs_unroll(const PMV& block, const Range<InputIt1>&, const Range<InputIt2>& s2,
                size_t score_cutoff = 0) -> LCSseqResult<RecordMatrix>
{
    uint64_t S[N];
    unroll<size_t, N>([&](size_t i) { S[i] = ~UINT64_C(0); });

    LCSseqResult<RecordMatrix> res;
    if constexpr (RecordMatrix) res.S = ShiftedBitMatrix<uint64_t>(s2.size(), N, ~UINT64_C(0));

    auto iter_s2 = s2.begin();
    for (size_t i = 0; i < s2.size(); ++i) {
        uint64_t carry = 0;

        static constexpr size_t unroll_factor = 3;
        for (unsigned int j = 0; j < N / unroll_factor; ++j) {
            unroll<size_t, unroll_factor>([&](size_t word_) {
                size_t word = word_ + j * unroll_factor;
                uint64_t Matches = block.get(word, *iter_s2);
                uint64_t u = S[word] & Matches;
                uint64_t x = addc64(S[word], u, carry, &carry);
                S[word] = x | (S[word] - u);

                if constexpr (RecordMatrix) res.S[i][word] = S[word];
            });
        }

        unroll<size_t, N % unroll_factor>([&](size_t word_) {
            size_t word = word_ + N / unroll_factor * unroll_factor;
            uint64_t Matches = block.get(word, *iter_s2);
            uint64_t u = S[word] & Matches;
            uint64_t x = addc64(S[word], u, carry, &carry);
            S[word] = x | (S[word] - u);

            if constexpr (RecordMatrix) res.S[i][word] = S[word];
        });

        iter_s2++;
    }

    res.sim = 0;
    unroll<size_t, N>([&](size_t i) { res.sim += popcount(~S[i]); });

    if (res.sim < score_cutoff) res.sim = 0;

    return res;
}

/**
 * implementation is following the paper Bit-Parallel LCS-length Computation Revisited
 * from Heikki Hyyrö
 *
 * The paper refers to s1 as m and s2 as n
 */
template <bool RecordMatrix, typename PMV, typename InputIt1, typename InputIt2>
auto lcs_blockwise(const PMV& PM, const Range<InputIt1>& s1, const Range<InputIt2>& s2,
                   size_t score_cutoff = 0) -> LCSseqResult<RecordMatrix>
{
    assert(score_cutoff <= s1.size());
    assert(score_cutoff <= s2.size());

    size_t word_size = sizeof(uint64_t) * 8;
    size_t words = PM.size();
    std::vector<uint64_t> S(words, ~UINT64_C(0));

    size_t band_width_left = s1.size() - score_cutoff;
    size_t band_width_right = s2.size() - score_cutoff;

    LCSseqResult<RecordMatrix> res;
    if constexpr (RecordMatrix) {
        size_t full_band = band_width_left + 1 + band_width_right;
        size_t full_band_words = std::min(words, full_band / word_size + 2);
        res.S = ShiftedBitMatrix<uint64_t>(s2.size(), full_band_words, ~UINT64_C(0));
    }

    /* first_block is the index of the first block in Ukkonen band. */
    size_t first_block = 0;
    size_t last_block = std::min(words, ceil_div(band_width_left + 1, word_size));

    auto iter_s2 = s2.begin();
    for (size_t row = 0; row < s2.size(); ++row) {
        uint64_t carry = 0;

        if constexpr (RecordMatrix) res.S.set_offset(row, static_cast<ptrdiff_t>(first_block * word_size));

        for (size_t word = first_block; word < last_block; ++word) {
            const uint64_t Matches = PM.get(word, *iter_s2);
            uint64_t Stemp = S[word];

            uint64_t u = Stemp & Matches;

            uint64_t x = addc64(Stemp, u, carry, &carry);
            S[word] = x | (Stemp - u);

            if constexpr (RecordMatrix) res.S[row][word - first_block] = S[word];
        }

        if (row > band_width_right) first_block = (row - band_width_right) / word_size;

        if (row + 1 + band_width_left <= s1.size())
            last_block = ceil_div(row + 1 + band_width_left, word_size);

        iter_s2++;
    }

    res.sim = 0;
    for (uint64_t Stemp : S)
        res.sim += popcount(~Stemp);

    if (res.sim < score_cutoff) res.sim = 0;

    return res;
}

template <typename PMV, typename InputIt1, typename InputIt2>
size_t longest_common_subsequence(const PMV& PM, const Range<InputIt1>& s1, const Range<InputIt2>& s2,
                                  size_t score_cutoff)
{
    assert(score_cutoff <= s1.size());
    assert(score_cutoff <= s2.size());

    size_t word_size = sizeof(uint64_t) * 8;
    size_t words = PM.size();
    size_t band_width_left = s1.size() - score_cutoff;
    size_t band_width_right = s2.size() - score_cutoff;
    size_t full_band = band_width_left + 1 + band_width_right;
    size_t full_band_words = std::min(words, full_band / word_size + 2);

    if (full_band_words < words) return lcs_blockwise<false>(PM, s1, s2, score_cutoff).sim;

    auto nr = ceil_div(s1.size(), 64);
    switch (nr) {
    case 0: return 0;
    case 1: return lcs_unroll<1, false>(PM, s1, s2, score_cutoff).sim;
    case 2: return lcs_unroll<2, false>(PM, s1, s2, score_cutoff).sim;
    case 3: return lcs_unroll<3, false>(PM, s1, s2, score_cutoff).sim;
    case 4: return lcs_unroll<4, false>(PM, s1, s2, score_cutoff).sim;
    case 5: return lcs_unroll<5, false>(PM, s1, s2, score_cutoff).sim;
    case 6: return lcs_unroll<6, false>(PM, s1, s2, score_cutoff).sim;
    case 7: return lcs_unroll<7, false>(PM, s1, s2, score_cutoff).sim;
    case 8: return lcs_unroll<8, false>(PM, s1, s2, score_cutoff).sim;
    default: return lcs_blockwise<false>(PM, s1, s2, score_cutoff).sim;
    }
}

template <typename InputIt1, typename InputIt2>
size_t longest_common_subsequence(const Range<InputIt1>& s1, const Range<InputIt2>& s2, size_t score_cutoff)
{
    if (s1.empty()) return 0;
    if (s1.size() <= 64) return longest_common_subsequence(PatternMatchVector(s1), s1, s2, score_cutoff);

    return longest_common_subsequence(BlockPatternMatchVector(s1), s1, s2, score_cutoff);
}

template <typename InputIt1, typename InputIt2>
size_t lcs_seq_similarity(const BlockPatternMatchVector& block, Range<InputIt1> s1, Range<InputIt2> s2,
                          size_t score_cutoff)
{
    auto len1 = s1.size();
    auto len2 = s2.size();

    if (score_cutoff > len1 || score_cutoff > len2) return 0;

    size_t max_misses = len1 + len2 - 2 * score_cutoff;

    /* no edits are allowed */
    if (max_misses == 0 || (max_misses == 1 && len1 == len2))
        return std::equal(s1.begin(), s1.end(), s2.begin(), s2.end()) ? len1 : 0;

    if (max_misses < abs_diff(len1, len2)) return 0;

    // do this first, since we can not remove any affix in encoded form
    if (max_misses >= 5) return longest_common_subsequence(block, s1, s2, score_cutoff);

    /* common affix does not effect Levenshtein distance */
    StringAffix affix = remove_common_affix(s1, s2);
    size_t lcs_sim = affix.prefix_len + affix.suffix_len;
    if (!s1.empty() && !s2.empty()) {
        size_t adjusted_cutoff = score_cutoff >= lcs_sim ? score_cutoff - lcs_sim : 0;
        lcs_sim += lcs_seq_mbleven2018(s1, s2, adjusted_cutoff);
    }

    return (lcs_sim >= score_cutoff) ? lcs_sim : 0;
}

template <typename InputIt1, typename InputIt2>
size_t lcs_seq_similarity(Range<InputIt1> s1, Range<InputIt2> s2, size_t score_cutoff)
{
    auto len1 = s1.size();
    auto len2 = s2.size();

    // Swapping the strings so the second string is shorter
    if (len1 < len2) return lcs_seq_similarity(s2, s1, score_cutoff);

    if (score_cutoff > len1 || score_cutoff > len2) return 0;

    size_t max_misses = len1 + len2 - 2 * score_cutoff;

    /* no edits are allowed */
    if (max_misses == 0 || (max_misses == 1 && len1 == len2))
        return std::equal(s1.begin(), s1.end(), s2.begin(), s2.end()) ? len1 : 0;

    if (max_misses < abs_diff(len1, len2)) return 0;

    /* common affix does not effect Levenshtein distance */
    StringAffix affix = remove_common_affix(s1, s2);
    size_t lcs_sim = affix.prefix_len + affix.suffix_len;
    if (s1.size() && s2.size()) {
        size_t adjusted_cutoff = score_cutoff >= lcs_sim ? score_cutoff - lcs_sim : 0;
        if (max_misses < 5)
            lcs_sim += lcs_seq_mbleven2018(s1, s2, adjusted_cutoff);
        else
            lcs_sim += longest_common_subsequence(s1, s2, adjusted_cutoff);
    }

    return (lcs_sim >= score_cutoff) ? lcs_sim : 0;
}

/**
 * @brief recover alignment from bitparallel Levenshtein matrix
 */
template <typename InputIt1, typename InputIt2>
Editops recover_alignment(const Range<InputIt1>& s1, const Range<InputIt2>& s2,
                          const LCSseqResult<true>& matrix, StringAffix affix)
{
    size_t len1 = s1.size();
    size_t len2 = s2.size();
    size_t dist = len1 + len2 - 2 * matrix.sim;
    Editops editops(dist);
    editops.set_src_len(len1 + affix.prefix_len + affix.suffix_len);
    editops.set_dest_len(len2 + affix.prefix_len + affix.suffix_len);

    if (dist == 0) return editops;

    [[maybe_unused]] size_t band_width_right = s2.size() - matrix.sim;

    auto col = len1;
    auto row = len2;

    while (row && col) {
        /* Deletion */
        if (matrix.S.test_bit(row - 1, col - 1)) {
            assert(dist > 0);
            assert(static_cast<ptrdiff_t>(col) >=
                   static_cast<ptrdiff_t>(row) - static_cast<ptrdiff_t>(band_width_right));
            dist--;
            col--;
            editops[dist].type = EditType::Delete;
            editops[dist].src_pos = col + affix.prefix_len;
            editops[dist].dest_pos = row + affix.prefix_len;
        }
        else {
            row--;

            /* Insertion */
            if (row && !(matrix.S.test_bit(row - 1, col - 1))) {
                assert(dist > 0);
                dist--;
                editops[dist].type = EditType::Insert;
                editops[dist].src_pos = col + affix.prefix_len;
                editops[dist].dest_pos = row + affix.prefix_len;
            }
            /* Match */
            else {
                col--;
                assert(s1[col] == s2[row]);
            }
        }
    }

    while (col) {
        dist--;
        col--;
        editops[dist].type = EditType::Delete;
        editops[dist].src_pos = col + affix.prefix_len;
        editops[dist].dest_pos = row + affix.prefix_len;
    }

    while (row) {
        dist--;
        row--;
        editops[dist].type = EditType::Insert;
        editops[dist].src_pos = col + affix.prefix_len;
        editops[dist].dest_pos = row + affix.prefix_len;
    }

    return editops;
}

template <typename InputIt1, typename InputIt2>
LCSseqResult<true> lcs_matrix(const Range<InputIt1>& s1, const Range<InputIt2>& s2)
{
    size_t nr = ceil_div(s1.size(), 64);
    switch (nr) {
    case 0:
    {
        LCSseqResult<true> res;
        res.sim = 0;
        return res;
    }
    case 1: return lcs_unroll<1, true>(PatternMatchVector(s1), s1, s2);
    case 2: return lcs_unroll<2, true>(BlockPatternMatchVector(s1), s1, s2);
    case 3: return lcs_unroll<3, true>(BlockPatternMatchVector(s1), s1, s2);
    case 4: return lcs_unroll<4, true>(BlockPatternMatchVector(s1), s1, s2);
    case 5: return lcs_unroll<5, true>(BlockPatternMatchVector(s1), s1, s2);
    case 6: return lcs_unroll<6, true>(BlockPatternMatchVector(s1), s1, s2);
    case 7: return lcs_unroll<7, true>(BlockPatternMatchVector(s1), s1, s2);
    case 8: return lcs_unroll<8, true>(BlockPatternMatchVector(s1), s1, s2);
    default: return lcs_blockwise<true>(BlockPatternMatchVector(s1), s1, s2);
    }
}

template <typename InputIt1, typename InputIt2>
Editops lcs_seq_editops(Range<InputIt1> s1, Range<InputIt2> s2)
{
    /* prefix and suffix are no-ops, which do not need to be added to the editops */
    StringAffix affix = remove_common_affix(s1, s2);

    return recover_alignment(s1, s2, lcs_matrix(s1, s2), affix);
}

class LCSseq : public SimilarityBase<LCSseq, size_t, 0, std::numeric_limits<int64_t>::max()> {
    friend SimilarityBase<LCSseq, size_t, 0, std::numeric_limits<int64_t>::max()>;
    friend NormalizedMetricBase<LCSseq>;

    template <typename InputIt1, typename InputIt2>
    static size_t maximum(const Range<InputIt1>& s1, const Range<InputIt2>& s2)
    {
        return std::max(s1.size(), s2.size());
    }

    template <typename InputIt1, typename InputIt2>
    static size_t _similarity(const Range<InputIt1>& s1, const Range<InputIt2>& s2, size_t score_cutoff,
                              [[maybe_unused]] size_t score_hint)
    {
        return lcs_seq_similarity(s1, s2, score_cutoff);
    }
};

} // namespace duckdb_rapidfuzz::detail
