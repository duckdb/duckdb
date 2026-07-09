//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/bitmap_selection_vector.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/bit_utils.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/types/validity_mask.hpp"

namespace duckdb {

// Per-byte selection extraction lookup.
// Low 4 bits contain popcount(byte), high 12 bits contain the offset into BITMAP_SELVEC_POSITIONS.
static constexpr uint16_t BITMAP_SELVEC_OFFSETS[256] = {
    0x0010, 0x0001, 0x0081, 0x0072, 0x0011, 0x0002, 0x00f2, 0x00e3, 0x0021, 0x02a2, 0x0082, 0x0073, 0x0012, 0x0003,
    0x0163, 0x0154, 0x0031, 0x0842, 0x0312, 0x0303, 0x0102, 0x0483, 0x00f3, 0x00e4, 0x0022, 0x02a3, 0x0083, 0x0074,
    0x0013, 0x0004, 0x01d4, 0x01c5, 0x0041, 0x1202, 0x08a2, 0x0893, 0x0382, 0x0a73, 0x0373, 0x0364, 0x0182, 0x0983,
    0x0613, 0x0604, 0x0173, 0x04e4, 0x0164, 0x0155, 0x0032, 0x0843, 0x0313, 0x0304, 0x0103, 0x0484, 0x00f4, 0x00e5,
    0x0023, 0x02a4, 0x0084, 0x0075, 0x0014, 0x0005, 0x0245, 0x0236, 0x0051, 0x1332, 0x0f92, 0x0f83, 0x0902, 0x1083,
    0x08f3, 0x08e4, 0x03f2, 0x1143, 0x0b73, 0x0b64, 0x03e3, 0x0ac4, 0x03d4, 0x03c5, 0x0202, 0x11c3, 0x0d03, 0x0cf4,
    0x0743, 0x0ca4, 0x0734, 0x0725, 0x01f3, 0x09d4, 0x0674, 0x0665, 0x01e4, 0x0545, 0x01d5, 0x01c6, 0x0042, 0x1203,
    0x08a3, 0x0894, 0x0383, 0x0a74, 0x0374, 0x0365, 0x0183, 0x0984, 0x0614, 0x0605, 0x0174, 0x04e5, 0x0165, 0x0156,
    0x0033, 0x0844, 0x0314, 0x0305, 0x0104, 0x0485, 0x00f5, 0x00e6, 0x0024, 0x02a5, 0x0085, 0x0076, 0x0015, 0x0006,
    0x1396, 0x1387, 0x0061, 0x1362, 0x1252, 0x1243, 0x0ea2, 0x1273, 0x0e93, 0x0e84, 0x0962, 0x12a3, 0x0ed3, 0x0ec4,
    0x0953, 0x0fc4, 0x0944, 0x0935, 0x0462, 0x12d3, 0x0f13, 0x0f04, 0x0c23, 0x1004, 0x0c14, 0x0c05, 0x0453, 0x10c4,
    0x0bc4, 0x0bb5, 0x0444, 0x0b15, 0x0435, 0x0426, 0x0282, 0x1303, 0x0f53, 0x0f44, 0x0c73, 0x1044, 0x0c64, 0x0c55,
    0x0813, 0x1104, 0x0d54, 0x0d45, 0x0804, 0x0d95, 0x07f5, 0x07e6, 0x0273, 0x1184, 0x0df4, 0x0de5, 0x07a4, 0x0e35,
    0x0795, 0x0786, 0x0264, 0x0a25, 0x06d5, 0x06c6, 0x0255, 0x05a6, 0x0246, 0x0237, 0x0052, 0x1333, 0x0f93, 0x0f84,
    0x0903, 0x1084, 0x08f4, 0x08e5, 0x03f3, 0x1144, 0x0b74, 0x0b65, 0x03e4, 0x0ac5, 0x03d5, 0x03c6, 0x0203, 0x11c4,
    0x0d04, 0x0cf5, 0x0744, 0x0ca5, 0x0735, 0x0726, 0x01f4, 0x09d5, 0x0675, 0x0666, 0x01e5, 0x0546, 0x01d6, 0x01c7,
    0x0043, 0x1204, 0x08a4, 0x0895, 0x0384, 0x0a75, 0x0375, 0x0366, 0x0184, 0x0985, 0x0615, 0x0606, 0x0175, 0x04e6,
    0x0166, 0x0157, 0x0034, 0x0845, 0x0315, 0x0306, 0x0105, 0x0486, 0x00f6, 0x00e7, 0x0025, 0x02a6, 0x0086, 0x0077,
    0x0016, 0x0007, 0x1397, 0x1388,
};

// array with all possible list subsets of 0,1,2,3,4,5,6,7 - BITMAP_SELVEC_OFFSETS contains 256 offsets into it
// clang-format off
static constexpr sel_t BITMAP_SELVEC_POSITIONS_STORAGE[321] = {
    0, 2, 3, 4, 5, 6, 7, /**/ 0, 1, 3, 4, 5, 6, 7, /**/ 0, 1, 2, 4, 5, 6, 7,
    0, 1, 2, 3, 5, 6, 7, /**/ 0, 1, 2, 3, 4, 6, 7, /**/ 0, 1, 2, 3, 4, 5, 7,
    0, 3, 4, 5, 6, 7, /**/ 0, 1, 4, 5, 6, 7, /**/ 0, 1, 2, 5, 6, 7, /**/ 0, 1, 2, 3, 6, 7, /**/ 0, 1, 2, 3, 4, 7,
    0, 2, 4, 5, 6, 7, /**/ 0, 2, 3, 5, 6, 7, /**/ 0, 2, 3, 4, 6, 7, /**/ 0, 2, 3, 4, 5, 7, /**/ 0, 1, 3, 5, 6, 7,
    0, 1, 3, 4, 6, 7, /**/ 0, 1, 3, 4, 5, 7, /**/ 0, 1, 2, 4, 6, 7, /**/ 0, 1, 2, 4, 5, 7, /**/ 0, 1, 2, 3, 5, 7,
    0, 4, 5, 6, 7, /**/ 0, 1, 5, 6, 7, /**/ 0, 1, 2, 6, 7, /**/ 0, 1, 2, 3, 7, /**/ 0, 3, 5, 6, 7,
    0, 3, 4, 6, 7, /**/ 0, 3, 4, 5, 7, /**/ 0, 2, 5, 6, 7, /**/ 0, 2, 3, 6, 7, /**/ 0, 2, 3, 4, 7,
    0, 1, 3, 6, 7, /**/ 0, 1, 3, 4, 7, /**/ 0, 1, 2, 4, 7, /**/ 0, 1, 2, 5, 7, /**/ 0, 2, 4, 6, 7,
    0, 1, 4, 6, 7, /**/ 0, 1, 3, 5, 7, /**/ 0, 2, 3, 5, 7, /**/ 0, 1, 4, 5, 7, /**/ 0, 2, 4, 5, 7,
    0, 1, 2, 7, /**/ 0, 1, 3, 7, /**/ 0, 1, 4, 7, /**/ 0, 1, 5, 7, /**/ 0, 1, 6, 7,
    0, 2, 3, 7, /**/ 0, 2, 4, 7, /**/ 0, 2, 5, 7, /**/ 0, 2, 6, 7, /**/ 0, 3, 4, 7,
    0, 3, 5, 7, /**/ 0, 3, 6, 7, /**/ 0, 4, 5, 7, /**/ 0, 4, 6, 7, /**/ 0, 5, 6, 7,
    0, 1, 7, /**/ 0, 2, 7, /**/ 0, 3, 7, /**/ 0, 4, 7, /**/ 0, 5, 7, /**/ 0, 6, 7,
    0, 7, /**/ 0, 1, 2, 3, 4, 5, 6, 7, 0}; /* last 0 pad to be able to read [1-7] */
// clang-format on

static constexpr const sel_t *BITMAP_SELVEC_POSITIONS = BITMAP_SELVEC_POSITIONS_STORAGE;

// Emit the set-bit positions of one byte (forward): writes 8 slots (padded), returns the popcount.
static inline sel_t BitmapSelectionEmitByte(sel_t *__restrict dst, sel_t base, uint8_t pattern) {
	const auto packed = BITMAP_SELVEC_OFFSETS[pattern];
	const auto len = UnsafeNumericCast<sel_t>(packed & 0xF);
	const auto *src = BITMAP_SELVEC_POSITIONS + (packed >> 4);
	for (idx_t j = 0; j < 8; j++) {
		dst[j] = base + src[j];
	}
	return len;
}

static inline validity_t BitmapSelectionLoadWord(const validity_t *bm, idx_t word_idx, idx_t word_count, idx_t count) {
	auto word = bm[word_idx];
	if (word_idx + 1 == word_count && (count & 63)) {
		word &= (validity_t(1) << (count & 63)) - 1;
	}
	return word;
}

static inline idx_t BitmapToSelectionVector(const validity_t *bm, idx_t count, SelectionVector &sel) {
	const auto word_count = (count + 63) / 64;
	if (word_count == 0) {
		return 0;
	}

	// EmitByte writes 8 slots (padded) at each step, so reserve a byte of head room past the last match
	const auto needed_capacity = word_count * 64 + 8;
	auto sel_data = sel.sel_data();
	auto *result_sel = sel_data ? reinterpret_cast<sel_t *>(sel_data->owned_data.get()) : nullptr;
	auto result_capacity = sel_data ? sel_data->owned_data.GetSize() / sizeof(sel_t) : idx_t(0);
	if (!sel_data || result_capacity < needed_capacity) {
		sel.Initialize(MaxValue<idx_t>(needed_capacity, STANDARD_VECTOR_SIZE));
		sel_data = sel.sel_data();
		result_sel = sel.data();
		result_capacity = sel.Capacity();
	}
	D_ASSERT(result_sel && result_capacity >= needed_capacity);

	// sample the first word(s) to choose between sparse(ctz, few set bits) and table-based extraction (dense)
	auto *dst = result_sel;
	const auto sample = MinValue<idx_t>(word_count, 2);
	for (idx_t w = 0; w < sample; w++) {
		auto base = UnsafeNumericCast<sel_t>(w * 64);
		for (auto word = BitmapSelectionLoadWord(bm, w, word_count, count); word; word >>= 8, base += 8) {
			dst += BitmapSelectionEmitByte(dst, base, static_cast<uint8_t>(word));
		}
	}
	const bool sparse = UnsafeNumericCast<idx_t>(dst - result_sel) <= 14 * sample;
	if (sparse) {
		// sparse kernel uses CountZeros to skip many consecutive 0 bits in one iteration
		for (idx_t w = sample; w < word_count; w++) {
			auto base = UnsafeNumericCast<sel_t>(w * 64);
			auto word = BitmapSelectionLoadWord(bm, w, word_count, count);
			while (word) {
				*dst++ = base + UnsafeNumericCast<sel_t>(CountZeros<uint64_t>::Trailing(word));
				word &= word - 1;
			}
		}
	} else {
		// dense kernel uses a lookup table to generate (up to) 8 sel_t indexes per iteration
		for (idx_t w = sample; w < word_count; w++) {
			auto base = UnsafeNumericCast<sel_t>(w * 64);
			auto word = BitmapSelectionLoadWord(bm, w, word_count, count);
			for (; word; word >>= 8, base += 8) {
				dst += BitmapSelectionEmitByte(dst, base, static_cast<uint8_t>(word));
			}
		}
	}
	const auto result_count = UnsafeNumericCast<idx_t>(dst - result_sel);
	sel.Initialize(sel_data, result_sel, result_capacity);
	return result_count;
}

} // namespace duckdb
