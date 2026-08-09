//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/bitmap_selection_vector.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/autovec.hpp"
#include "duckdb/common/bit_utils.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/types/validity_mask.hpp"

namespace duckdb {

#if DUCKDB_AUTOVEC

//! Set-bit positions per byte pattern, generated at compile time: pos[b] lists them, len[b] is the popcount.
struct BitmapSelvecTable {
	uint8_t pos[256][8];
	uint8_t len[256];
	constexpr BitmapSelvecTable() : pos(), len() {
		for (uint32_t b = 0; b < 256; b++) {
			uint8_t n = 0;
			for (uint8_t i = 0; i < 8; i++) {
				if (b & (1u << i)) {
					pos[b][n++] = i;
				}
			}
			len[b] = n;
		}
	}
};
inline constexpr BitmapSelvecTable BITMAP_SELVEC {};

// Emit the set-bit positions of one byte (forward): writes 8 slots (padded), returns the popcount.
DUCKDB_AUTOVEC_TARGET static inline sel_t BitmapSelectionEmitByte(sel_t *__restrict dst, sel_t base, uint8_t pattern) {
	const auto *src = BITMAP_SELVEC.pos[pattern];
	DUCKDB_UNROLL_LOOP
	for (idx_t j = 0; j < 8; j++) {
		dst[j] = base + src[j];
	}
	return BITMAP_SELVEC.len[pattern];
}

static inline validity_t BitmapSelectionLoadWord(const validity_t *bm, idx_t word_idx, idx_t word_count, idx_t count) {
	auto word = bm[word_idx];
	if (word_idx + 1 == word_count && (count & 63)) {
		word &= (validity_t(1) << (count & 63)) - 1;
	}
	return word;
}

//! Append the set-bit positions of words [from, to) at dst and return the new cursor.
//! SPARSE skips runs of zero bits with CountZeros; otherwise the lookup table emits (up to) 8 positions per iteration.
template <bool SPARSE>
DUCKDB_AUTOVEC_TARGET static inline sel_t *BitmapEmitWords(const validity_t *bm, idx_t from, idx_t to, idx_t word_count,
                                                           idx_t count, sel_t *dst) {
	for (idx_t w = from; w < to; w++) {
		auto base = UnsafeNumericCast<sel_t>(w * 64);
		auto word = BitmapSelectionLoadWord(bm, w, word_count, count);
		DUCKDB_UNROLL_LOOP
		while (word) {
			if constexpr (SPARSE) {
				*dst++ = base + UnsafeNumericCast<sel_t>(CountZeros<uint64_t>::Trailing(word));
				word &= word - 1;
			} else {
				dst += BitmapSelectionEmitByte(dst, base, static_cast<uint8_t>(word));
				word >>= 8;
				base += 8;
			}
		}
	}
	return dst;
}

DUCKDB_AUTOVEC_TARGET static inline idx_t BitmapToSelectionVector(const validity_t *bm, idx_t count,
                                                                  SelectionVector &sel) {
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
	const auto sample = MinValue<idx_t>(word_count, 2);
	auto *dst = BitmapEmitWords<false>(bm, 0, sample, word_count, count, result_sel);
	const bool sparse = UnsafeNumericCast<idx_t>(dst - result_sel) <= 14 * sample;
	dst = sparse ? BitmapEmitWords<true>(bm, sample, word_count, word_count, count, dst)
	             : BitmapEmitWords<false>(bm, sample, word_count, word_count, count, dst);
	const auto result_count = UnsafeNumericCast<idx_t>(dst - result_sel);
	sel.Initialize(sel_data);
	return result_count;
}

#endif // DUCKDB_AUTOVEC

} // namespace duckdb
