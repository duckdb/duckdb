//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/filter_result.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/types/bitmap_selection_vector.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/types/validity_mask.hpp"
#include "duckdb/common/vector_size.hpp"

namespace duckdb {

inline void ScatterSelectionToBitmap(const SelectionVector &sel, idx_t sel_count, idx_t row_span,
                                     validity_t *__restrict bitmap) {
	const idx_t nw = (row_span + 63) / 64;
	for (idx_t w = 0; w < nw; w++) {
		bitmap[w] = 0;
	}
	for (idx_t i = 0; i < sel_count; i++) {
		auto idx = sel.get_index(i);
		bitmap[idx / 64] |= validity_t(1) << (idx % 64);
	}
}

struct ScanFilterResult {
	enum class Rep : uint8_t { BITMAP, SELVEC };
	static constexpr idx_t NWORDS = (STANDARD_VECTOR_SIZE + 63) / 64;

	Rep rep = Rep::BITMAP;
	idx_t row_span = 0;
	idx_t sel_count = 0;
	validity_t bitmap[NWORDS];
	SelectionVector sel;

	void Init(idx_t span, bool all_pass) {
		rep = Rep::BITMAP;
		row_span = span;
		const idx_t nw = (span + 63) / 64;
		for (idx_t w = 0; w < nw; w++) {
			bitmap[w] = all_pass ? ~validity_t(0) : validity_t(0);
		}
		if (all_pass && (span & 63)) {
			bitmap[nw - 1] &= (validity_t(1) << (span & 63)) - 1;
		}
	}

	void InitFromSelection(const SelectionVector &s, idx_t s_count, idx_t span) {
		if (s_count == span) {
			Init(span, true); // all rows selected: word-fill instead of setting each bit individually
			return;
		}
		rep = Rep::BITMAP;
		row_span = span;
		ScatterSelectionToBitmap(s, s_count, span, bitmap);
	}
};

inline void ScanFilterAnd(ScanFilterResult &acc, const ScanFilterResult &child) {
	D_ASSERT(acc.row_span == child.row_span);
	const idx_t nw = (acc.row_span + 63) / 64;
	if (acc.rep == ScanFilterResult::Rep::SELVEC) {
		ScatterSelectionToBitmap(acc.sel, acc.sel_count, acc.row_span, acc.bitmap);
		acc.rep = ScanFilterResult::Rep::BITMAP;
	}
	if (child.rep == ScanFilterResult::Rep::BITMAP) {
		for (idx_t w = 0; w < nw; w++) {
			acc.bitmap[w] &= child.bitmap[w];
		}
	} else {
		validity_t mask[ScanFilterResult::NWORDS];
		ScatterSelectionToBitmap(child.sel, child.sel_count, child.row_span, mask);
		for (idx_t w = 0; w < nw; w++) {
			acc.bitmap[w] &= mask[w];
		}
	}
}

inline bool ScanFilterResultIsEmpty(const ScanFilterResult &r) {
	if (r.rep == ScanFilterResult::Rep::SELVEC) {
		return r.sel_count == 0;
	}
	const idx_t nw = (r.row_span + 63) / 64;
	validity_t any = 0;
	for (idx_t w = 0; w < nw; w++) {
		any |= r.bitmap[w];
	}
	return any == 0;
}

inline idx_t MaterializeScanFilterResult(const ScanFilterResult &r, SelectionVector &out_sel) {
	if (r.rep == ScanFilterResult::Rep::SELVEC) {
		out_sel.Initialize(r.sel);
		return r.sel_count;
	}
	return BitmapToSelectionVector(r.bitmap, r.row_span, out_sel);
}

} // namespace duckdb
