//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/selection_result.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/autovec.hpp"
#include "duckdb/common/bit_utils.hpp"
#include "duckdb/common/types/bitmap_selection_vector.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/types/validity_mask.hpp"

#include <cstring>

namespace duckdb {
//! The result of selecting tuples: either an index selection or a bitmap over a row span. The bitmap form lets
//! filters combine with word-wise AND/OR; Flattened() materializes the index form, which is all that ever escapes.
struct SelectionResult {
	SelectionResult() = default;
	explicit SelectionResult(idx_t count) : indices(count) {
	}
	// move-only: the bitmap buffer is owned outright
	SelectionResult(SelectionResult &&) noexcept = default;
	SelectionResult &operator=(SelectionResult &&) noexcept = default;

	bool IsBitmap() const {
		return is_bitmap;
	}
	bool IsSet() const {
		return is_bitmap || indices.IsSet();
	}
	idx_t Capacity() const {
		return indices.Capacity();
	}
	idx_t RowSpan() const {
		return row_span;
	}
	//! Materialize the index selection; a bitmap is converted once and the indices become authoritative
	SelectionVector &Flattened() {
		if (is_bitmap) {
#if !DUCKDB_AUTOVEC
			throw InternalException("bitmap selection result in a build without autovec support");
#else
			BitmapToSelectionVector(Bitmap(), row_span, indices);
			is_bitmap = false;
#endif
		}
		return indices;
	}
	void Initialize(const SelectionVector &other) {
		is_bitmap = false;
		indices.Initialize(other);
	}
	//! Clear to "no selection", i.e. the identity over the full row span
	void Reset() {
		is_bitmap = false;
		indices.Initialize(buffer_ptr<SelectionData>());
	}
	//! Hand the index selection to a plain selection vector output, keeping its buffer here for reuse
	void SwapInto(SelectionVector &out) {
		std::swap(out, Flattened());
	}
	void EnsureIndexWritable(idx_t count) {
		is_bitmap = false;
		indices.EnsureCapacity(count);
	}
	void ToBitmap(idx_t count, idx_t span) { // promote index selection to bitmap
		if (!is_bitmap) {
			IndexToBitmap(count, span);
		}
	}
	// AND + popcount; the target attr fuses ToBitmap with the word loop (reachable only behind CpuBenefitsFromAutoVec)
	DUCKDB_AUTOVEC_TARGET idx_t Intersect(SelectionResult &other, idx_t count, idx_t other_count, idx_t span) {
		ToBitmap(count, span);
		if (!other.IsSet()) {
			D_ASSERT(other_count == span);
			return count;
		}
		other.ToBitmap(other_count, span);
		D_ASSERT(other.RowSpan() == span);
		return CombineBitmap<false>(other.Bitmap());
	}
	DUCKDB_AUTOVEC_TARGET idx_t Union(SelectionResult &other) { // OR + popcount
		D_ASSERT(IsBitmap() && other.IsBitmap());
		D_ASSERT(RowSpan() == other.RowSpan());
		return CombineBitmap<true>(other.Bitmap());
	}
	validity_t *Bitmap() {
		return reinterpret_cast<validity_t *>(bitmap_data.get());
	}
	validity_t *PrepareBitmap(idx_t span) {
		D_ASSERT(CpuBenefitsFromAutoVec());     // bitmap existence gates the avx2-targeted kernels
		D_ASSERT(span <= STANDARD_VECTOR_SIZE); // fixed vector-sized bitmap buffer
		if (!bitmap_data.IsSet()) {
			bitmap_data = Allocator::DefaultAllocator().Allocate(ValidityMask::EntryCount(STANDARD_VECTOR_SIZE) *
			                                                     sizeof(validity_t));
		}
		is_bitmap = true;
		row_span = span;
		return Bitmap();
	}

private:
	DUCKDB_AUTOVEC_TARGET void IndexToBitmap(idx_t count, idx_t span) {
		D_ASSERT(!is_bitmap && span <= STANDARD_VECTOR_SIZE);
		const auto *index_data = indices.data(); // separate buffer: unaffected by the bitmap we are about to fill
		auto words = PrepareBitmap(span);
		memset(words, 0, ValidityMask::EntryCount(STANDARD_VECTOR_SIZE) * sizeof(validity_t));
		DUCKDB_UNROLL_LOOP
		for (idx_t i = 0; i < count; i++) {
			const auto idx = index_data ? index_data[i] : i; // a null selection is the identity
			words[idx >> 6] |= validity_t(1) << (idx & 63);
		}
	}
	//! AND (or OR) another bitmap into this one, returning the surviving count
	template <bool IS_UNION>
	DUCKDB_AUTOVEC_TARGET idx_t CombineBitmap(const validity_t *b) {
		D_ASSERT(IsBitmap());
		auto a = Bitmap();
		const idx_t nwords = ValidityMask::EntryCount(row_span);
		idx_t total = 0;
		DUCKDB_UNROLL_LOOP
		for (idx_t w = 0; w < nwords; w++) {
			a[w] = IS_UNION ? (a[w] | b[w]) : (a[w] & b[w]);
			total += CountOnes<validity_t>::Count(a[w]);
		}
		return total;
	}

private:
	//! Index form; also the buffer a materialized bitmap is written into
	SelectionVector indices;
	//! Bitmap form, owned outright (never shared, unlike the index buffer)
	AllocatedData bitmap_data;
	idx_t row_span = 0;
	bool is_bitmap = false;
};

} // namespace duckdb
