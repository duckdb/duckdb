//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/selection_result.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/bit_utils.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/types/validity_mask.hpp"

#include <cstring>

namespace duckdb {

//! Private inheritance: a SelectionResult cannot be passed where a plain SelectionVector is expected, so
//! per-value index access on a possibly-bitmap selection is a compile error; Flattened() is the explicit,
//! once-per-vector conversion. The safe (non-indexing) part of the base API is re-exposed below.
struct SelectionResult : private SelectionVector {
	using SelectionVector::Capacity;
	using SelectionVector::Initialize;
	using SelectionVector::IsBitmap;
	using SelectionVector::IsSet;
	using SelectionVector::RowSpan;
	using SelectionVector::SelectionVector;

	//! Materialize a bitmap (no-op if flat) and view as a plain, index-only SelectionVector.
	SelectionVector &Flattened() {
		Flatten();
		return *this;
	}
	//! Share another result's representation (the base overload cannot accept a SelectionResult from outside).
	void Initialize(const SelectionResult &other) {
		SelectionVector::Initialize(static_cast<const SelectionVector &>(other));
	}
	//! Swap representations with a plain selection (used to hand a result over to a caller-owned output).
	void SwapInto(SelectionVector &out) {
		std::swap(out, static_cast<SelectionVector &>(*this));
	}

	void EnsureIndexWritable(idx_t count) {
		if (sel_vector && capacity >= count) {
			if (selection_data) {
				selection_data->is_bitmap = false;
				selection_data->index_cache_offset = DConstants::INVALID_INDEX;
			}
			return;
		}
		if (selection_data && selection_data.use_count() == 1 &&
		    selection_data->owned_data.GetSize() >= count * sizeof(sel_t)) {
			selection_data->is_bitmap = false;
			selection_data->index_cache_offset = DConstants::INVALID_INDEX;
			sel_vector = reinterpret_cast<sel_t *>(selection_data->owned_data.get());
			capacity = selection_data->owned_data.GetSize() / sizeof(sel_t);
			return;
		}
		Initialize(MaxValue<idx_t>(count, STANDARD_VECTOR_SIZE));
	}

	void ToBitmap(idx_t count, idx_t row_span) {
		if (!IsBitmap()) {
			IndexToBitmap(count, row_span);
		}
	}

	idx_t Intersect(SelectionResult &other, idx_t count, idx_t other_count, idx_t row_span) {
		ToBitmap(count, row_span);
		if (!other.IsSet()) {
			D_ASSERT(other_count == row_span);
			return count;
		}
		if (other.IsBitmap()) {
			D_ASSERT(other.RowSpan() == row_span);
			auto other_bitmap = reinterpret_cast<const validity_t *>(other.selection_data->bitmap_data.get());
			return IntersectBitmap(other_bitmap);
		}
		SelectionResult other_result;
		other_result.Initialize(other);
		other_result.ToBitmap(other_count, row_span);
		return IntersectBitmap(other_result);
	}

	//! Fill this as the bitwise complement of `other` (a bitmap) over row_span rows; returns the bitmap words.
	validity_t *Complement(const SelectionResult &other, idx_t row_span) {
		D_ASSERT(other.IsBitmap() && other.RowSpan() == row_span);
		auto dst = reinterpret_cast<validity_t *>(PrepareBitmap(row_span));
		auto src = reinterpret_cast<const validity_t *>(other.selection_data->bitmap_data.get());
		for (idx_t w = 0; w < (row_span + 63) / 64; w++) {
			dst[w] = ~src[w];
		}
		return dst;
	}

	uint64_t *PrepareBitmap(idx_t row_span) {
		static constexpr idx_t NWORDS = (STANDARD_VECTOR_SIZE + 63) / 64;
		if (!selection_data || selection_data.use_count() > 1) {
			selection_data = make_shared_ptr<SelectionData>();
		}
		if (!selection_data->bitmap_data.get()) {
			selection_data->bitmap_data = Allocator::DefaultAllocator().Allocate(NWORDS * sizeof(uint64_t));
		}
		selection_data->index_cache_offset = DConstants::INVALID_INDEX;
		selection_data->is_bitmap = true;
		selection_data->row_span = row_span;
		sel_vector = nullptr;
		capacity = row_span;
		return reinterpret_cast<uint64_t *>(selection_data->bitmap_data.get());
	}

private:
	void IndexToBitmap(idx_t count, idx_t row_span) {
		D_ASSERT(!IsBitmap() && row_span <= STANDARD_VECTOR_SIZE);
		auto keep = selection_data;
		auto indices = sel_vector;
		auto words = PrepareBitmap(row_span);
		memset(words, 0, ((STANDARD_VECTOR_SIZE + 63) / 64) * sizeof(uint64_t));
		if (!indices) {
			D_ASSERT(count <= row_span);
			for (idx_t i = 0; i < count; i++) {
				words[i >> 6] |= uint64_t(1) << (i & 63);
			}
			return;
		}
		for (idx_t i = 0; i < count; i++) {
			auto idx = indices[i];
			words[idx >> 6] |= uint64_t(1) << (idx & 63);
		}
	}

	idx_t IntersectBitmap(const SelectionResult &other) {
		D_ASSERT(IsBitmap() && other.IsBitmap());
		D_ASSERT(RowSpan() == other.RowSpan());
		auto b = reinterpret_cast<const validity_t *>(other.selection_data->bitmap_data.get());
		return IntersectBitmap(b);
	}

	idx_t IntersectBitmap(const validity_t *other_bitmap) {
		D_ASSERT(IsBitmap());
		selection_data->index_cache_offset = DConstants::INVALID_INDEX;
		auto a = reinterpret_cast<validity_t *>(selection_data->bitmap_data.get());
		const idx_t nwords = (selection_data->row_span + 63) / 64;
		idx_t total = 0;
		for (idx_t w = 0; w < nwords; w++) {
			a[w] &= other_bitmap[w];
			total += CountOnes<validity_t>::Count(a[w]);
		}
		return total;
	}
};

} // namespace duckdb
