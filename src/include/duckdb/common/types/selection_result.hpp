//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/selection_result.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/autovec.hpp"
#include "duckdb/common/bit_utils.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/types/validity_mask.hpp"

#include <cstring>

namespace duckdb {

//! Bitmap-capable selection result; Flattened() materializes an index selection.
struct SelectionResult : private SelectionVector {
	using SelectionVector::Capacity;
	using SelectionVector::Initialize;
	using SelectionVector::IsBitmap;
	using SelectionVector::IsSet;
	using SelectionVector::RowSpan;
	using SelectionVector::SelectionVector;

	SelectionVector &Flattened() { // explicit bitmap-to-index boundary
		Flatten();
		return *this;
	}
	void Initialize(const SelectionResult &other) { // expose sharing between results
		SelectionVector::Initialize(static_cast<const SelectionVector &>(other));
	}
	void SwapInto(SelectionVector &out) { // hand bitmap-capable result to plain selection output
		std::swap(out, static_cast<SelectionVector &>(*this));
	}

	void EnsureIndexWritable(idx_t count) {
		const auto *before = selection_data.get();
		EnsureCapacity(count);
		if (selection_data && selection_data.get() == before) { // recycled: it is no longer a bitmap
			selection_data->is_bitmap = false;
			selection_data->indices_cached = false;
		}
	}

	void ToBitmap(idx_t count, idx_t row_span) { // promote index selection to bitmap
		if (!IsBitmap()) {
			IndexToBitmap(count, row_span);
		}
	}

	// AND + popcount; target attr fuses ToBitmap with the word loops (only reachable behind CpuBenefitsFromAutoVec)
	DUCKDB_AUTOVEC_TARGET idx_t Intersect(SelectionResult &other, idx_t count, idx_t other_count, idx_t row_span) {
		ToBitmap(count, row_span);
		if (!other.IsSet()) {
			D_ASSERT(other_count == row_span);
			return count;
		}
		if (other.IsBitmap()) {
			D_ASSERT(other.RowSpan() == row_span);
			return CombineBitmap<false>(other.Bitmap());
		}
		SelectionResult other_result;
		other_result.Initialize(other);
		other_result.ToBitmap(other_count, row_span);
		D_ASSERT(RowSpan() == other_result.RowSpan());
		return CombineBitmap<false>(other_result.Bitmap());
	}
	DUCKDB_AUTOVEC_TARGET idx_t Union(SelectionResult &other) { // OR + popcount
		D_ASSERT(IsBitmap() && other.IsBitmap());
		D_ASSERT(RowSpan() == other.RowSpan());
		return CombineBitmap<true>(other.Bitmap());
	}

	DUCKDB_AUTOVEC_TARGET validity_t *Complement(const SelectionResult &other, idx_t row_span) { // false-side bitmap
		D_ASSERT(other.IsBitmap() && other.RowSpan() == row_span);
		auto dst = reinterpret_cast<validity_t *>(PrepareBitmap(row_span));
		auto src = other.Bitmap();
		DUCKDB_UNROLL_LOOP
		for (idx_t w = 0; w < (row_span + 63) / 64; w++) {
			dst[w] = ~src[w];
		}
		return dst;
	}

	validity_t *Bitmap() const {
		return reinterpret_cast<validity_t *>(selection_data->bitmap_data.get());
	}

	uint64_t *PrepareBitmap(idx_t row_span) {
		static constexpr idx_t NWORDS = (STANDARD_VECTOR_SIZE + 63) / 64;
		D_ASSERT(CpuBenefitsFromAutoVec());         // bitmap existence gates the avx2-targeted kernels
		D_ASSERT(row_span <= STANDARD_VECTOR_SIZE); // fixed vector-sized bitmap buffer
		if (!selection_data || selection_data.use_count() > 1) {
			selection_data = make_shared_ptr<SelectionData>();
		}
		if (!selection_data->bitmap_data.get()) {
			selection_data->bitmap_data = Allocator::DefaultAllocator().Allocate(NWORDS * sizeof(uint64_t));
		}
		selection_data->indices_cached = false;
		selection_data->is_bitmap = true;
		selection_data->row_span = row_span;
		sel_vector = nullptr;
		capacity = row_span;
		return reinterpret_cast<uint64_t *>(selection_data->bitmap_data.get());
	}

private:
	DUCKDB_AUTOVEC_TARGET void IndexToBitmap(idx_t count, idx_t row_span) {
		D_ASSERT(!IsBitmap() && row_span <= STANDARD_VECTOR_SIZE);
		auto keep = selection_data;
		auto indices = sel_vector;
		auto words = PrepareBitmap(row_span);
		memset(words, 0, ((STANDARD_VECTOR_SIZE + 63) / 64) * sizeof(uint64_t));
		if (!indices) {
			D_ASSERT(count <= row_span);
			DUCKDB_UNROLL_LOOP
			for (idx_t i = 0; i < count; i++) {
				words[i >> 6] |= uint64_t(1) << (i & 63);
			}
			return;
		}
		DUCKDB_UNROLL_LOOP
		for (idx_t i = 0; i < count; i++) {
			auto idx = indices[i];
			words[idx >> 6] |= uint64_t(1) << (idx & 63);
		}
	}

	//! AND (or OR) another bitmap into this one, returning the surviving count
	template <bool IS_UNION>
	DUCKDB_AUTOVEC_TARGET idx_t CombineBitmap(const validity_t *other_bitmap) {
		D_ASSERT(IsBitmap());
		selection_data->indices_cached = false;
		auto a = Bitmap();
		const idx_t nwords = (selection_data->row_span + 63) / 64;
		idx_t total = 0;
		DUCKDB_UNROLL_LOOP
		for (idx_t w = 0; w < nwords; w++) {
			a[w] = IS_UNION ? (a[w] | other_bitmap[w]) : (a[w] & other_bitmap[w]);
			total += CountOnes<validity_t>::Count(a[w]);
		}
		return total;
	}
};

} // namespace duckdb
