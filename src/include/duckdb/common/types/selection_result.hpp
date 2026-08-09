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
	// AND + popcount; the target attr fuses ToBitmap with the word loop (reachable only behind CpuBenefitsFromAutoVec)
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
	uint8_t *Bitmap() const { // one 0/1 byte per row
		return reinterpret_cast<uint8_t *>(selection_data->bitmap_data.get());
	}
	uint8_t *PrepareBitmap(idx_t row_span) {
		D_ASSERT(CpuBenefitsFromAutoVec());         // bitmap existence gates the avx2-targeted kernels
		D_ASSERT(row_span <= STANDARD_VECTOR_SIZE); // fixed vector-sized bitmap buffer
		if (!selection_data || selection_data.use_count() > 1) {
			selection_data = make_shared_ptr<SelectionData>();
		}
		if (!selection_data->bitmap_data.get()) {
			selection_data->bitmap_data = Allocator::DefaultAllocator().Allocate(STANDARD_VECTOR_SIZE);
		}
		selection_data->indices_cached = false;
		selection_data->is_bitmap = true;
		selection_data->row_span = row_span;
		sel_vector = nullptr;
		capacity = row_span;
		return Bitmap();
	}

private:
	DUCKDB_AUTOVEC_TARGET void IndexToBitmap(idx_t count, idx_t row_span) {
		D_ASSERT(!IsBitmap() && row_span <= STANDARD_VECTOR_SIZE);
		auto keep = selection_data;
		auto indices = sel_vector;
		auto bytes = PrepareBitmap(row_span);
		memset(bytes, 0, STANDARD_VECTOR_SIZE);
		if (!indices) {
			D_ASSERT(count <= row_span);
			memset(bytes, 1, count);
			return;
		}
		DUCKDB_UNROLL_LOOP
		for (idx_t i = 0; i < count; i++) {
			bytes[indices[i]] = 1;
		}
	}
	//! AND (or OR) another bytemap in, eight rows per word: popcount of 0/1 bytes is the surviving count.
	template <bool IS_UNION>
	DUCKDB_AUTOVEC_TARGET idx_t CombineBitmap(const uint8_t *other_bytemap) {
		D_ASSERT(IsBitmap());
		selection_data->indices_cached = false;
		auto a = reinterpret_cast<validity_t *>(Bitmap());
		auto b = reinterpret_cast<const validity_t *>(other_bytemap);
		const idx_t nwords = (selection_data->row_span + 7) / 8;
		idx_t total = 0;
		DUCKDB_UNROLL_LOOP
		for (idx_t w = 0; w < nwords; w++) {
			a[w] = IS_UNION ? (a[w] | b[w]) : (a[w] & b[w]);
			total += CountOnes<validity_t>::Count(a[w]);
		}
		return total;
	}
};

} // namespace duckdb
