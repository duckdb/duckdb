//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/selection_result.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/types/validity_mask.hpp"

#include <cstring>

#if defined(_MSC_VER)
#include <intrin.h>
#endif

namespace duckdb {

struct SelectionResult : public SelectionVector {
	using SelectionVector::SelectionVector;

	void EnsureIndexWritable(idx_t count) {
		if (sel_vector && capacity >= count) {
			return;
		}
		if (selection_data && selection_data.use_count() == 1 &&
		    selection_data->owned_data.GetSize() >= count * sizeof(sel_t)) {
			selection_data->is_bitmap = false;
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

	idx_t Intersect(SelectionVector &other, idx_t count, idx_t other_count, idx_t row_span) {
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

	uint64_t *PrepareBitmap(idx_t row_span) {
		static constexpr idx_t NWORDS = (STANDARD_VECTOR_SIZE + 63) / 64;
		if (!selection_data || selection_data.use_count() > 1) {
			selection_data = make_shared_ptr<SelectionData>();
		}
		if (!selection_data->bitmap_data.get()) {
			selection_data->bitmap_data = Allocator::DefaultAllocator().Allocate(NWORDS * sizeof(uint64_t));
		}
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
		auto a = reinterpret_cast<validity_t *>(selection_data->bitmap_data.get());
		const idx_t nwords = (selection_data->row_span + 63) / 64;
		idx_t total = 0;
		for (idx_t w = 0; w < nwords; w++) {
			a[w] &= other_bitmap[w];
#if defined(_MSC_VER)
			total += idx_t(__popcnt64(a[w]));
#else
			total += idx_t(__builtin_popcountll(a[w]));
#endif
		}
		return total;
	}
};

} // namespace duckdb
