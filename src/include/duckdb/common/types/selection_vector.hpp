//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/selection_vector.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/likely.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/vector_size.hpp"

namespace duckdb {
class VectorBuffer;
struct SelectionResult;

struct SelectionData {
	DUCKDB_API explicit SelectionData(idx_t count);
	SelectionData() = default;
	// Out-of-line destructor: prevents GCC IPA-ICF from folding
	// _Sp_counted_ptr_inplace<SelectionData>::_M_dispose with the
	// corresponding instantiation for TemplatedValidityData, which produces
	// a spurious -Warray-bounds with g++ >= 14.
	DUCKDB_API ~SelectionData();

	AllocatedData owned_data;
	AllocatedData bitmap_data;
	idx_t row_span = 0;
	//! Start of the materialized bitmap indices within owned_data (set by Flatten, INVALID = not filled).
	idx_t index_cache_offset = DConstants::INVALID_INDEX;
	bool is_bitmap = false;
};

struct SelectionVector {
	friend struct SelectionResult;

	SelectionVector() : sel_vector(nullptr), capacity(0) {
	}
	explicit SelectionVector(sel_t *sel, idx_t capacity) {
		Initialize(sel, capacity);
	}
	explicit SelectionVector(idx_t count) {
		Initialize(count);
	}
	SelectionVector(idx_t start, idx_t count) {
		Initialize(MaxValue<idx_t>(count, STANDARD_VECTOR_SIZE));
		for (idx_t i = 0; i < count; i++) {
			set_index(i, start + i);
		}
	}
	SelectionVector(const SelectionVector &sel_vector) {
		Initialize(sel_vector);
	}
	SelectionVector(SelectionVector &&other) noexcept
	    : sel_vector(other.sel_vector), selection_data(std::move(other.selection_data)), capacity(other.capacity) {
		other.sel_vector = nullptr;
		other.capacity = 0;
	}
	explicit SelectionVector(buffer_ptr<SelectionData> &&data) {
		Initialize(std::move(data));
	}
	SelectionVector &operator=(SelectionVector &&other) noexcept {
		sel_vector = other.sel_vector;
		other.sel_vector = nullptr;
		selection_data = std::move(other.selection_data);
		capacity = other.capacity;
		other.capacity = 0;
		return *this;
	}

public:
	//! Generate an incremental selection vector with start "start" going up to "count"
	static SelectionVector Incremental(idx_t start, idx_t count) {
		SelectionVector result(MaxValue<idx_t>(count, STANDARD_VECTOR_SIZE));
		for (idx_t i = 0; i < count; i++) {
			result.set_index(i, start + i);
		}
		return result;
	}
	//! Generate an incremental selection vector with start "0" going up to "count"
	static SelectionVector Incremental(idx_t count) {
		return Incremental(0ULL, count);
	}
	static idx_t Inverted(const SelectionVector &src, SelectionVector &dst, idx_t source_size, idx_t count) {
		idx_t src_idx = 0;
		idx_t dst_idx = 0;
		for (idx_t i = 0; i < count; i++) {
			if (src_idx < source_size && src.get_index(src_idx) == i) {
				src_idx++;
				// This index is selected by 'src', skip it in 'dst'
				continue;
			}
			// This index does not exist in 'src', add it to the selection of 'dst'
			dst.set_index(dst_idx++, i);
		}
		return dst_idx;
	}

	void Initialize(sel_t *sel, idx_t capacity_p) {
#ifdef DEBUG
		D_ASSERT(sel || capacity_p == 0);
		if (sel && capacity_p > 0) {
			(void)sel[capacity_p - 1];
		}
#endif
		selection_data.reset();
		sel_vector = sel;
		capacity = capacity_p;
	}
	void Initialize(idx_t count = STANDARD_VECTOR_SIZE) {
		selection_data = make_shared_ptr<SelectionData>(count);
		sel_vector = reinterpret_cast<sel_t *>(selection_data->owned_data.get());
		capacity = count;
	}
	void Initialize(buffer_ptr<SelectionData> data) {
		selection_data = std::move(data);
		if (selection_data) {
			sel_vector = reinterpret_cast<sel_t *>(selection_data->owned_data.get());
			capacity = selection_data->owned_data.GetSize() / sizeof(sel_t);
		} else {
			sel_vector = nullptr;
			capacity = 0ULL;
		}
	}
	void Initialize(buffer_ptr<SelectionData> data, sel_t *sel, idx_t capacity_p) {
		selection_data = std::move(data);
		sel_vector = sel;
		capacity = capacity_p;
	}
	void Initialize(const SelectionVector &other) {
		selection_data = other.selection_data;
		sel_vector = other.sel_vector;
		capacity = other.capacity;
	}

	inline void set_index(idx_t idx, idx_t loc) { // NOLINT: allow casing for legacy reasons
		D_ASSERT(sel_vector && idx < capacity);
		sel_vector[idx] = UnsafeNumericCast<sel_t>(loc);
	}
	inline void swap(idx_t i, idx_t j) { // NOLINT: allow casing for legacy reasons
		D_ASSERT(i < capacity && j < capacity);
		sel_t tmp = sel_vector[i];
		sel_vector[i] = sel_vector[j];
		sel_vector[j] = tmp;
	}
	inline idx_t get_index(idx_t idx) const { // NOLINT: allow casing for legacy reasons
		D_ASSERT(sel_vector || !IsBitmap());
		return sel_vector ? get_index_unsafe(idx) : idx;
	}
	inline idx_t get_index_unsafe(idx_t idx) const { // NOLINT: allow casing for legacy reasons
		D_ASSERT(idx < capacity);
		return sel_vector[idx];
	}
	sel_t *data() { // NOLINT: allow casing for legacy reasons
		if (DUCKDB_UNLIKELY(!sel_vector && IsBitmap())) {
			Flatten();
		}
		return sel_vector;
	}
	const sel_t *data() const { // NOLINT: allow casing for legacy reasons
		if (DUCKDB_UNLIKELY(!sel_vector && IsBitmap())) {
			Flatten();
		}
		return sel_vector;
	}
	const buffer_ptr<SelectionData> &sel_data() { // NOLINT: allow casing for legacy reasons
		return selection_data;
	}
	idx_t Capacity() const {
		return capacity;
	}

	inline bool IsBitmap() const {
		return selection_data && selection_data->is_bitmap && !sel_vector;
	}
	idx_t RowSpan() const {
		return selection_data ? selection_data->row_span : 0;
	}
	void Flatten() const;

	const void *RepresentationHandle() const {
		if (sel_vector) {
			return sel_vector;
		}
		return selection_data && selection_data->is_bitmap ? selection_data->bitmap_data.get() : nullptr;
	}
	static bool SameSelection(const SelectionVector &a, const SelectionVector &b) {
		return a.RepresentationHandle() == b.RepresentationHandle() && a.RowSpan() == b.RowSpan() &&
		       a.capacity == b.capacity;
	}
	buffer_ptr<SelectionData> Slice(const SelectionVector &sel, idx_t count) const;
	idx_t SliceInPlace(const SelectionVector &sel, idx_t count);

	string ToString(idx_t count = 0) const;
	void Print(idx_t count = 0) const;

	inline const sel_t &operator[](idx_t index) const {
		if (DUCKDB_UNLIKELY(!sel_vector && IsBitmap())) {
			Flatten();
		}
		D_ASSERT(index < capacity);
		return sel_vector[index];
	}
	inline sel_t &operator[](idx_t index) {
		if (DUCKDB_UNLIKELY(!sel_vector && IsBitmap())) {
			Flatten();
		}
		if (DUCKDB_UNLIKELY(selection_data && selection_data->is_bitmap)) {
			auto keep = selection_data;
			auto old_sel = sel_vector;
			auto old_capacity = capacity;
			Initialize(old_capacity);
			for (idx_t i = 0; i < old_capacity; i++) {
				sel_vector[i] = old_sel[i];
			}
		}
		D_ASSERT(index < capacity);
		return sel_vector[index];
	}
	inline bool IsSet() const {
		return sel_vector || IsBitmap();
	}
	void Verify(idx_t count, idx_t vector_size) const;
	void Sort(idx_t count);
	idx_t GetAllocationSize() const;

protected:
	mutable sel_t *sel_vector;
	mutable buffer_ptr<SelectionData> selection_data;
	mutable idx_t capacity;
};

} // namespace duckdb
