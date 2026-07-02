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

struct SelectionData {
	DUCKDB_API explicit SelectionData(idx_t count);
	//! Empty SelectionData, used as the backing store for a bitmap-backed SelectionVector
	//! (the index array in owned_data is allocated lazily on Flatten()).
	SelectionData() = default;

	//! The materialized index array (sel_t[]); for bitmap-backed data this is filled lazily.
	AllocatedData owned_data;
	//! Optional bitmap representation: one bit per row over [0, row_span). When is_bitmap is set,
	//! the SelectionVector is bitmap-backed and owned_data is empty until Flatten() materializes it.
	AllocatedData bitmap_data; // 64-bit words (validity_t layout)
	idx_t row_span = 0;
	bool is_bitmap = false;
};

struct SelectionVector {
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
	}
	explicit SelectionVector(buffer_ptr<SelectionData> &&data) {
		Initialize(std::move(data));
	}
	SelectionVector &operator=(SelectionVector &&other) noexcept {
		sel_vector = other.sel_vector;
		other.sel_vector = nullptr;
		selection_data = std::move(other.selection_data);
		capacity = other.capacity;
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
		if (DUCKDB_UNLIKELY(!sel_vector)) {
			MakeWritable();
		}
		D_ASSERT(idx < capacity);
		sel_vector[idx] = UnsafeNumericCast<sel_t>(loc);
	}
	inline void swap(idx_t i, idx_t j) { // NOLINT: allow casing for legacy reasons
		if (DUCKDB_UNLIKELY(!sel_vector && IsBitmap())) {
			Flatten();
		}
		D_ASSERT(i < capacity && j < capacity);
		sel_t tmp = sel_vector[i];
		sel_vector[i] = sel_vector[j];
		sel_vector[j] = tmp;
	}
	inline idx_t get_index(idx_t idx) const { // NOLINT: allow casing for legacy reasons
		if (sel_vector) {
			return get_index_unsafe(idx);
		}
		if (DUCKDB_UNLIKELY(IsBitmap())) {
			Flatten();
			return get_index_unsafe(idx);
		}
		return idx;
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

	//! Whether this selection is currently backed by a bitmap (and not yet materialized to indices).
	inline bool IsBitmap() const {
		return selection_data && selection_data->is_bitmap && !sel_vector;
	}
	//! Domain of the bitmap: bits cover rows [0, RowSpan()).
	idx_t RowSpan() const {
		return selection_data ? selection_data->row_span : 0;
	}
	//! Materialize a bitmap-backed selection into an index array (no-op otherwise). const because it
	//! only fills the lazy cache; mutates mutable members.
	void Flatten() const;

	//! Reached on a write (set_index) into a selection with no index array: lazily provide a writable index
	//! buffer. A never-read bitmap is replaced over its full domain (its content is discarded because the
	//! caller is overwriting it); an empty selection gets a fresh standard-sized buffer.
	void MakeWritable() {
		Initialize(IsBitmap() ? MaxValue<idx_t>(selection_data->row_span, STANDARD_VECTOR_SIZE) : STANDARD_VECTOR_SIZE);
	}

	//! Prepare this selection to be bitmap-backed over [0, row_span): ensures a fixed
	//! STANDARD_VECTOR_SIZE-bit buffer (allocated once, reused across calls), marks it bitmap, and
	//! returns the writable 64-bit words for the producer to fill. Caller must fill all NWORDS words.
	uint64_t *PrepareBitmap(idx_t row_span) {
		static constexpr idx_t NWORDS = (STANDARD_VECTOR_SIZE + 63) / 64;
		if (!selection_data || !selection_data->bitmap_data.get()) {
			auto data = make_shared_ptr<SelectionData>();
			data->bitmap_data = Allocator::DefaultAllocator().Allocate(NWORDS * sizeof(uint64_t));
			selection_data = std::move(data);
		}
		selection_data->is_bitmap = true;
		selection_data->row_span = row_span;
		sel_vector = nullptr; // the bitmap is now the source of truth
		capacity = row_span;
		return reinterpret_cast<uint64_t *>(selection_data->bitmap_data.get());
	}

	//! AND another bitmap-backed selection (same row span) into this one; returns the resulting set-bit
	//! count. Both selections must currently be bitmap-backed. Used to intersect conjunction predicates.
	idx_t IntersectBitmap(const SelectionVector &other);

	//! Cheap identity token for "same selection" comparisons WITHOUT materializing: the index-array
	//! pointer for index reps, or the bitmap buffer pointer for bitmap reps (nullptr for flat).
	//! Not meant to be dereferenced.
	const void *RepresentationHandle() const {
		if (sel_vector) {
			return sel_vector;
		}
		return selection_data && selection_data->is_bitmap ? selection_data->bitmap_data.get() : nullptr;
	}
	//! Two selections are the same if they share the same backing (index array or bitmap) and span.
	static bool SameSelection(const SelectionVector &a, const SelectionVector &b) {
		return a.RepresentationHandle() == b.RepresentationHandle() && a.RowSpan() == b.RowSpan() &&
		       a.capacity == b.capacity;
	}
	buffer_ptr<SelectionData> Slice(const SelectionVector &sel, idx_t count) const;
	idx_t SliceInPlace(const SelectionVector &sel, idx_t count);

	string ToString(idx_t count = 0) const;
	void Print(idx_t count = 0) const;

	inline const sel_t &operator[](idx_t index) const {
		D_ASSERT(index < capacity);
		return sel_vector[index];
	}
	inline sel_t &operator[](idx_t index) {
		D_ASSERT(index < capacity);
		return sel_vector[index];
	}
	inline bool IsSet() const {
		return sel_vector || IsBitmap();
	}
	void Verify(idx_t count, idx_t vector_size) const;
	void Sort(idx_t count);
	idx_t GetAllocationSize() const;

private:
	// mutable: Flatten() lazily materializes a bitmap into an index array under const access
	mutable sel_t *sel_vector;
	mutable buffer_ptr<SelectionData> selection_data;
	mutable idx_t capacity;
};

} // namespace duckdb
