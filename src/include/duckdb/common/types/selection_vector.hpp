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
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/vector_size.hpp"

namespace duckdb {
class VectorBuffer;

struct SelectionData {
	DUCKDB_API explicit SelectionData(idx_t count);

	AllocatedData owned_data;
};

struct SelectionVector {
	SelectionVector() : sel_vector(nullptr) {
	}
	explicit SelectionVector(sel_t *sel) {
		Initialize(sel);
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
	explicit SelectionVector(buffer_ptr<SelectionData> data) {
		Initialize(std::move(data));
	}
	SelectionVector &operator=(SelectionVector &&other) noexcept {
		sel_vector = other.sel_vector;
		other.sel_vector = nullptr;
		selection_data = std::move(other.selection_data);
		return *this;
	}

public:
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

	void Initialize(sel_t *sel) {
		selection_data.reset();
		sel_vector = sel;
	}
	void Initialize(idx_t count = STANDARD_VECTOR_SIZE) {
		selection_data = make_shared_ptr<SelectionData>(count);
		sel_vector = reinterpret_cast<sel_t *>(selection_data->owned_data.get());
	}
	void Initialize(buffer_ptr<SelectionData> data) {
		selection_data = std::move(data);
		sel_vector = reinterpret_cast<sel_t *>(selection_data->owned_data.get());
	}
	void Initialize(const SelectionVector &other) {
		selection_data = other.selection_data;
		sel_vector = other.sel_vector;
	}

	inline void set_index(idx_t idx, idx_t loc) { // NOLINT: allow casing for legacy reasons
		sel_vector[idx] = UnsafeNumericCast<sel_t>(loc);
	}
	inline void swap(idx_t i, idx_t j) { // NOLINT: allow casing for legacy reasons
		sel_t tmp = sel_vector[i];
		sel_vector[i] = sel_vector[j];
		sel_vector[j] = tmp;
	}
	inline idx_t get_index(idx_t idx) const { // NOLINT: allow casing for legacy reasons
		return sel_vector ? get_index_unsafe(idx) : idx;
	}
	inline idx_t get_index_unsafe(idx_t idx) const { // NOLINT: allow casing for legacy reasons
		return sel_vector[idx];
	}
	sel_t *data() { // NOLINT: allow casing for legacy reasons
		return sel_vector;
	}
	const sel_t *data() const { // NOLINT: allow casing for legacy reasons
		return sel_vector;
	}
	buffer_ptr<SelectionData> sel_data() { // NOLINT: allow casing for legacy reasons
		return selection_data;
	}
	buffer_ptr<SelectionData> Slice(const SelectionVector &sel, idx_t count) const;
	idx_t SliceInPlace(const SelectionVector &sel, idx_t count);

	string ToString(idx_t count = 0) const;
	void Print(idx_t count = 0) const;

	inline const sel_t &operator[](idx_t index) const {
		return sel_vector[index];
	}
	inline sel_t &operator[](idx_t index) {
		return sel_vector[index];
	}
	inline bool IsSet() const {
		return sel_vector;
	}
	void Verify(idx_t count, idx_t vector_size) const;
	void Sort(idx_t count);

private:
	sel_t *sel_vector;
	buffer_ptr<SelectionData> selection_data;
};

class OptionalSelection {
public:
	explicit OptionalSelection(SelectionVector *sel_p) {
		Initialize(sel_p);
	}
	void Initialize(SelectionVector *sel_p) {
		sel = sel_p;
		if (sel) {
			vec.Initialize(sel->data());
			sel = &vec;
		}
	}

	inline operator SelectionVector *() { // NOLINT: allow implicit conversion to SelectionVector
		return sel;
	}

	inline void Append(idx_t &count, const idx_t idx) {
		if (sel) {
			sel->set_index(count, idx);
		}
		++count;
	}

	inline void Advance(idx_t completed) {
		if (sel) {
			sel->Initialize(sel->data() + completed);
		}
	}

private:
	SelectionVector *sel;
	SelectionVector vec;
};

} // namespace duckdb
