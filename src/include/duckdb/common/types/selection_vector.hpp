//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/selection_vector.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/vector_size.hpp"

namespace duckdb {
class VectorBuffer;

struct SelectionData {
	DUCKDB_API explicit SelectionData(idx_t count);

	unsafe_unique_array<sel_t> owned_data;
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
		Initialize(STANDARD_VECTOR_SIZE);
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
		sel_vector = selection_data->owned_data.get();
	}
	void Initialize(buffer_ptr<SelectionData> data) {
		selection_data = std::move(data);
		sel_vector = selection_data->owned_data.get();
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
		return sel_vector ? sel_vector[idx] : idx;
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

	string ToString(idx_t count = 0) const;
	void Print(idx_t count = 0) const;

	inline sel_t &operator[](idx_t index) const {
		return sel_vector[index];
	}
	inline bool IsSet() const {
		return sel_vector;
	}
	void Verify(idx_t count, idx_t vector_size) const;

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

// Contains a selection vector, combined with a count
class ManagedSelection {
public:
	explicit inline ManagedSelection(idx_t size, bool initialize = true)
	    : initialized(initialize), size(size), internal_opt_selvec(nullptr) {
		count = 0;
		if (!initialized) {
			return;
		}
		sel_vec.Initialize(size);
		internal_opt_selvec.Initialize(&sel_vec);
	}

public:
	bool Initialized() const {
		return initialized;
	}
	void Initialize(idx_t new_size) {
		D_ASSERT(!initialized);
		this->size = new_size;
		sel_vec.Initialize(new_size);
		internal_opt_selvec.Initialize(&sel_vec);
		initialized = true;
	}

	inline idx_t operator[](idx_t index) const {
		D_ASSERT(index < size);
		return sel_vec.get_index(index);
	}
	inline bool IndexMapsToLocation(idx_t idx, idx_t location) const {
		return idx < count && sel_vec.get_index(idx) == location;
	}
	inline void Append(const idx_t idx) {
		internal_opt_selvec.Append(count, idx);
	}
	inline idx_t Count() const {
		return count;
	}
	inline idx_t Size() const {
		return size;
	}
	inline const SelectionVector &Selection() const {
		return sel_vec;
	}
	inline SelectionVector &Selection() {
		return sel_vec;
	}

private:
	bool initialized = false;
	idx_t count;
	idx_t size;
	SelectionVector sel_vec;
	OptionalSelection internal_opt_selvec;
};

} // namespace duckdb
