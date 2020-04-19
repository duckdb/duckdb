//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/selection_vector.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
class VectorBuffer;

struct SelectionData {
	SelectionData(idx_t count) {
		owned_data = unique_ptr<sel_t[]>(new sel_t[count]);
	}

	unique_ptr<sel_t[]> owned_data;
};

struct SelectionVector {
	SelectionVector() : sel_vector(nullptr) {
	}
	SelectionVector(sel_t *sel) {
		Initialize(sel);
	}
	SelectionVector(idx_t count) {
		Initialize(count);
	}
	SelectionVector(const SelectionVector &sel_vector) {
		Initialize(sel_vector);
	}
	SelectionVector(buffer_ptr<SelectionData> data) {
		Initialize(move(data));
	}

public:
	void Initialize(sel_t *sel) {
		selection_data.reset();
		sel_vector = sel;
	}
	void Initialize(idx_t count = STANDARD_VECTOR_SIZE) {
		selection_data = make_buffer<SelectionData>(count);
		sel_vector = selection_data->owned_data.get();
	}
	void Initialize(buffer_ptr<SelectionData> data) {
		selection_data = move(data);
		sel_vector = selection_data->owned_data.get();
	}
	void Initialize(const SelectionVector &other) {
		selection_data = other.selection_data;
		sel_vector = other.sel_vector;
	}

	bool empty() const {
		return !sel_vector;
	}
	void set_index(idx_t idx, idx_t loc) {
		sel_vector[idx] = loc;
	}
	void swap(idx_t i, idx_t j) {
		sel_t tmp = sel_vector[i];
		sel_vector[i] = sel_vector[j];
		sel_vector[j] = tmp;
	}
	idx_t get_index(idx_t idx) const {
		return sel_vector[idx];
	}
	sel_t *data() {
		return sel_vector;
	}
	buffer_ptr<SelectionData> sel_data() {
		return selection_data;
	}
	buffer_ptr<SelectionData> Slice(const SelectionVector &sel, idx_t count);

	string ToString(idx_t count = 0) const;
	void Print(idx_t count = 0) const;

private:
	sel_t *sel_vector;
	buffer_ptr<SelectionData> selection_data;
};

typedef unordered_map<sel_t *, buffer_ptr<VectorBuffer>> sel_cache_t;

} // namespace duckdb
