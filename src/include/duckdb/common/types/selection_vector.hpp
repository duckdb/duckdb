//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/selection_vector.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

struct SelectionData {
	SelectionData(idx_t count) {
		owned_data = unique_ptr<sel_t[]>(new sel_t[count]);;
	}

	unique_ptr<sel_t[]> owned_data;
};

struct SelectionVector {
	SelectionVector() : sel_vector(nullptr) {}
	SelectionVector(sel_t *sel) {
		Initialize(sel);
	}
	SelectionVector(idx_t count) {
		Initialize(count);
	}
	SelectionVector(const SelectionVector &sel_vector) {
		Initialize(sel_vector);
	}

	void Initialize(sel_t *sel) {
		selection_data.reset();
		sel_vector = sel;
	}
	void Initialize(idx_t count = STANDARD_VECTOR_SIZE) {
		selection_data = make_buffer<SelectionData>(count);
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
	idx_t get_index(idx_t idx) const {
		return sel_vector[idx];
	}
private:
	sel_t *sel_vector;
	buffer_ptr<SelectionData> selection_data;
};

}
