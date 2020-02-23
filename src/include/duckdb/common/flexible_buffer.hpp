//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/flexible_buffer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

class FlexibleBuffer {
public:
	FlexibleBuffer() : capacity(0) {}

	void Extend(index_t new_capacity) {
		if (new_capacity > capacity) {
			capacity = new_capacity;
			owned_data = unique_ptr<data_t[]>{new data_t[new_capacity]};
		}
	}

	data_ptr_t GetData() {
		return owned_data.get();
	}
private:
	index_t capacity = 0;
	unique_ptr<data_t[]> owned_data;
};

} // namespace duckdb
