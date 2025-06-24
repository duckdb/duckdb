//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/numpy/numpy_result_conversion.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb_python/pybind11/pybind_wrapper.hpp"
#include "duckdb_python/numpy/array_wrapper.hpp"
#include "duckdb.hpp"

namespace duckdb {

class NumpyResultConversion {
public:
	NumpyResultConversion(const vector<LogicalType> &types, idx_t initial_capacity,
	                      const ClientProperties &client_properties, bool pandas = false);

	void Append(DataChunk &chunk);

	py::object ToArray(idx_t col_idx) {
		return owned_data[col_idx].ToArray();
	}
	bool ToPandas() const {
		return pandas;
	}

private:
	void Resize(idx_t new_capacity);

private:
	vector<ArrayWrapper> owned_data;
	idx_t count;
	idx_t capacity;
	bool pandas;
};

} // namespace duckdb
