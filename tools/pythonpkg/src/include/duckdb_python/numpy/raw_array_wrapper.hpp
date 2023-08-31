//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/array_wrapper.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb_python/pybind11/pybind_wrapper.hpp"
#include "duckdb.hpp"

namespace duckdb {

struct RawArrayWrapper {

	explicit RawArrayWrapper(const LogicalType &type);

	py::array array;
	data_ptr_t data;
	LogicalType type;
	idx_t type_width;
	idx_t count;

public:
	static string DuckDBToNumpyDtype(const LogicalType &type);
	void Initialize(idx_t capacity);
	void Resize(idx_t new_capacity);
	void Append(idx_t current_offset, Vector &input, idx_t count);
};

} // namespace duckdb
