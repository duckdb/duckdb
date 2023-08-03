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

struct RegisteredArray {
	explicit RegisteredArray(py::array numpy_array) : numpy_array(std::move(numpy_array)) {
	}
	py::array numpy_array;
};

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

struct ArrayWrapper {
	explicit ArrayWrapper(const LogicalType &type, const ClientProperties &client_properties);

	unique_ptr<RawArrayWrapper> data;
	unique_ptr<RawArrayWrapper> mask;
	bool requires_mask;
	const ClientProperties client_properties;

public:
	void Initialize(idx_t capacity);
	void Resize(idx_t new_capacity);
	void Append(idx_t current_offset, Vector &input, idx_t count);
	py::object ToArray(idx_t count) const;
};

class NumpyResultConversion {
public:
	NumpyResultConversion(const vector<LogicalType> &types, idx_t initial_capacity,
	                      const ClientProperties &client_properties);

	void Append(DataChunk &chunk);

	py::object ToArray(idx_t col_idx) {
		return owned_data[col_idx].ToArray(count);
	}

private:
	void Resize(idx_t new_capacity);

private:
	vector<ArrayWrapper> owned_data;
	idx_t count;
	idx_t capacity;
};

} // namespace duckdb
