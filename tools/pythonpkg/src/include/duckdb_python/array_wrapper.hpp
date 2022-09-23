//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/array_wrapper.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb_python/pybind_wrapper.hpp"
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
	void Initialize(idx_t capacity);
	void Resize(idx_t new_capacity);
	void Append(idx_t current_offset, Vector &input, idx_t count);
};
struct ArrayWrapper {
	explicit ArrayWrapper(const LogicalType &type);

	unique_ptr<RawArrayWrapper> data;
	unique_ptr<RawArrayWrapper> mask;
	bool requires_mask;

public:
	void Initialize(idx_t capacity);
	void Resize(idx_t new_capacity);
	void Append(idx_t current_offset, Vector &input, idx_t count);
	py::object ToArray(idx_t count) const;
};

class NumpyResultConversion {
public:
	NumpyResultConversion(vector<LogicalType> &types, idx_t initial_capacity);

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
