//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/array_wrapper.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb_python/pybind11/pybind_wrapper.hpp"
#include "duckdb_python/numpy/raw_array_wrapper.hpp"
#include "duckdb.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

struct ClientProperties;

struct NumpyAppendData {
public:
	NumpyAppendData(UnifiedVectorFormat &idata, const ClientProperties &client_properties, Vector &input)
	    : idata(idata), client_properties(client_properties), input(input) {
	}

public:
	UnifiedVectorFormat &idata;
	const ClientProperties &client_properties;
	Vector &input;

	idx_t source_offset;
	idx_t target_offset;
	data_ptr_t target_data;
	bool *target_mask;
	idx_t count;
	idx_t source_size;
	PhysicalType physical_type = PhysicalType::INVALID;
	bool pandas = false;
};

struct ArrayWrapper {
	explicit ArrayWrapper(const LogicalType &type, const ClientProperties &client_properties, bool pandas = false);

	unique_ptr<RawArrayWrapper> data;
	unique_ptr<RawArrayWrapper> mask;
	bool requires_mask;
	const ClientProperties client_properties;
	bool pandas;

public:
	void Initialize(idx_t capacity);
	void Resize(idx_t new_capacity);
	void Append(idx_t current_offset, Vector &input, idx_t source_size, idx_t source_offset = 0,
	            idx_t count = DConstants::INVALID_INDEX);
	py::object ToArray() const;
};

} // namespace duckdb
