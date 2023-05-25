//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/array_wrapper.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb_python/pybind11/pybind_wrapper.hpp"
#include "duckdb/common/unordered_map.hpp"
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
	explicit ArrayWrapper(const LogicalType &type);

	unique_ptr<RawArrayWrapper> data;
	unique_ptr<RawArrayWrapper> mask;
	bool requires_mask;

public:
	void Initialize(idx_t capacity);
	void Resize(idx_t new_capacity);
	void Append(idx_t current_offset, Vector &input, idx_t count);
	py::object ToArray(idx_t count) const;
	const LogicalType &Type() const;
};

class NumpyResultConversion {
public:
	NumpyResultConversion(const vector<LogicalType> &types, idx_t initial_capacity);

	void Append(DataChunk &chunk);

	const LogicalType &Type(idx_t col_idx) {
		return owned_data[col_idx].Type();
	}

	py::object ToArray(idx_t col_idx) {
		if (Type(col_idx).id() == LogicalTypeId::ENUM) {
			// first we (might) need to create the categorical type
			auto category_entry = categories_type.find(col_idx);
			if (category_entry == categories_type.end()) {
				// Equivalent to: pandas.CategoricalDtype(['a', 'b'], ordered=True)
				categories_type[col_idx] = py::module::import("pandas").attr("CategoricalDtype")(
				    categories[col_idx], py::arg("ordered") = true);
			}
			// Equivalent to: pandas.Categorical.from_codes(codes=[0, 1, 0, 1], dtype=dtype)
			return py::module::import("pandas")
			    .attr("Categorical")
			    .attr("from_codes")(ToArrayInternal(col_idx), py::arg("dtype") = category_entry->second);
		} else {
			return ToArrayInternal(col_idx);
		}
	}

private:
	void Resize(idx_t new_capacity);

	py::object ToArrayInternal(idx_t col_idx) {
		return owned_data[col_idx].ToArray(count);
	}

private:
	vector<ArrayWrapper> owned_data;
	idx_t count;
	idx_t capacity;
	// Holds the categories of Categorical/ENUM types
	unordered_map<idx_t, py::list> categories;
	// Holds the categorical type of Categorical/ENUM types
	unordered_map<idx_t, py::object> categories_type;
};

} // namespace duckdb
