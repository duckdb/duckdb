//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/numpy/array_wrapper.hpp
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
	explicit RawArrayWrapper(py::array array, const LogicalType &type);
	~RawArrayWrapper() {
		D_ASSERT(py::gil_check());
	}

	py::array array;
	data_ptr_t data;
	LogicalType type;
	idx_t type_width;

public:
	static string DuckDBToNumpyDtype(const LogicalType &type);
	static idx_t DuckDBToNumpyTypeWidth(const LogicalType &type);
	void Initialize(idx_t capacity);
	void Resize(idx_t new_capacity);
	void Append(idx_t current_offset, Vector &input, idx_t count);
};

struct ArrayWrapper {
	explicit ArrayWrapper(const LogicalType &type);
	explicit ArrayWrapper(unique_ptr<RawArrayWrapper> data, unique_ptr<RawArrayWrapper> mask, bool requires_mask);

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

public:
	py::list &InsertCategory(idx_t col_idx);
	void Append(DataChunk &chunk, idx_t offset);
	void Append(DataChunk &chunk);
	void SetCardinality(idx_t cardinality);

	vector<LogicalType> Types() const {
		vector<LogicalType> types;
		types.reserve(owned_data.size());
		for (auto &data : owned_data) {
			types.push_back(data.Type());
		}
		return types;
	}

	idx_t Count() const {
		return count;
	}
	idx_t Capacity() const {
		return capacity;
	}

	void Reset() {
		D_ASSERT(py::gil_check());
		owned_data.clear();
		count = 0;
		capacity = 0;
	}

	const LogicalType &Type(idx_t col_idx) {
		return owned_data[col_idx].Type();
	}

	py::list &GetCategory(idx_t col_idx) {
		D_ASSERT(Type(col_idx).id() == LogicalTypeId::ENUM);
		auto category_entry = categories.find(col_idx);
		if (category_entry != categories.end()) {
			return category_entry->second;
		}
		return InsertCategory(col_idx);
	}

	py::object ToArray(idx_t col_idx) {
		D_ASSERT(py::gil_check());
		if (Type(col_idx).id() == LogicalTypeId::ENUM) {
			py::list &category = GetCategory(col_idx);
			// first we (might) need to create the categorical type
			auto category_entry = categories_type.find(col_idx);
			if (category_entry == categories_type.end()) {
				// Equivalent to: pandas.CategoricalDtype(['a', 'b'], ordered=True)
				auto result = categories_type.emplace(std::make_pair(
				    col_idx,
				    py::module::import("pandas").attr("CategoricalDtype")(category, py::arg("ordered") = true)));
				D_ASSERT(result.second);
				category_entry = result.first;
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
