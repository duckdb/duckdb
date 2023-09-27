#include "duckdb_python/numpy/array_wrapper.hpp"
#include "duckdb_python/numpy/numpy_result_conversion.hpp"

namespace duckdb {

NumpyResultConversion::NumpyResultConversion(const vector<LogicalType> &types, idx_t initial_capacity,
                                             const ClientProperties &client_properties, bool pandas)
    : count(0), capacity(0) {
	owned_data.reserve(types.size());
	for (auto &type : types) {
		owned_data.emplace_back(type, client_properties, pandas);
	}
	Resize(initial_capacity);
}

void NumpyResultConversion::Resize(idx_t new_capacity) {
	if (capacity == 0) {
		for (auto &data : owned_data) {
			data.Initialize(new_capacity);
		}
	} else {
		for (auto &data : owned_data) {
			data.Resize(new_capacity);
		}
	}
	capacity = new_capacity;
}

void NumpyResultConversion::Append(DataChunk &chunk, idx_t offset) {
	D_ASSERT(offset < capacity || (offset == 0 && chunk.size() == 0));
	auto chunk_types = chunk.GetTypes();
	for (idx_t col_idx = 0; col_idx < owned_data.size(); col_idx++) {
		owned_data[col_idx].Append(offset, chunk.data[col_idx], chunk.size());
	}
}

void NumpyResultConversion::SetCardinality(idx_t cardinality) {
	count = cardinality;
}

py::list &NumpyResultConversion::InsertCategory(idx_t col_idx) {
	auto &type = Type(col_idx);
	D_ASSERT(type.id() == LogicalTypeId::ENUM);
	D_ASSERT(categories.find(col_idx) == categories.end());

	auto &categories_list = EnumType::GetValuesInsertOrder(type);
	auto categories_size = EnumType::GetSize(type);
	auto result = categories.insert(std::make_pair(col_idx, py::list()));
	D_ASSERT(result.second);
	auto &list = result.first->second;
	for (idx_t i = 0; i < categories_size; i++) {
		list.append(py::cast(categories_list.GetValue(i).ToString()));
	}
	return list;
}

void NumpyResultConversion::Append(DataChunk &chunk) {
	if (count + chunk.size() > capacity) {
		py::gil_scoped_acquire gil;
		Resize(capacity * 2);
	}
	Append(chunk, count);
	count += chunk.size();
}

} // namespace duckdb
