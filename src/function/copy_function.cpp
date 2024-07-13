#include "duckdb/function/copy_function.hpp"
#include <numeric>

namespace duckdb {

vector<string> GetCopyFunctionReturnNames(CopyFunctionReturnType return_type) {
	switch (return_type) {
	case CopyFunctionReturnType::CHANGED_ROWS:
		return {"Count"};
	case CopyFunctionReturnType::CHANGED_ROWS_AND_FILE_LIST:
		return {"Count", "Files"};
	default:
		throw NotImplementedException("Unknown CopyFunctionReturnType");
	}
}

vector<LogicalType> GetCopyFunctionReturnLogicalTypes(CopyFunctionReturnType return_type) {
	switch (return_type) {
	case CopyFunctionReturnType::CHANGED_ROWS:
		return {LogicalType::BIGINT};
	case CopyFunctionReturnType::CHANGED_ROWS_AND_FILE_LIST:
		return {LogicalType::BIGINT, LogicalType::LIST(LogicalType::VARCHAR)};
	default:
		throw NotImplementedException("Unknown CopyFunctionReturnType");
	}
}

vector<idx_t> GetColumnsToCopy(vector<LogicalType> &types, vector<idx_t> &excluded_columns, bool no_partition_columns) {
	vector<column_t> result;

	if (!no_partition_columns) {
		result.resize(types.size(), 0);
		std::iota(std::begin(result), std::end(result), 0);
		return result;
	}
	set<column_t> excluded_column_set(excluded_columns.begin(), excluded_columns.end());
	for (idx_t i = 0; i < types.size(); i++) {
		if (excluded_column_set.find(i) == excluded_column_set.end()) {
			result.emplace_back(i);
		}
	}
	return result;
}

vector<LogicalType> GetTypesToCopy(const vector<LogicalType> &col_types, const vector<column_t> &cols_to_copy) {
	vector<LogicalType> types;
	for (auto col_idx : cols_to_copy) {
		types.push_back(col_types[col_idx]);
	}
	return types;
}

vector<string> GetNamesToCopy(const vector<string> &col_names, const vector<column_t> &cols_to_copy) {
	vector<string> names;
	for (auto col_idx : cols_to_copy) {
		names.push_back(col_names[col_idx]);
	}
	return names;
}

void SetDataToCopy(DataChunk &chunk, DataChunk &source, const vector<idx_t> &cols_to_copy,
                   const vector<LogicalType> &types) {
	D_ASSERT(cols_to_copy.size() == types.size());
	chunk.InitializeEmpty(types);
	for (idx_t i = 0; i < cols_to_copy.size(); i++) {
		chunk.data[i].Reference(source.data[cols_to_copy[i]]);
	}
	chunk.SetCardinality(source.size());
}

} // namespace duckdb
