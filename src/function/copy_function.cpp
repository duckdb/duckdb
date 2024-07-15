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

vector<LogicalType> GetTypesWithoutPartitions(const vector<LogicalType> &col_types, const vector<idx_t> &part_cols) {
	vector<LogicalType> types;
	set<idx_t> part_col_set(part_cols.begin(), part_cols.end());
	for (idx_t col_idx = 0; col_idx < col_types.size(); col_idx++) {
		if (part_col_set.find(col_idx) == part_col_set.end()) {
			types.push_back(col_types[col_idx]);
		}
	}
	return types;
}

vector<string> GetNamesWithoutPartitions(const vector<string> &col_names, const vector<column_t> &part_cols) {
	vector<string> names;
	set<idx_t> part_col_set(part_cols.begin(), part_cols.end());
	for (idx_t col_idx = 0; col_idx < col_names.size(); col_idx++) {
		if (part_col_set.find(col_idx) == part_col_set.end()) {
			names.push_back(col_names[col_idx]);
		}
	}
	return names;
}

void SetDataWithoutPartitions(DataChunk &chunk, const DataChunk &source, const vector<LogicalType> &col_types,
                              const vector<idx_t> &part_cols) {
	D_ASSERT(source.ColumnCount() == col_types.size());
	auto types = GetTypesWithoutPartitions(col_types, part_cols);
	chunk.InitializeEmpty(types);
	set<idx_t> part_col_set(part_cols.begin(), part_cols.end());
	idx_t new_col_id = 0;
	for (idx_t col_idx = 0; col_idx < source.ColumnCount(); col_idx++) {
		if (part_col_set.find(col_idx) == part_col_set.end()) {
			chunk.data[new_col_id].Reference(source.data[col_idx]);
			new_col_id++;
		}
	}
	chunk.SetCardinality(source.size());
}

} // namespace duckdb
