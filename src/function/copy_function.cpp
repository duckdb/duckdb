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

} // namespace duckdb
