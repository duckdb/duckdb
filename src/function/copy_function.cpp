#include "duckdb/function/copy_function.hpp"

namespace duckdb {

vector<string> GetCopyFunctionReturnNames(CopyFunctionReturnType return_type) {
	switch (return_type) {
	case CopyFunctionReturnType::CHANGED_ROWS:
		return {"Count"};
	case CopyFunctionReturnType::CHANGED_ROWS_AND_FILE_LIST:
		return {"Count", "Files"};
	case CopyFunctionReturnType::WRITTEN_FILE_STATISTICS:
		return {"filename", "count", "file_size_bytes", "footer_offset", "footer_size", "column_statistics"};
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
	case CopyFunctionReturnType::WRITTEN_FILE_STATISTICS:
		return {LogicalType::VARCHAR,
		        LogicalType::UBIGINT,
		        LogicalType::UBIGINT,
		        LogicalType::UBIGINT,
		        LogicalType::UBIGINT,
		        LogicalType::MAP(LogicalType::VARCHAR, LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR))};
	default:
		throw NotImplementedException("Unknown CopyFunctionReturnType");
	}
}

} // namespace duckdb
