#include "duckdb/function/copy_function.hpp"

namespace duckdb {

CopyFunction::CopyFunction(const string &name)
    : Function(name), plan(nullptr), copy_to_select(nullptr), copy_to_bind(nullptr), copy_options(nullptr),
      copy_to_initialize_local(nullptr), copy_to_initialize_global(nullptr), copy_to_get_written_statistics(nullptr),
      copy_to_sink(nullptr), copy_to_combine(nullptr), copy_to_finalize(nullptr), execution_mode(nullptr),
      initialize_operator(nullptr), prepare_batch(nullptr), flush_batch(nullptr), desired_batch_size(nullptr),
      rotate_files(nullptr), rotate_next_file(nullptr), serialize(nullptr), deserialize(nullptr),
      copy_from_bind(nullptr) {
}

CopyOption::CopyOption() : type(LogicalType::ANY), mode(CopyOptionMode::READ_WRITE) {
}

CopyOption::CopyOption(LogicalType type_p, CopyOptionMode mode_p) : type(std::move(type_p)), mode(mode_p) {
}

vector<string> GetCopyFunctionReturnNames(CopyFunctionReturnType return_type) {
	switch (return_type) {
	case CopyFunctionReturnType::CHANGED_ROWS:
		return {"Count"};
	case CopyFunctionReturnType::CHANGED_ROWS_AND_FILE_LIST:
		return {"Count", "Files"};
	case CopyFunctionReturnType::WRITTEN_FILE_STATISTICS:
		return {"filename", "count", "file_size_bytes", "footer_size_bytes", "column_statistics", "partition_keys"};
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
		        LogicalType::MAP(LogicalType::VARCHAR, LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR)),
		        LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR)};
	default:
		throw NotImplementedException("Unknown CopyFunctionReturnType");
	}
}

} // namespace duckdb
