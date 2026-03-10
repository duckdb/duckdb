#include "duckdb/function/copy_function.hpp"

#include "duckdb/common/types/column/column_data_collection.hpp"

namespace duckdb {

CopyFunction::CopyFunction(const string &name)
    : Function(name), plan(nullptr), copy_to_select(nullptr), copy_to_bind(nullptr), copy_options(nullptr),
      copy_to_initialize_local(nullptr), copy_to_initialize_global(nullptr), copy_to_get_written_statistics(nullptr),
      copy_to_sink(nullptr), copy_to_combine(nullptr), copy_to_finalize(nullptr), execution_mode(nullptr),
      initialize_operator(nullptr), prepare_batch(nullptr), flush_batch(nullptr), default_batch_size(nullptr),
      default_batch_size_bytes(nullptr), file_size_bytes(nullptr), desired_batch_size(nullptr), serialize(nullptr),
      deserialize(nullptr), copy_from_bind(nullptr) {
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
		return {//! filename
		        LogicalType::VARCHAR,
		        //! count
		        LogicalType::UBIGINT,
		        //! file size bytes
		        LogicalType::UBIGINT,
		        //! footer size bytes
		        LogicalType::UBIGINT,
		        //! column_path (potentially nested) -> map(stats_type -> value)
		        LogicalType::MAP(LogicalType::VARCHAR, LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR)),
		        //! partition key -> value
		        LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR)};
	default:
		throw NotImplementedException("Unknown CopyFunctionReturnType");
	}
}

bool CopyFunctionMustFlushBatch(const idx_t &current_batch_size, const idx_t current_batch_size_bytes,
                                const optional_idx &batch_size, const optional_idx &batch_size_bytes) {
	const auto exceeds_count = current_batch_size >= batch_size.GetIndex();
	const auto exceeds_size = batch_size_bytes.IsValid() && current_batch_size_bytes >= batch_size_bytes.GetIndex();
	return exceeds_count || exceeds_size;
}

bool CopyFunctionMustFlushBatch(const ColumnDataCollection &batch, const optional_idx &batch_size,
                                const optional_idx &batch_size_bytes) {
	return CopyFunctionMustFlushBatch(batch.Count(), batch.SizeInBytes(), batch_size, batch_size_bytes);
}

} // namespace duckdb
