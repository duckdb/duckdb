#include "duckdb/function/copy_function.hpp"

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/row/tuple_data_collection.hpp"

namespace duckdb {

CopyFunction::CopyFunction(const Identifier &name)
    : Function(name), plan(nullptr), copy_to_select(nullptr), copy_to_bind(nullptr), copy_options(nullptr),
      copy_to_initialize_local(nullptr), copy_to_initialize_global(nullptr), copy_to_get_written_statistics(nullptr),
      copy_to_sink(nullptr), copy_to_combine(nullptr), copy_to_finalize(nullptr), execution_mode(nullptr),
      initialize_operator(nullptr), prepare_batch(nullptr), flush_batch(nullptr), file_size_bytes(nullptr),
      desired_batch_size(nullptr), serialize(nullptr), deserialize(nullptr), copy_from_bind(nullptr) {
}

//===--------------------------------------------------------------------===//
// Copy Batch Appender
//===--------------------------------------------------------------------===//
//! How far past BATCH_SIZE_BYTES a batch may run before a chunk is cut
static constexpr idx_t BATCH_SIZE_BYTES_SLACK_DIVISOR = 8;

//! Bytes a row of this type always occupies. ComputeHeapSizes ignores fixed-width struct children, so sum them
static idx_t FixedRowBytes(const LogicalType &type) {
	if (type.InternalType() != PhysicalType::STRUCT) {
		return GetTypeIdSize(type.InternalType());
	}
	idx_t result = 0;
	for (const auto &child_type : StructType::GetChildTypes(type)) {
		result += FixedRowBytes(child_type.second);
	}
	return result;
}

static bool HasVariableRowBytes(const LogicalType &type) {
	switch (type.InternalType()) {
	case PhysicalType::VARCHAR:
	case PhysicalType::LIST:
	case PhysicalType::ARRAY:
		return true;
	case PhysicalType::STRUCT:
		for (const auto &child_type : StructType::GetChildTypes(type)) {
			if (HasVariableRowBytes(child_type.second)) {
				return true;
			}
		}
		return false;
	default:
		return false;
	}
}

CopyBatchAppender::CopyBatchAppender(const vector<LogicalType> &types, const optional_idx &batch_size_p,
                                     const optional_idx &batch_size_bytes_p)
    : batch_size(batch_size_p), batch_size_bytes(batch_size_bytes_p) {
	if (!batch_size_bytes.IsValid()) {
		// no byte limit, chunks are always appended whole
		return;
	}
	vector<column_t> variable_columns;
	for (idx_t col_idx = 0; col_idx < types.size(); col_idx++) {
		fixed_row_bytes += FixedRowBytes(types[col_idx]);
		if (HasVariableRowBytes(types[col_idx])) {
			variable_columns.push_back(col_idx);
		}
	}
	variable_row_bytes = !variable_columns.empty();
	partial = make_uniq<DataChunk>();
	partial->InitializeEmpty(types);
	if (variable_row_bytes) {
		row_bytes_state = make_uniq<TupleDataChunkState>();
		// only these columns need ToUnifiedFormat - ComputeHeapSizes returns early for the rest
		TupleDataCollection::InitializeChunkState(*row_bytes_state, types, std::move(variable_columns));
	}
}

CopyBatchAppender::~CopyBatchAppender() {
}

void CopyBatchAppender::ComputeRowBytes(DataChunk &chunk) {
	TupleDataCollection::ToUnifiedFormat(*row_bytes_state, chunk);
	TupleDataCollection::ComputeHeapSizes(*row_bytes_state, chunk, *FlatVector::IncrementalSelectionVector(),
	                                      chunk.size());
}

idx_t CopyBatchAppender::RowsThatFit(const idx_t count, const idx_t offset, const idx_t budget) const {
	const idx_t remaining = count - offset;
	if (!variable_row_bytes) {
		// every row costs the same - round up so the batch reaches the limit
		if (fixed_row_bytes == 0) {
			return remaining;
		}
		const idx_t fits = (budget + fixed_row_bytes - 1) / fixed_row_bytes;
		return MaxValue<idx_t>(MinValue(remaining, fits), 1);
	}
	// rows differ in size - stop at the first row reaching the budget, overshooting by at most one
	const auto heap_sizes = FlatVector::GetData<idx_t>(row_bytes_state->heap_sizes);
	idx_t total = 0;
	for (idx_t i = 0; i < remaining; i++) {
		total += fixed_row_bytes + heap_sizes[offset + i];
		if (total >= budget) {
			return i + 1;
		}
	}
	return remaining;
}

void CopyBatchAppender::Append(ColumnDataCollection &collection, ColumnDataAppendState &append_state, DataChunk &chunk,
                               idx_t &offset) {
	const idx_t count = chunk.size();
	if (!batch_size_bytes.IsValid()) {
		collection.Append(append_state, chunk);
		offset = count;
		return;
	}
	if (variable_row_bytes && offset == 0) {
		// the per-row sizes are reused for every slice of this chunk
		ComputeRowBytes(chunk);
	}
	const idx_t limit = batch_size_bytes.GetIndex();
	// cutting a chunk that only just crosses the limit is not worth a slice, so allow some slack
	const idx_t slack_limit = limit + limit / BATCH_SIZE_BYTES_SLACK_DIVISOR;
	idx_t append_count =
	    RowsThatFit(count, offset, slack_limit > collection_bytes ? slack_limit - collection_bytes : 0);
	if (append_count < count - offset) {
		// the chunk has to be cut, so cut it at the limit rather than at the slack
		append_count = RowsThatFit(count, offset, limit > collection_bytes ? limit - collection_bytes : 0);
	}

	if (offset == 0 && append_count == count) {
		collection.Append(append_state, chunk);
	} else {
		for (idx_t col_idx = 0; col_idx < chunk.ColumnCount(); col_idx++) {
			partial->data[col_idx].Slice(chunk.data[col_idx], offset, offset + append_count);
		}
		partial->CheckCardinality(append_count);
		collection.Append(append_state, *partial);
	}
	offset += append_count;
	collection_bytes = collection.SizeInBytes();
}

bool CopyBatchAppender::AppendUntilFull(ColumnDataCollection &collection, ColumnDataAppendState &append_state,
                                        DataChunk &chunk, idx_t &offset) {
	Append(collection, append_state, chunk, offset);
	const CopyFunctionBatchAnalyzer batch_analyzer(collection.Count(), collection_bytes, batch_size, batch_size_bytes);
	return batch_analyzer.MeetsFlushCriteria();
}

CopyOption::CopyOption() : type(LogicalType::ANY), mode(CopyOptionMode::READ_WRITE) {
}

CopyOption::CopyOption(LogicalType type_p, CopyOptionMode mode_p) : type(std::move(type_p)), mode(mode_p) {
}

vector<Identifier> GetCopyFunctionReturnNames(CopyFunctionReturnType return_type) {
	switch (return_type) {
	case CopyFunctionReturnType::CHANGED_ROWS:
		return {"Count"};
	case CopyFunctionReturnType::CHANGED_ROWS_AND_FILE_LIST:
		return {"Count", "Files"};
	case CopyFunctionReturnType::WRITTEN_FILE_STATISTICS:
		return {"filename",          "count",          "file_size_bytes", "footer_size_bytes",
		        "column_statistics", "partition_keys", "extra_info"};
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
		        LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR),
		        //! format-specific extra info (e.g. row_group_count)
		        LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARIANT())};
	default:
		throw NotImplementedException("Unknown CopyFunctionReturnType");
	}
}

CopyFunctionBatchAnalyzer::CopyFunctionBatchAnalyzer(const idx_t &current_batch_size,
                                                     const idx_t &current_batch_size_bytes,
                                                     const optional_idx &batch_size,
                                                     const optional_idx &batch_size_bytes)
    : current_batch_size(current_batch_size), current_batch_size_bytes(current_batch_size_bytes),
      batch_size(batch_size), batch_size_bytes(batch_size_bytes) {
}

CopyFunctionBatchAnalyzer::CopyFunctionBatchAnalyzer(const ColumnDataCollection &batch, const optional_idx &batch_size,
                                                     const optional_idx &batch_size_bytes)
    : CopyFunctionBatchAnalyzer(batch.Count(), batch.SizeInBytes(), batch_size, batch_size_bytes) {
}

bool CopyFunctionBatchAnalyzer::AnyBatchQualifies() const {
	return !batch_size.IsValid() && !batch_size_bytes.IsValid();
}

bool CopyFunctionBatchAnalyzer::ExceedsBatchSize() const {
	return batch_size.IsValid() && current_batch_size >= batch_size.GetIndex();
}

bool CopyFunctionBatchAnalyzer::ExceedsBatchSizeBytes() const {
	return batch_size_bytes.IsValid() && current_batch_size_bytes >= batch_size_bytes.GetIndex();
}

bool CopyFunctionBatchAnalyzer::MeetsFlushCriteria() const {
	return AnyBatchQualifies() || ExceedsBatchSize() || ExceedsBatchSizeBytes();
}

int64_t CopyFunctionBatchAnalyzer::BatchSizeVectorDiff() const {
	if (!batch_size.IsValid()) {
		return -1000000;
	}
	const auto batch_size_diff = NumericCast<int64_t>(current_batch_size) - NumericCast<int64_t>(batch_size.GetIndex());
	return (batch_size_diff + STANDARD_VECTOR_SIZE - 1) / STANDARD_VECTOR_SIZE;
}

int64_t CopyFunctionBatchAnalyzer::BatchSizeBytesVectorDiff() const {
	if (!batch_size_bytes.IsValid()) {
		return -1000000;
	}
	const auto size_bytes_diff =
	    NumericCast<int64_t>(current_batch_size_bytes) - NumericCast<int64_t>(batch_size_bytes.GetIndex());
	const auto bytes_per_tuple = NumericCast<int64_t>(current_batch_size_bytes / current_batch_size) + 1;
	return (size_bytes_diff / bytes_per_tuple + STANDARD_VECTOR_SIZE - 1) / STANDARD_VECTOR_SIZE;
}

bool CopyFunctionBatchAnalyzer::IsAcceptable() const {
	if (AnyBatchQualifies()) {
		return true;
	}

	if (BatchSizeVectorDiff() == 0) {
		// Acceptable row count, require low or acceptable byte size
		return BatchSizeBytesVectorDiff() <= 0;
	}

	if (BatchSizeBytesVectorDiff() == 0) {
		// Acceptable byte size, require low or acceptable row count
		return BatchSizeVectorDiff() <= 0;
	}

	return false;
}

CopyFunctionFlushBatchReason CopyFunctionBatchAnalyzer::ToReason() const {
	if (AnyBatchQualifies() || ExceedsBatchSize()) {
		return CopyFunctionFlushBatchReason::BATCH_SIZE;
	}

	if (ExceedsBatchSizeBytes()) {
		return CopyFunctionFlushBatchReason::BATCH_SIZE_BYTES;
	}

	return CopyFunctionFlushBatchReason::FORCED_FLUSH;
}

} // namespace duckdb
