#include "duckdb/storage/segment/numeric_uncompressed.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/transaction/update_info.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/common/vector_size.hpp"
#include "duckdb/storage/statistics/numeric_statistics.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/storage/segment/compressed_segment.hpp"
#include "duckdb/storage/table/column_data_checkpointer.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Analyze
//===--------------------------------------------------------------------===//
struct NumericAnalyzeState : public AnalyzeState {
	NumericAnalyzeState() : count(0) {}

	idx_t count;
};

unique_ptr<AnalyzeState> NumericInitAnalyze(ColumnData &col_data, PhysicalType type) {
	return make_unique<NumericAnalyzeState>();
}

bool NumericAnalyze(AnalyzeState &state_p, Vector &input, idx_t count) {
	auto &state = (NumericAnalyzeState &) state_p;
	state.count += count;
	return true;
}

template<class T>
idx_t NumericFinalAnalyze(AnalyzeState &state_p) {
	auto &state = (NumericAnalyzeState &) state_p;
	return sizeof(T) * state.count;
}

//===--------------------------------------------------------------------===//
// Compress
//===--------------------------------------------------------------------===//
struct NumericCompressState : public CompressionState {
	NumericCompressState(ColumnDataCheckpointer &checkpointer) :
		checkpointer(checkpointer) {
		auto &db = checkpointer.GetDatabase();
		auto &config = DBConfig::GetConfig(db);
		auto &type = checkpointer.GetType();
		function = config.GetCompressionFunction(CompressionType::COMPRESSION_UNCOMPRESSED, type.InternalType());
		CreateEmptySegment(checkpointer.GetRowGroup().start);
	}

	void CreateEmptySegment(idx_t row_start) {
		auto &db = checkpointer.GetDatabase();
		auto &type = checkpointer.GetType();
		current_segment = make_unique<CompressedSegment>(db, type.InternalType(), row_start, function);
		segment_stats = make_unique<SegmentStatistics>(type);
	}

	void FlushSegment() {
		auto &state = checkpointer.GetCheckpointState();
		state.FlushSegment(*current_segment, move(segment_stats->statistics));
	}

	void Finalize() {
		FlushSegment();
		current_segment.reset();
		segment_stats.reset();
	}

	ColumnDataCheckpointer &checkpointer;
	CompressionFunction *function;
	unique_ptr<BaseSegment> current_segment;
	unique_ptr<SegmentStatistics> segment_stats;
};

template<class T>
unique_ptr<CompressionState> NumericInitCompression(ColumnDataCheckpointer &checkpointer, unique_ptr<AnalyzeState> state) {
	return make_unique<NumericCompressState>(checkpointer);
}

template<class T>
void NumericCompress(CompressionState& state_p, Vector &data, idx_t count) {
	auto &state = (NumericCompressState &) state_p;
	VectorData vdata;
	data.Orrify(count, vdata);

	idx_t offset = 0;
	while (count > 0) {
		idx_t appended = state.current_segment->Append(*state.segment_stats, vdata, offset, count);
		if (appended == count) {
			// appended everything: finished
			return;
		}
		auto next_start = state.current_segment->row_start + state.current_segment->tuple_count;
		// the segment is full: flush it to disk
		state.FlushSegment();

		// now create a new segment and continue appending
		state.CreateEmptySegment(next_start);
		offset += appended;
		count -= appended;
	}
}

template<class T>
void NumericFinalizeCompress(CompressionState& state_p) {
	auto &state = (NumericCompressState &) state_p;
	state.Finalize();
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
struct NumericScanState : public SegmentScanState {
	unique_ptr<BufferHandle> handle;
};

unique_ptr<SegmentScanState> NumericInitScan(CompressedSegment &segment) {
	auto result = make_unique<NumericScanState>();
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	result->handle = buffer_manager.Pin(segment.block);
	return move(result);
}

//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//
template<class T>
void NumericScanPartial(CompressedSegment &segment, ColumnScanState &state, idx_t start, idx_t scan_count, Vector &result, idx_t result_offset) {
	auto &scan_state = (NumericScanState &) *state.scan_state;
	D_ASSERT(start <= segment.tuple_count);
	D_ASSERT(start + scan_count <= segment.tuple_count);

	auto data = scan_state.handle->node->buffer;
	auto source_data = data + start * sizeof(T);

	// copy the data from the base table
	result.SetVectorType(VectorType::FLAT_VECTOR);
	memcpy(FlatVector::GetData(result) + result_offset * sizeof(T), source_data, scan_count * sizeof(T));
}

template<class T>
void NumericScan(CompressedSegment &segment, ColumnScanState &state, idx_t start, idx_t scan_count, Vector &result) {
	// FIXME: we should be able to do a zero-copy here
	NumericScanPartial<T>(segment, state, start, scan_count, result, 0);
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
template<class T>
void NumericFetchRow(CompressedSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx) {
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	auto handle = buffer_manager.Pin(segment.block);

	// first fetch the data from the base table
	auto data_ptr = handle->node->buffer + row_id * sizeof(T);

	memcpy(FlatVector::GetData(result) + result_idx * sizeof(T), data_ptr, sizeof(T));
}

//===--------------------------------------------------------------------===//
// Append
//===--------------------------------------------------------------------===//
template <class T>
static void AppendLoop(SegmentStatistics &stats, data_ptr_t target, idx_t target_offset, VectorData &adata,
                       idx_t offset, idx_t count) {
	auto sdata = (T *)adata.data;
	auto tdata = (T *)target;
	if (!adata.validity.AllValid()) {
		for (idx_t i = 0; i < count; i++) {
			auto source_idx = adata.sel->get_index(offset + i);
			auto target_idx = target_offset + i;
			bool is_null = !adata.validity.RowIsValid(source_idx);
			if (!is_null) {
				NumericStatistics::Update<T>(stats, sdata[source_idx]);
				tdata[target_idx] = sdata[source_idx];
			} else {
				// we insert a NullValue<T> in the null gap for debuggability
				// this value should never be used or read anywhere
				tdata[target_idx] = NullValue<T>();
			}
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			auto source_idx = adata.sel->get_index(offset + i);
			auto target_idx = target_offset + i;
			NumericStatistics::Update<T>(stats, sdata[source_idx]);
			tdata[target_idx] = sdata[source_idx];
		}
	}
}

template<>
void AppendLoop<list_entry_t>(SegmentStatistics &stats, data_ptr_t target, idx_t target_offset, VectorData &adata,
                           idx_t offset, idx_t count) {
	auto sdata = (list_entry_t *)adata.data;
	auto tdata = (list_entry_t *)target;
	for (idx_t i = 0; i < count; i++) {
		auto source_idx = adata.sel->get_index(offset + i);
		auto target_idx = target_offset + i;
		tdata[target_idx] = sdata[source_idx];
	}
}

template<class T>
idx_t NumericAppend(CompressedSegment &segment, SegmentStatistics &stats, VectorData &data, idx_t offset, idx_t count) {
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	auto handle = buffer_manager.Pin(segment.block);

	auto target_ptr = handle->node->buffer;
	idx_t max_tuple_count = Storage::BLOCK_SIZE / sizeof(T);
	idx_t copy_count = MinValue<idx_t>(count, max_tuple_count - segment.tuple_count);

	AppendLoop<T>(stats, target_ptr, segment.tuple_count, data, offset, copy_count);
	segment.tuple_count += copy_count;
	return copy_count;
}

//===--------------------------------------------------------------------===//
// Get Function
//===--------------------------------------------------------------------===//
template<class T>
CompressionFunction NumericGetFunction(PhysicalType data_type) {
	return CompressionFunction(
		CompressionType::COMPRESSION_UNCOMPRESSED,
		data_type,
		NumericInitAnalyze,
		NumericAnalyze,
		NumericFinalAnalyze<T>,
		NumericInitCompression<T>,
		NumericCompress<T>,
		NumericFinalizeCompress<T>,
		NumericInitScan,
		NumericScan<T>,
		NumericScanPartial<T>,
		NumericFetchRow<T>,
		NumericAppend<T>
	);
}


CompressionFunction NumericUncompressed::GetFunction(PhysicalType data_type) {
	switch(data_type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return NumericGetFunction<int8_t>(data_type);
	case PhysicalType::INT16:
		return NumericGetFunction<int16_t>(data_type);
	case PhysicalType::INT32:
		return NumericGetFunction<int32_t>(data_type);
	case PhysicalType::INT64:
		return NumericGetFunction<int64_t>(data_type);
	case PhysicalType::UINT8:
		return NumericGetFunction<uint8_t>(data_type);
	case PhysicalType::UINT16:
		return NumericGetFunction<uint16_t>(data_type);
	case PhysicalType::UINT32:
		return NumericGetFunction<uint32_t>(data_type);
	case PhysicalType::UINT64:
		return NumericGetFunction<uint64_t>(data_type);
	case PhysicalType::INT128:
		return NumericGetFunction<hugeint_t>(data_type);
	case PhysicalType::FLOAT:
		return NumericGetFunction<float>(data_type);
	case PhysicalType::DOUBLE:
		return NumericGetFunction<double>(data_type);
	case PhysicalType::INTERVAL:
		return NumericGetFunction<interval_t>(data_type);
	case PhysicalType::LIST:
		return NumericGetFunction<list_entry_t>(data_type);
	default:
		throw InternalException("Unsupported type for NumericUncompressed::GetFunction");
	}
}

} // namespace duckdb
