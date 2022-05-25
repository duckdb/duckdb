#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/checkpoint/write_overflow_strings_to_disk.hpp"
#include "duckdb/storage/segment/uncompressed.hpp"
#include "duckdb/storage/statistics/numeric_statistics.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/column_data_checkpointer.hpp"
#include "duckdb/storage/table/column_segment.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Analyze
//===--------------------------------------------------------------------===//
struct FixedSizeAnalyzeState : public AnalyzeState {
	FixedSizeAnalyzeState() : count(0) {
	}

	idx_t count;
};

unique_ptr<AnalyzeState> FixedSizeInitAnalyze(ColumnData &col_data, PhysicalType type) {
	return make_unique<FixedSizeAnalyzeState>();
}

bool FixedSizeAnalyze(AnalyzeState &state_p, Vector &input, idx_t count) {
	auto &state = (FixedSizeAnalyzeState &)state_p;
	state.count += count;
	return true;
}

template <class T>
idx_t FixedSizeFinalAnalyze(AnalyzeState &state_p) {
	auto &state = (FixedSizeAnalyzeState &)state_p;
	return sizeof(T) * state.count;
}

//===--------------------------------------------------------------------===//
// Compress
//===--------------------------------------------------------------------===//
UncompressedCompressState::UncompressedCompressState(ColumnDataCheckpointer &checkpointer)
    : checkpointer(checkpointer) {
	CreateEmptySegment(checkpointer.GetRowGroup().start);
}

void UncompressedCompressState::CreateEmptySegment(idx_t row_start) {
	auto &db = checkpointer.GetDatabase();
	auto &type = checkpointer.GetType();
	auto compressed_segment = ColumnSegment::CreateTransientSegment(db, type, row_start);
	if (type.InternalType() == PhysicalType::VARCHAR) {
		auto &state = (UncompressedStringSegmentState &)*compressed_segment->GetSegmentState();
		state.overflow_writer = make_unique<WriteOverflowStringsToDisk>(db);
	}
	current_segment = move(compressed_segment);
}

void UncompressedCompressState::FlushSegment(idx_t segment_size) {
	auto &state = checkpointer.GetCheckpointState();
	state.FlushSegment(move(current_segment), segment_size);
}

void UncompressedCompressState::Finalize(idx_t segment_size) {
	FlushSegment(segment_size);
	current_segment.reset();
}

unique_ptr<CompressionState> UncompressedFunctions::InitCompression(ColumnDataCheckpointer &checkpointer,
                                                                    unique_ptr<AnalyzeState> state) {
	return make_unique<UncompressedCompressState>(checkpointer);
}

void UncompressedFunctions::Compress(CompressionState &state_p, Vector &data, idx_t count) {
	auto &state = (UncompressedCompressState &)state_p;
	VectorData vdata;
	data.Orrify(count, vdata);

	ColumnAppendState append_state;
	idx_t offset = 0;
	while (count > 0) {
		idx_t appended = state.current_segment->Append(append_state, vdata, offset, count);
		if (appended == count) {
			// appended everything: finished
			return;
		}
		auto next_start = state.current_segment->start + state.current_segment->count;
		// the segment is full: flush it to disk
		state.FlushSegment(state.current_segment->FinalizeAppend());

		// now create a new segment and continue appending
		state.CreateEmptySegment(next_start);
		offset += appended;
		count -= appended;
	}
}

void UncompressedFunctions::FinalizeCompress(CompressionState &state_p) {
	auto &state = (UncompressedCompressState &)state_p;
	state.Finalize(state.current_segment->FinalizeAppend());
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
struct FixedSizeScanState : public SegmentScanState {
	unique_ptr<BufferHandle> handle;
};

unique_ptr<SegmentScanState> FixedSizeInitScan(ColumnSegment &segment) {
	auto result = make_unique<FixedSizeScanState>();
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	result->handle = buffer_manager.Pin(segment.block);
	return move(result);
}

//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//
template <class T>
void FixedSizeScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
                          idx_t result_offset) {
	auto &scan_state = (FixedSizeScanState &)*state.scan_state;
	auto start = segment.GetRelativeIndex(state.row_index);

	auto data = scan_state.handle->node->buffer + segment.GetBlockOffset();
	auto source_data = data + start * sizeof(T);

	// copy the data from the base table
	result.SetVectorType(VectorType::FLAT_VECTOR);
	memcpy(FlatVector::GetData(result) + result_offset * sizeof(T), source_data, scan_count * sizeof(T));
}

template <class T>
void FixedSizeScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result) {
	auto &scan_state = (FixedSizeScanState &)*state.scan_state;
	auto start = segment.GetRelativeIndex(state.row_index);

	auto data = scan_state.handle->node->buffer + segment.GetBlockOffset();
	auto source_data = data + start * sizeof(T);

	result.SetVectorType(VectorType::FLAT_VECTOR);
	if (std::is_same<T, list_entry_t>()) {
		// list columns are modified in-place during the scans to correct the offsets
		// so we can't do a zero-copy there
		memcpy(FlatVector::GetData(result), source_data, scan_count * sizeof(T));
	} else {
		FlatVector::SetData(result, source_data);
	}
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
template <class T>
void FixedSizeFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result,
                       idx_t result_idx) {
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	auto handle = buffer_manager.Pin(segment.block);

	// first fetch the data from the base table
	auto data_ptr = handle->node->buffer + segment.GetBlockOffset() + row_id * sizeof(T);

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

template <>
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

template <class T>
idx_t FixedSizeAppend(ColumnSegment &segment, SegmentStatistics &stats, VectorData &data, idx_t offset, idx_t count) {
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	auto handle = buffer_manager.Pin(segment.block);
	D_ASSERT(segment.GetBlockOffset() == 0);

	auto target_ptr = handle->node->buffer;
	idx_t max_tuple_count = Storage::BLOCK_SIZE / sizeof(T);
	idx_t copy_count = MinValue<idx_t>(count, max_tuple_count - segment.count);

	AppendLoop<T>(stats, target_ptr, segment.count, data, offset, copy_count);
	segment.count += copy_count;
	return copy_count;
}

template <class T>
idx_t FixedSizeFinalizeAppend(ColumnSegment &segment, SegmentStatistics &stats) {
	return segment.count * sizeof(T);
}

//===--------------------------------------------------------------------===//
// Get Function
//===--------------------------------------------------------------------===//
template <class T>
CompressionFunction FixedSizeGetFunction(PhysicalType data_type) {
	return CompressionFunction(CompressionType::COMPRESSION_UNCOMPRESSED, data_type, FixedSizeInitAnalyze,
	                           FixedSizeAnalyze, FixedSizeFinalAnalyze<T>, UncompressedFunctions::InitCompression,
	                           UncompressedFunctions::Compress, UncompressedFunctions::FinalizeCompress,
	                           FixedSizeInitScan, FixedSizeScan<T>, FixedSizeScanPartial<T>, FixedSizeFetchRow<T>,
	                           UncompressedFunctions::EmptySkip, nullptr, FixedSizeAppend<T>,
	                           FixedSizeFinalizeAppend<T>, nullptr);
}

CompressionFunction FixedSizeUncompressed::GetFunction(PhysicalType data_type) {
	switch (data_type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return FixedSizeGetFunction<int8_t>(data_type);
	case PhysicalType::INT16:
		return FixedSizeGetFunction<int16_t>(data_type);
	case PhysicalType::INT32:
		return FixedSizeGetFunction<int32_t>(data_type);
	case PhysicalType::INT64:
		return FixedSizeGetFunction<int64_t>(data_type);
	case PhysicalType::UINT8:
		return FixedSizeGetFunction<uint8_t>(data_type);
	case PhysicalType::UINT16:
		return FixedSizeGetFunction<uint16_t>(data_type);
	case PhysicalType::UINT32:
		return FixedSizeGetFunction<uint32_t>(data_type);
	case PhysicalType::UINT64:
		return FixedSizeGetFunction<uint64_t>(data_type);
	case PhysicalType::INT128:
		return FixedSizeGetFunction<hugeint_t>(data_type);
	case PhysicalType::FLOAT:
		return FixedSizeGetFunction<float>(data_type);
	case PhysicalType::DOUBLE:
		return FixedSizeGetFunction<double>(data_type);
	case PhysicalType::INTERVAL:
		return FixedSizeGetFunction<interval_t>(data_type);
	case PhysicalType::LIST:
		return FixedSizeGetFunction<list_entry_t>(data_type);
	default:
		throw InternalException("Unsupported type for FixedSizeUncompressed::GetFunction");
	}
}

} // namespace duckdb
