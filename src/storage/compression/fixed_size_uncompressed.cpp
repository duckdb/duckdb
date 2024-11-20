#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/checkpoint/write_overflow_strings_to_disk.hpp"
#include "duckdb/storage/segment/uncompressed.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/column_data_checkpointer.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/storage/table/scan_state.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Analyze
//===--------------------------------------------------------------------===//
struct FixedSizeAnalyzeState : public AnalyzeState {
	explicit FixedSizeAnalyzeState(const CompressionInfo &info) : AnalyzeState(info), count(0) {
	}

	idx_t count;
};

unique_ptr<AnalyzeState> FixedSizeInitAnalyze(ColumnData &col_data, PhysicalType type) {
	CompressionInfo info(col_data.GetBlockManager().GetBlockSize());
	return make_uniq<FixedSizeAnalyzeState>(info);
}

bool FixedSizeAnalyze(AnalyzeState &state_p, Vector &input, idx_t count) {
	auto &state = state_p.Cast<FixedSizeAnalyzeState>();
	state.count += count;
	return true;
}

template <class T>
idx_t FixedSizeFinalAnalyze(AnalyzeState &state_p) {
	auto &state = state_p.template Cast<FixedSizeAnalyzeState>();
	return sizeof(T) * state.count;
}

//===--------------------------------------------------------------------===//
// Compress
//===--------------------------------------------------------------------===//
struct UncompressedCompressState : public CompressionState {
	UncompressedCompressState(ColumnDataCheckpointer &checkpointer, const CompressionInfo &info);

	ColumnDataCheckpointer &checkpointer;
	unique_ptr<ColumnSegment> current_segment;
	ColumnAppendState append_state;

	virtual void CreateEmptySegment(idx_t row_start);
	void FlushSegment(idx_t segment_size);
	void Finalize(idx_t segment_size);
};

UncompressedCompressState::UncompressedCompressState(ColumnDataCheckpointer &checkpointer, const CompressionInfo &info)
    : CompressionState(info), checkpointer(checkpointer) {
	UncompressedCompressState::CreateEmptySegment(checkpointer.GetRowGroup().start);
}

void UncompressedCompressState::CreateEmptySegment(idx_t row_start) {
	auto &db = checkpointer.GetDatabase();
	auto &type = checkpointer.GetType();

	auto compressed_segment =
	    ColumnSegment::CreateTransientSegment(db, type, row_start, info.GetBlockSize(), info.GetBlockSize());
	if (type.InternalType() == PhysicalType::VARCHAR) {
		auto &state = compressed_segment->GetSegmentState()->Cast<UncompressedStringSegmentState>();
		state.overflow_writer =
		    make_uniq<WriteOverflowStringsToDisk>(checkpointer.GetCheckpointState().GetPartialBlockManager());
	}
	current_segment = std::move(compressed_segment);
	current_segment->InitializeAppend(append_state);
}

void UncompressedCompressState::FlushSegment(idx_t segment_size) {
	auto &state = checkpointer.GetCheckpointState();
	if (current_segment->type.InternalType() == PhysicalType::VARCHAR) {
		auto &segment_state = current_segment->GetSegmentState()->Cast<UncompressedStringSegmentState>();
		segment_state.overflow_writer->Flush();
		segment_state.overflow_writer.reset();
	}
	append_state.child_appends.clear();
	append_state.append_state.reset();
	append_state.lock.reset();
	state.FlushSegmentInternal(std::move(current_segment), segment_size);
}

void UncompressedCompressState::Finalize(idx_t segment_size) {
	FlushSegment(segment_size);
	current_segment.reset();
}

unique_ptr<CompressionState> UncompressedFunctions::InitCompression(ColumnDataCheckpointer &checkpointer,
                                                                    unique_ptr<AnalyzeState> state) {
	return make_uniq<UncompressedCompressState>(checkpointer, state->info);
}

void UncompressedFunctions::Compress(CompressionState &state_p, Vector &data, idx_t count) {
	auto &state = state_p.Cast<UncompressedCompressState>();
	UnifiedVectorFormat vdata;
	data.ToUnifiedFormat(count, vdata);

	idx_t offset = 0;
	while (count > 0) {
		idx_t appended = state.current_segment->Append(state.append_state, vdata, offset, count);
		if (appended == count) {
			// appended everything: finished
			return;
		}
		auto next_start = state.current_segment->start + state.current_segment->count;
		// the segment is full: flush it to disk
		state.FlushSegment(state.current_segment->FinalizeAppend(state.append_state));

		// now create a new segment and continue appending
		state.CreateEmptySegment(next_start);
		offset += appended;
		count -= appended;
	}
}

void UncompressedFunctions::FinalizeCompress(CompressionState &state_p) {
	auto &state = state_p.Cast<UncompressedCompressState>();
	state.Finalize(state.current_segment->FinalizeAppend(state.append_state));
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
struct FixedSizeScanState : public SegmentScanState {
	BufferHandle handle;
};

unique_ptr<SegmentScanState> FixedSizeInitScan(ColumnSegment &segment) {
	auto result = make_uniq<FixedSizeScanState>();
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	result->handle = buffer_manager.Pin(segment.block);
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//
template <class T>
void FixedSizeScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
                          idx_t result_offset) {
	auto &scan_state = state.scan_state->Cast<FixedSizeScanState>();
	auto start = segment.GetRelativeIndex(state.row_index);

	auto data = scan_state.handle.Ptr() + segment.GetBlockOffset();
	auto source_data = data + start * sizeof(T);

	// copy the data from the base table
	result.SetVectorType(VectorType::FLAT_VECTOR);
	memcpy(FlatVector::GetData(result) + result_offset * sizeof(T), source_data, scan_count * sizeof(T));
}

template <class T>
void FixedSizeScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result) {
	auto &scan_state = state.scan_state->template Cast<FixedSizeScanState>();
	auto start = segment.GetRelativeIndex(state.row_index);

	auto data = scan_state.handle.Ptr() + segment.GetBlockOffset();
	auto source_data = data + start * sizeof(T);

	result.SetVectorType(VectorType::FLAT_VECTOR);
	FlatVector::SetData(result, source_data);
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
	auto data_ptr = handle.Ptr() + segment.GetBlockOffset() + NumericCast<idx_t>(row_id) * sizeof(T);

	memcpy(FlatVector::GetData(result) + result_idx * sizeof(T), data_ptr, sizeof(T));
}

//===--------------------------------------------------------------------===//
// Append
//===--------------------------------------------------------------------===//
static unique_ptr<CompressionAppendState> FixedSizeInitAppend(ColumnSegment &segment) {
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	auto handle = buffer_manager.Pin(segment.block);
	return make_uniq<CompressionAppendState>(std::move(handle));
}

struct StandardFixedSizeAppend {
	template <class T>
	static void Append(SegmentStatistics &stats, data_ptr_t target, idx_t target_offset, UnifiedVectorFormat &adata,
	                   idx_t offset, idx_t count) {
		auto sdata = UnifiedVectorFormat::GetData<T>(adata);
		auto tdata = reinterpret_cast<T *>(target);
		if (!adata.validity.AllValid()) {
			for (idx_t i = 0; i < count; i++) {
				auto source_idx = adata.sel->get_index(offset + i);
				auto target_idx = target_offset + i;
				bool is_null = !adata.validity.RowIsValid(source_idx);
				if (!is_null) {
					stats.statistics.UpdateNumericStats<T>(sdata[source_idx]);
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
				stats.statistics.UpdateNumericStats<T>(sdata[source_idx]);
				tdata[target_idx] = sdata[source_idx];
			}
		}
	}
};

struct ListFixedSizeAppend {
	template <class T>
	static void Append(SegmentStatistics &stats, data_ptr_t target, idx_t target_offset, UnifiedVectorFormat &adata,
	                   idx_t offset, idx_t count) {
		auto sdata = UnifiedVectorFormat::GetData<uint64_t>(adata);
		auto tdata = reinterpret_cast<uint64_t *>(target);
		for (idx_t i = 0; i < count; i++) {
			auto source_idx = adata.sel->get_index(offset + i);
			auto target_idx = target_offset + i;
			tdata[target_idx] = sdata[source_idx];
		}
	}
};

template <class T, class OP>
idx_t FixedSizeAppend(CompressionAppendState &append_state, ColumnSegment &segment, SegmentStatistics &stats,
                      UnifiedVectorFormat &data, idx_t offset, idx_t count) {
	D_ASSERT(segment.GetBlockOffset() == 0);

	auto target_ptr = append_state.handle.Ptr();
	idx_t max_tuple_count = segment.SegmentSize() / sizeof(T);
	idx_t copy_count = MinValue<idx_t>(count, max_tuple_count - segment.count);

	OP::template Append<T>(stats, target_ptr, segment.count, data, offset, copy_count);
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
template <class T, class APPENDER = StandardFixedSizeAppend>
CompressionFunction FixedSizeGetFunction(PhysicalType data_type) {
	return CompressionFunction(CompressionType::COMPRESSION_UNCOMPRESSED, data_type, FixedSizeInitAnalyze,
	                           FixedSizeAnalyze, FixedSizeFinalAnalyze<T>, UncompressedFunctions::InitCompression,
	                           UncompressedFunctions::Compress, UncompressedFunctions::FinalizeCompress,
	                           FixedSizeInitScan, FixedSizeScan<T>, FixedSizeScanPartial<T>, FixedSizeFetchRow<T>,
	                           UncompressedFunctions::EmptySkip, nullptr, FixedSizeInitAppend,
	                           FixedSizeAppend<T, APPENDER>, FixedSizeFinalizeAppend<T>);
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
	case PhysicalType::UINT128:
		return FixedSizeGetFunction<uhugeint_t>(data_type);
	case PhysicalType::FLOAT:
		return FixedSizeGetFunction<float>(data_type);
	case PhysicalType::DOUBLE:
		return FixedSizeGetFunction<double>(data_type);
	case PhysicalType::INTERVAL:
		return FixedSizeGetFunction<interval_t>(data_type);
	case PhysicalType::LIST:
		return FixedSizeGetFunction<uint64_t, ListFixedSizeAppend>(data_type);
	default:
		throw InternalException("Unsupported type for FixedSizeUncompressed::GetFunction");
	}
}

} // namespace duckdb
