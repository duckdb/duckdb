#include "duckdb/storage/compression/roaring.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/function/compression/compression.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/table/column_data_checkpointer.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/storage/table/scan_state.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Analyze
//===--------------------------------------------------------------------===//
template <class T>
struct RoaringAnalyzeState : public AnalyzeState {
	explicit RoaringAnalyzeState(const CompressionInfo &info) : AnalyzeState(info) {};
};

template <class T>
unique_ptr<AnalyzeState> RoaringInitAnalyze(ColumnData &col_data, PhysicalType type) {
	auto &config = DBConfig::GetConfig(col_data.GetDatabase());

	CompressionInfo info(col_data.GetBlockManager().GetBlockSize());
	auto state = make_uniq<RoaringAnalyzeState<T>>(info);

	return std::move(state);
}

template <class T>
bool RoaringAnalyze(AnalyzeState &state, Vector &input, idx_t count) {
	auto &analyze_state = state.Cast<RoaringAnalyzeState<T>>();

	// TODO: implement
	return true;
}

template <class T>
idx_t RoaringFinalAnalyze(AnalyzeState &state) {
	auto &roaring_state = state.Cast<RoaringAnalyzeState<T>>();
	// TODO: implement
	return DConstants::INVALID_INDEX;
}

//===--------------------------------------------------------------------===//
// Compress
//===--------------------------------------------------------------------===//
template <class T>
struct RoaringCompressState : public CompressionState {
public:
	explicit RoaringCompressState(ColumnDataCheckpointer &checkpointer, const CompressionInfo &info)
	    : CompressionState(info), checkpointer(checkpointer),
	      function(checkpointer.GetCompressionFunction(CompressionType::COMPRESSION_ROARING)) {
		CreateEmptySegment(checkpointer.GetRowGroup().start);
	}

	ColumnDataCheckpointer &checkpointer;
	CompressionFunction &function;
	unique_ptr<ColumnSegment> current_segment;
	BufferHandle handle;

	// Ptr to next free spot in segment;
	data_ptr_t data_ptr;
	// Ptr to next free spot for storing bitwidths and frame-of-references (growing downwards).
	data_ptr_t metadata_ptr;

public:
	void CreateEmptySegment(idx_t row_start) {
		auto &db = checkpointer.GetDatabase();
		auto &type = checkpointer.GetType();

		auto compressed_segment =
		    ColumnSegment::CreateTransientSegment(db, type, row_start, info.GetBlockSize(), info.GetBlockSize());
		compressed_segment->function = function;
		current_segment = std::move(compressed_segment);

		auto &buffer_manager = BufferManager::GetBufferManager(db);
		handle = buffer_manager.Pin(current_segment->block);

		data_ptr = handle.Ptr() + RoaringPrimitives::ROARING_HEADER_SIZE;
		metadata_ptr = handle.Ptr() + info.GetBlockSize();
	}

	void FlushSegment() {
		auto &state = checkpointer.GetCheckpointState();
		auto base_ptr = handle.Ptr();

		// TODO: implement

		state.FlushSegment(std::move(current_segment), std::move(handle), total_segment_size);
	}

	void Finalize() {
		// TODO: implement
		FlushSegment();
		current_segment.reset();
	}
};

template <class T>
unique_ptr<CompressionState> RoaringInitCompression(ColumnDataCheckpointer &checkpointer,
                                                    unique_ptr<AnalyzeState> state) {
	return make_uniq<RoaringCompressState<T>>(checkpointer, state->info);
}

template <class T>
void RoaringCompress(CompressionState &state_p, Vector &scan_vector, idx_t count) {
	auto &state = state_p.Cast<RoaringCompressState<T>>();
	UnifiedVectorFormat vdata;
	scan_vector.ToUnifiedFormat(count, vdata);
	state.Append(vdata, count);
}

template <class T>
void RoaringFinalizeCompress(CompressionState &state_p) {
	auto &state = state_p.Cast<RoaringCompressState<T>>();
	state.Finalize();
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//

template <class T>
struct RoaringScanState : public SegmentScanState {
public:
	explicit RoaringScanState(ColumnSegment &segment) : current_segment(segment) {
		auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
		handle = buffer_manager.Pin(segment.block);
		auto data_ptr = handle.Ptr();

		// TODO: implement
	}

	BufferHandle handle;
	ColumnSegment &current_segment;

public:
	void Skip(ColumnSegment &segment, idx_t skip_count) {
		// TODO: implement
	}
};

template <class T>
unique_ptr<SegmentScanState> RoaringInitScan(ColumnSegment &segment) {
	auto result = make_uniq<RoaringScanState<T>>(segment);
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//
template <class T>
void RoaringScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
                        idx_t result_offset) {
	auto &scan_state = state.scan_state->Cast<RoaringScanState<T>>();

	T *result_data = FlatVector::GetData<T>(result);
	result.SetVectorType(VectorType::FLAT_VECTOR);

	// TODO: implement
}

template <class T>
void RoaringScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result) {
	RoaringScanPartial<T>(segment, state, scan_count, result, 0);
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
template <class T>
void RoaringFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx) {
	RoaringScanState<T> scan_state(segment);

	// TODO: implement
}

template <class T>
void RoaringSkip(ColumnSegment &segment, ColumnScanState &state, idx_t skip_count) {
	auto &scan_state = static_cast<RoaringScanState<T> &>(*state.scan_state);
	scan_state.Skip(segment, skip_count);
}

//===--------------------------------------------------------------------===//
// Get Function
//===--------------------------------------------------------------------===//
template <class T>
CompressionFunction GetCompressionFunction(PhysicalType data_type) {
	return CompressionFunction(CompressionType::COMPRESSION_ROARING, data_type, RoaringInitAnalyze<T>,
	                           RoaringAnalyze<T>, RoaringFinalAnalyze<T>, RoaringInitCompression<T>, RoaringCompress<T>,
	                           RoaringFinalizeCompress<T>, RoaringInitScan<T>, RoaringScan<T>, RoaringScanPartial<T>,
	                           RoaringFetchRow<T>, RoaringSkip<T>);
}

CompressionFunction RoaringCompressionFun::GetFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::VALIDITY:
		return GetCompressionFunction<validity_t>(type);
	default:
		throw InternalException("Unsupported type for Roaring");
	}
}

bool RoaringCompressionFun::TypeIsSupported(const PhysicalType physical_type) {
	switch (physical_type) {
	case PhysicalType::VALIDITY:
		return true;
	default:
		return false;
	}
}

} // namespace duckdb
