#include "duckdb/common/limits.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/function/compression/compression.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/statistics/numeric_statistics.hpp"
#include "duckdb/storage/table/column_data_checkpointer.hpp"
#include "duckdb/storage/table/column_segment.hpp"

#include <duckdb/storage/segment/uncompressed.hpp>
#include <functional>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Analyze
//===--------------------------------------------------------------------===//
template <class T>
struct BitpackingAnalyzeState : public AnalyzeState {
	explicit BitpackingAnalyzeState(PhysicalType current_type)
	    : min_value(NumericLimits<T>::Maximum()), max_value(NumericLimits<T>::Minimum()), count(0),
	      current_type(current_type) {
	}
	T min_value;
	T max_value;
	idx_t count;
	PhysicalType current_type;

	// Calculates the smallest fitting type
	PhysicalType GetSmallestFittingType() {
		size_t current_size = sizeof(T);
		D_ASSERT(current_size == 2 || current_size == 4 || current_size == 8);

		// TODO make nicer?
		if (std::is_signed<T>::value) {
			if ((int64_t)min_value > (int64_t)NumericLimits<int8_t>::Minimum() &&
			    (int64_t)max_value < (int64_t)NumericLimits<int8_t>::Maximum()) {
				return PhysicalType::INT8;
			}
			if ((int64_t)min_value > (int64_t)NumericLimits<int16_t>::Minimum() &&
			    (int64_t)max_value < (int64_t)NumericLimits<int16_t>::Maximum()) {
				return PhysicalType::INT16;
			}
			if ((int64_t)min_value > (int64_t)NumericLimits<int32_t>::Minimum() &&
			    (int64_t)max_value < (int64_t)NumericLimits<int32_t>::Maximum()) {
				return PhysicalType::INT32;
			}
		} else {
			if ((uint64_t)max_value < (uint64_t)NumericLimits<uint8_t>::Maximum()) {
				return PhysicalType::UINT8;
			}
			if ((uint64_t)max_value < (uint64_t)NumericLimits<uint16_t>::Maximum()) {
				return PhysicalType::UINT16;
			}
			if ((uint64_t)max_value < (uint64_t)NumericLimits<uint32_t>::Maximum()) {
				return PhysicalType::UINT32;
			}
		}

		return current_type;
	}
};

template <class T>
unique_ptr<AnalyzeState> BitpackingInitAnalyze(ColumnData &col_data, PhysicalType type) {
	return make_unique<BitpackingAnalyzeState<T>>(type);
}

template <class T>
bool BitpackingAnalyze(AnalyzeState &state, Vector &input, idx_t count) {
	auto &bitpacking_state = (BitpackingAnalyzeState<T> &)state;
	VectorData vdata;
	input.Orrify(count, vdata);

	auto data = (T *)vdata.data;
	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);

		if (vdata.validity.RowIsValid(idx)) {
			if (data[idx] > bitpacking_state.max_value) {
				bitpacking_state.max_value = data[idx];
			}

			if (std::is_signed<T>::value) {
				if (data[idx] < bitpacking_state.min_value) {
					bitpacking_state.min_value = data[idx];
				}
			}
			// TODO if min_value or max_value approaches a value that prevents bitpacking, return false
		}
	}
	bitpacking_state.count += count;
	return true;
}

template <class T>
idx_t BitpackingFinalAnalyze(AnalyzeState &state) {
	auto &bitpacking_state = (BitpackingAnalyzeState<T> &)state;
	return GetTypeIdSize(bitpacking_state.GetSmallestFittingType()) * bitpacking_state.count;
}

//===--------------------------------------------------------------------===//
// Compress
//===--------------------------------------------------------------------===//
template <class T>
struct BitpackingCompressState : public CompressionState {
	explicit BitpackingCompressState(ColumnDataCheckpointer &checkpointer, PhysicalType compress_type)
	    : checkpointer(checkpointer), compress_type(compress_type) {
		CreateEmptySegment(checkpointer.GetRowGroup().start);
	}

	void CreateEmptySegment(idx_t row_start) {
		auto &db = checkpointer.GetDatabase();
		auto &type = checkpointer.GetType();
		auto compressed_segment = ColumnSegment::CreateTransientSegment(db, type, row_start);

		current_segment = move(compressed_segment);
	}

	void FlushSegment(idx_t segment_size) {
		auto &state = checkpointer.GetCheckpointState();
		state.FlushSegment(move(current_segment), segment_size);
	}

	void Finalize(idx_t segment_size) {
		FlushSegment(segment_size);
		current_segment.reset();
	}

	ColumnDataCheckpointer &checkpointer;
	unique_ptr<ColumnSegment> current_segment;
	unique_ptr<BufferHandle> handle;

	PhysicalType compress_type;
};

template <class T>
unique_ptr<CompressionState> BitpackingInitCompression(ColumnDataCheckpointer &checkpointer,
                                                       unique_ptr<AnalyzeState> state) {

	PhysicalType smallest_fitting_type = ((BitpackingAnalyzeState<T>*)state.get())->GetSmallestFittingType();
	return make_unique<BitpackingCompressState<T>>(checkpointer, smallest_fitting_type);
}

template <class T>
void BitpackingCompress(CompressionState &state_p, Vector &data, idx_t count) {
	auto &state = (BitpackingCompressState<T> &)state_p;
	VectorData vdata;
	data.Orrify(count, vdata);

	ColumnAppendState append_state;
	idx_t offset = 0;
	while (count > 0) {
		// TODO this does not actually compress yet
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

template <class T>
void BitpackingFinalizeCompress(CompressionState &state_p) {
	auto &state = (BitpackingCompressState<T> &)state_p;
	state.Finalize(state.current_segment->FinalizeAppend());
}
//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
struct BitPackingScanState : public SegmentScanState {
	unique_ptr<BufferHandle> handle;
};

template <class T>
unique_ptr<SegmentScanState> BitpackingInitScan(ColumnSegment &segment) {
	auto result = make_unique<BitPackingScanState>();
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	result->handle = buffer_manager.Pin(segment.block);
	return move(result);
}
//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//
template <class T>
void BitpackingScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
                           idx_t result_offset) {
	auto &scan_state = (BitPackingScanState &)*state.scan_state;
	auto start = segment.GetRelativeIndex(state.row_index);

	auto data = scan_state.handle->node->buffer + segment.GetBlockOffset();
	auto source_data = data + start * sizeof(T);

	// copy the data from the base table
	result.SetVectorType(VectorType::FLAT_VECTOR);
	memcpy(FlatVector::GetData(result) + result_offset * sizeof(T), source_data, scan_count * sizeof(T));
}

template <class T>
void BitpackingScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result) {
	// FIXME: we should be able to do a zero-copy here
	BitpackingScanPartial<T>(segment, state, scan_count, result, 0);
}
//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
template <class T>
void BitpackingFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result,
                        idx_t result_idx) {
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	auto handle = buffer_manager.Pin(segment.block);

	// first fetch the data from the base table
	auto data_ptr = handle->node->buffer + segment.GetBlockOffset() + row_id * sizeof(T);

	memcpy(FlatVector::GetData(result) + result_idx * sizeof(T), data_ptr, sizeof(T));
}
//===--------------------------------------------------------------------===//
// Get Function
//===--------------------------------------------------------------------===//
template <class T>
CompressionFunction GetBitpackingFunction(PhysicalType data_type) {
	return CompressionFunction(CompressionType::COMPRESSION_BITPACKING, data_type, BitpackingInitAnalyze<T>,
	                           BitpackingAnalyze<T>, BitpackingFinalAnalyze<T>, BitpackingInitCompression<T>,
	                           BitpackingCompress<T>, BitpackingFinalizeCompress<T>, BitpackingInitScan<T>,
	                           BitpackingScan<T>, BitpackingScanPartial<T>, BitpackingFetchRow<T>,
	                           UncompressedFunctions::EmptySkip);
}

CompressionFunction BitpackingFun::GetFunction(PhysicalType type) {
	switch (type) {
		//	case PhysicalType::BOOL:
		//	case PhysicalType::INT8:
		//		return GetBitpackingFunction<int8_t>(type); // Not compressible at byte alignment
	case PhysicalType::INT16:
		return GetBitpackingFunction<int16_t>(type);
	case PhysicalType::INT32:
		return GetBitpackingFunction<int32_t>(type);
	case PhysicalType::INT64:
		return GetBitpackingFunction<int64_t>(type);
		//	case PhysicalType::INT128:
		//		return GetBitpackingFunction<hugeint_t>(type); // TODO implement
		//	case PhysicalType::UINT8:
		//		return GetBitpackingFunction<uint8_t>(type); // Not compressible at byte alignment
	case PhysicalType::UINT16:
		return GetBitpackingFunction<uint16_t>(type);
	case PhysicalType::UINT32:
		return GetBitpackingFunction<uint32_t>(type);
	case PhysicalType::UINT64:
		return GetBitpackingFunction<uint64_t>(type);
	default:
		throw InternalException("Unsupported type for RLE");
	}
}

bool BitpackingFun::TypeIsSupported(PhysicalType type) {
	switch (type) {
		//	case PhysicalType::BOOL:
		//	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
		//	case PhysicalType::INT128:
		//	case PhysicalType::UINT8:
	case PhysicalType::UINT16:
	case PhysicalType::UINT32:
	case PhysicalType::UINT64:
		return true;
	default:
		return false;
	}
}

} // namespace duckdb
