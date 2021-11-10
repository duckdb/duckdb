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

		// TODO we could store signed as unsigned as well if no negative values occur possibly allowing a smaller datatype
		// TODO make nicer? possible reuse existing code? or move this to existing code?
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
			// TODO we can stop early if we find a value that exceeds the numerical limits of the type 1 smaller than
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
struct BitpackingConstants {
	static constexpr const idx_t BITPACKING_HEADER_SIZE = sizeof(uint64_t);
};

template <class T>
struct BitpackingCompressState : public CompressionState {
	explicit BitpackingCompressState(ColumnDataCheckpointer &checkpointer, PhysicalType compress_type)
	    : checkpointer(checkpointer), compress_type(compress_type) {
		auto &db = checkpointer.GetDatabase();
		auto &type = checkpointer.GetType();
		auto &config = DBConfig::GetConfig(db);
		function = config.GetCompressionFunction(CompressionType::COMPRESSION_BITPACKING, type.InternalType());
		CreateEmptySegment(checkpointer.GetRowGroup().start);
		max_entry_count = (Storage::BLOCK_SIZE - BitpackingConstants::BITPACKING_HEADER_SIZE) /
		                  GetTypeIdSize(compress_type); // TODO where do we store the compress_type size?
	}

	void CreateEmptySegment(idx_t row_start) {
		auto &db = checkpointer.GetDatabase();
		auto &type = checkpointer.GetType();
		auto compressed_segment = ColumnSegment::CreateTransientSegment(db, type, row_start);
		compressed_segment->function = function;
		current_segment = move(compressed_segment);
		auto &buffer_manager = BufferManager::GetBufferManager(db);
		handle = buffer_manager.Pin(current_segment->block);
	}

	void Append(VectorData &vdata, idx_t count) {
		auto data = (T *)vdata.data;
		for (idx_t i = 0; i < count; i++) {
			auto idx = vdata.sel->get_index(i);
			WriteValue(data[idx], !vdata.validity.RowIsValid(idx));
		}
	}

	// TODO Use Store?
	void WriteCastValue(void *data_pointer, T value) {
		switch (compress_type) {
		case PhysicalType::INT16:
			((int16_t *)data_pointer)[entry_count] = (int16_t)value;
			break;
		case PhysicalType::INT32:
			((int32_t *)data_pointer)[entry_count] = (int32_t)value;
			break;
		case PhysicalType::INT64:
			((int64_t *)data_pointer)[entry_count] = (int64_t)value;
			break;
		case PhysicalType::UINT16:
			((uint16_t *)data_pointer)[entry_count] = (uint16_t)value;
			break;
		case PhysicalType::UINT32:
			((uint32_t *)data_pointer)[entry_count] = (uint32_t)value;
			break;
		case PhysicalType::UINT64:
			((uint64_t *)data_pointer)[entry_count] = (uint64_t)value;
			break;
		default:
			throw InternalException("Unsupported type for Bitpacking");
		}
	}

	void WriteValue(T value, bool is_null) {
		auto data_pointer = (T *)(handle->Ptr() + BitpackingConstants::BITPACKING_HEADER_SIZE);

		WriteCastValue(data_pointer, value);
		entry_count++;

		// update meta data
		if (!is_null) {
			NumericStatistics::Update<T>(current_segment->stats, value);
		}
		current_segment->count++;

		if (entry_count == max_entry_count) {
			// we have finished writing this segment: flush it and create a new segment
			auto row_start = current_segment->start + current_segment->count;
			FlushSegment();
			CreateEmptySegment(row_start);
			entry_count = 0;
		}
	}

	void FlushSegment() {
		auto &state = checkpointer.GetCheckpointState();

		// Store the compressed type in the beginning of the segment
		Store<uint8_t>(static_cast<uint8_t>(compress_type), handle->node->buffer);
		handle.reset();

		state.FlushSegment(move(current_segment), entry_count * GetTypeIdSize(compress_type));
	}

	void Finalize() {
		FlushSegment();
		current_segment.reset();
	}

	ColumnDataCheckpointer &checkpointer;
	CompressionFunction *function;
	unique_ptr<ColumnSegment> current_segment;
	unique_ptr<BufferHandle> handle;

	PhysicalType compress_type;
	idx_t entry_count = 0;
	idx_t max_entry_count;
};

template <class T>
unique_ptr<CompressionState> BitpackingInitCompression(ColumnDataCheckpointer &checkpointer,
                                                       unique_ptr<AnalyzeState> state) {

	PhysicalType smallest_fitting_type = ((BitpackingAnalyzeState<T> *)state.get())->GetSmallestFittingType();
	return make_unique<BitpackingCompressState<T>>(checkpointer, smallest_fitting_type);
}

template <class T>
void BitpackingCompress(CompressionState &state_p, Vector &scan_vector, idx_t count) {
	auto &state = (BitpackingCompressState<T> &)state_p;
	VectorData vdata;
	scan_vector.Orrify(count, vdata);
	state.Append(vdata, count);
}

template <class T>
void BitpackingFinalizeCompress(CompressionState &state_p) {
	auto &state = (BitpackingCompressState<T> &)state_p;
	state.Finalize();
}
//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
struct BitpackingScanState : public SegmentScanState {
	unique_ptr<BufferHandle> handle;
	PhysicalType compress_type;

	void (*decompress_function)(data_ptr_t, data_ptr_t);

	explicit BitpackingScanState(ColumnSegment &segment) {
		auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
		handle = buffer_manager.Pin(segment.block);

		// Load the Compression Type from the BitpackingHeader
		uint8_t compress_type_id = 0;
		compress_type_id = Load<uint8_t>((uint8_t*)handle->node->buffer);
		compress_type = static_cast<PhysicalType>(compress_type_id);
	}
};

template <class FROM_TYPE, class TO_TYPE>
void CastCopy(data_ptr_t dst, data_ptr_t src) {
	*(TO_TYPE *)dst = (TO_TYPE)* (FROM_TYPE *)src;
}

template <class T>
unique_ptr<SegmentScanState> BitpackingInitScan(ColumnSegment &segment) {
	auto result = make_unique<BitpackingScanState>(segment);
	// TODO move elsewhere
	switch (result->compress_type) {
	case PhysicalType::INT16:
		result->decompress_function = &CastCopy<int16_t, T>;
		break;
	case PhysicalType::INT32:
		result->decompress_function = &CastCopy<int32_t, T>;
		break;
	case PhysicalType::INT64:
		result->decompress_function = &CastCopy<int64_t, T>;
		break;
	case PhysicalType::UINT16:
		result->decompress_function = &CastCopy<uint16_t, T>;
		break;
	case PhysicalType::UINT32:
		result->decompress_function = &CastCopy<uint32_t, T>;
		break;
	case PhysicalType::UINT64:
		result->decompress_function = &CastCopy<uint64_t, T>;
		break;
	default:
		throw InternalException("Invalid type found in Bitpacking");
	}

	return move(result);
}

//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//
template <class T>
void BitpackingScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
                           idx_t result_offset) {
	auto &scan_state = (BitpackingScanState &)*state.scan_state;
	auto start = segment.GetRelativeIndex(state.row_index);

	auto data = scan_state.handle->node->buffer + segment.GetBlockOffset();
	auto source_data = data + start * GetTypeIdSize(scan_state.compress_type) + BitpackingConstants::BITPACKING_HEADER_SIZE;

	// copy the data from the base table
	result.SetVectorType(VectorType::FLAT_VECTOR);

	auto result_data = FlatVector::GetData(result) + result_offset * sizeof(T);

	for (idx_t i = 0; i < scan_count; i++) {
		scan_state.decompress_function(result_data, source_data);
		result_data += sizeof(T);
		source_data += GetTypeIdSize(scan_state.compress_type);
	}
}

template <class T>
void BitpackingScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result) {
	BitpackingScanPartial<T>(segment, state, scan_count, result, 0);
}
//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
template <class T>
void BitpackingFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result,
                        idx_t result_idx) {
	BitpackingScanState scan_state(segment);
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	auto handle = buffer_manager.Pin(segment.block);

	auto data_ptr = handle->node->buffer + segment.GetBlockOffset() + row_id * GetTypeIdSize(scan_state.compress_type);
	auto result_data = FlatVector::GetData(result) + result_idx * sizeof(T);
	scan_state.decompress_function(result_data, data_ptr);
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
	case PhysicalType::INT16:
		return GetBitpackingFunction<int16_t>(type);
	case PhysicalType::INT32:
		return GetBitpackingFunction<int32_t>(type);
	case PhysicalType::INT64:
		return GetBitpackingFunction<int64_t>(type);
	case PhysicalType::UINT16:
		return GetBitpackingFunction<uint16_t>(type);
	case PhysicalType::UINT32:
		return GetBitpackingFunction<uint32_t>(type);
	case PhysicalType::UINT64:
		return GetBitpackingFunction<uint64_t>(type);
	default:
		throw InternalException("Unsupported type for Bitpacking");
	}
}

bool BitpackingFun::TypeIsSupported(PhysicalType type) {
	// TODO support INT128
	switch (type) {
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
	case PhysicalType::UINT16:
	case PhysicalType::UINT32:
	case PhysicalType::UINT64:
		return true;
	default:
		return false;
	}
}

} // namespace duckdb
