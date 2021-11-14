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

using bitpacking_width_t = uint8_t;

struct BitpackingConstants {
	static constexpr const idx_t BITPACKING_HEADER_SIZE = sizeof(uint64_t);

	// needs to be a factor of STANDARD_VECTOR_SIZE
	static constexpr const idx_t BITPACKING_GROUPING_SIZE = STANDARD_VECTOR_SIZE;
};

template<class T>
bitpacking_width_t MinimumBitWidth(T min_value, T max_value) {
	if (std::is_signed<T>::value) {
		if ((int64_t)min_value > (int64_t)NumericLimits<int8_t>::Minimum() &&
		    (int64_t)max_value < (int64_t)NumericLimits<int8_t>::Maximum()) {
			return 1;
		}
		if ((int64_t)min_value > (int64_t)NumericLimits<int16_t>::Minimum() &&
		    (int64_t)max_value < (int64_t)NumericLimits<int16_t>::Maximum()) {
			return 2;
		}
		if ((int64_t)min_value > (int64_t)NumericLimits<int32_t>::Minimum() &&
		    (int64_t)max_value < (int64_t)NumericLimits<int32_t>::Maximum()) {
			return 4;
		}
	} else {
		if ((uint64_t)max_value < (uint64_t)NumericLimits<uint8_t>::Maximum()) {
			return 1;
		}
		if ((uint64_t)max_value < (uint64_t)NumericLimits<uint16_t>::Maximum()) {
			return 2;
		}
		if ((uint64_t)max_value < (uint64_t)NumericLimits<uint32_t>::Maximum()) {
			return 4;
		}
	}

	return 8;
}

template<class T>
bitpacking_width_t FindMinBitWidth(VectorData &vdata, idx_t count) {
	auto data = (T *)vdata.data;
	T min_value = NumericLimits<T>::Maximum();
	T max_value = NumericLimits<T>::Minimum();
	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);
		if (vdata.validity.RowIsValid(idx)) {
			if (data[idx] > max_value) {
				max_value = data[idx];
			}

			if (std::is_signed<T>::value) {
				if (data[idx] < min_value) {
					min_value = data[idx];
				}
			}
			// TODO we can stop early here depending on available bit widths?
		}
	}

	return MinimumBitWidth<T>(min_value, max_value);
}

//===--------------------------------------------------------------------===//
// Analyze
//===--------------------------------------------------------------------===//
template <class T>
struct BitpackingAnalyzeState : public AnalyzeState {
//	explicit BitpackingAnalyzeState() {
//	}
	idx_t total_size = 0;
};

template <class T>
unique_ptr<AnalyzeState> BitpackingInitAnalyze(ColumnData &col_data, PhysicalType type) {
	return make_unique<BitpackingAnalyzeState<T>>();
}

// TODO this size is not very accurate as it disregards both the header and the empty spaces that are to small for a
// TODO bitpacking group
template <class T>
bool BitpackingAnalyze(AnalyzeState &state, Vector &input, idx_t count) {
	auto &bitpacking_state = (BitpackingAnalyzeState<T> &)state;
	VectorData vdata;
	input.Orrify(count, vdata);

  bitpacking_width_t bitwidth = FindMinBitWidth<T>(vdata, count);
	bitpacking_state.total_size = (idx_t)bitwidth * count + sizeof(bitpacking_width_t);
	return true;
}

template <class T>
idx_t BitpackingFinalAnalyze(AnalyzeState &state) {
	auto &bitpacking_state = (BitpackingAnalyzeState<T> &)state;
	return bitpacking_state.total_size;
}

//===--------------------------------------------------------------------===//
// Compress
//===--------------------------------------------------------------------===//
template <class T>
struct BitpackingCompressState : public CompressionState {
	explicit BitpackingCompressState(ColumnDataCheckpointer &checkpointer)
	    : checkpointer(checkpointer) {
		auto &db = checkpointer.GetDatabase();
		auto &type = checkpointer.GetType();
		auto &config = DBConfig::GetConfig(db);
		function = config.GetCompressionFunction(CompressionType::COMPRESSION_BITPACKING, type.InternalType());
		CreateEmptySegment(checkpointer.GetRowGroup().start);
	}

	// Space remaining between the width_ptr growing down and data ptr growing up
	idx_t RemainingSize() {
		return width_ptr - data_ptr;
	}

	void CreateEmptySegment(idx_t row_start) {
		auto &db = checkpointer.GetDatabase();
		auto &type = checkpointer.GetType();
		auto compressed_segment = ColumnSegment::CreateTransientSegment(db, type, row_start);
		compressed_segment->function = function;
		current_segment = move(compressed_segment);
		auto &buffer_manager = BufferManager::GetBufferManager(db);
		handle = buffer_manager.Pin(current_segment->block);

		data_ptr = handle->Ptr() + BitpackingConstants::BITPACKING_HEADER_SIZE;
		width_ptr = handle->Ptr() + Storage::BLOCK_SIZE - sizeof(bitpacking_width_t);
	}

	// TODO rewrite for bitpacking groups below vector size
	// TODO rewrite with a packValue function that packs a fixed amount of values?
	idx_t Append(VectorData &vdata, idx_t offset, idx_t count) {
		auto data = (T *)vdata.data;

		auto bitwidth = FindMinBitWidth<T>(vdata, count);

		// TODO we now stop when a vector does not fit, this could be improved to the bitpacking grouping size
		if (RemainingSize() < bitwidth * count + sizeof(bitpacking_width_t)) {
			// Does not fit in segment, append nothing.
			return 0;
		}

		// APPEND LOOP
		// TODO optimize like appendloop from uncompressed segment.
		for (idx_t i = 0; i < count; i++) {
			auto idx = vdata.sel->get_index(i + offset);
			bool is_null = !vdata.validity.RowIsValid(idx);
			if (!is_null) {
				NumericStatistics::Update<T>(current_segment->stats, data[idx]);
				PackValue(data[idx], bitwidth);
			}

			current_segment->count++;
		}

		// Store bitwidth for this vector
		Store<bitpacking_width_t>(bitwidth, width_ptr);
		width_ptr -= sizeof(bitpacking_width_t);

		return count;
	}

	// TODO do switch statement once per vector?
	void PackValue(T value, bitpacking_width_t width) {
		switch (width) {
		case 1:
			(*(int8_t *)data_ptr) = (int8_t)value;
			data_ptr += 1;
			break;
		case 2:
			(*(int16_t *)data_ptr) = (int16_t)value;
			data_ptr += 2;
			break;
		case 4:
			(*(int32_t *)data_ptr) = (int32_t)value;
			data_ptr += 4;
			break;
		case 8:
			(*(int64_t *)data_ptr) = (int64_t)value;
			data_ptr += 8;
			break;
		default:
			throw InternalException("Unsupported type for Bitpacking");
		}
	}

	void FlushSegment() {
		auto &state = checkpointer.GetCheckpointState();

		// compact the segment by moving the widths next to the data.
		idx_t minimal_widths_offset = AlignValue(data_ptr - handle->node->buffer);
		idx_t widths_size = handle->node->buffer + Storage::BLOCK_SIZE - width_ptr - 1;
		idx_t total_segment_size = minimal_widths_offset + widths_size;
		memmove(handle->node->buffer + minimal_widths_offset, width_ptr+1, widths_size);

		// Store the offset of the first width (which is at the highest address).
		Store<idx_t>(minimal_widths_offset + widths_size - 1, handle->node->buffer);
		handle.reset();

		state.FlushSegment(move(current_segment), total_segment_size);
	}

	void Finalize() {
		FlushSegment();
		current_segment.reset();
	}

	ColumnDataCheckpointer &checkpointer;
	CompressionFunction *function;
	unique_ptr<ColumnSegment> current_segment;
	unique_ptr<BufferHandle> handle;

	// ptr to next free spot in segment;
	data_ptr_t data_ptr;
	// ptr to next free spot for storing bitwidths (growing downwards).
	data_ptr_t width_ptr;
};

template <class T>
unique_ptr<CompressionState> BitpackingInitCompression(ColumnDataCheckpointer &checkpointer,
                                                       unique_ptr<AnalyzeState> state) {
	return make_unique<BitpackingCompressState<T>>(checkpointer);
}

template <class T>
void BitpackingCompress(CompressionState &state_p, Vector &scan_vector, idx_t count) {
	auto &state = (BitpackingCompressState<T> &)state_p;
	VectorData vdata;
	scan_vector.Orrify(count, vdata);

	idx_t offset = 0;
	while (count > 0) {
		idx_t appended = state.Append(vdata, offset, count);

		auto next_start = state.current_segment->start + state.current_segment->count;

		// the segment is full: flush it to disk
		state.FlushSegment();

		// now create a new segment and continue appending
		state.CreateEmptySegment(next_start);

		if (appended == count) {
			// appended everything: finished
			return;
		}

		offset += appended;
		count -= appended;
	}
}

template <class T>
void BitpackingFinalizeCompress(CompressionState &state_p) {
	auto &state = (BitpackingCompressState<T> &)state_p;
	state.Finalize();
}
//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
template <class FROM_TYPE, class TO_TYPE>
void UnpackUnsigned(data_ptr_t dst, data_ptr_t src) {
	*(TO_TYPE *)dst = (TO_TYPE) * (FROM_TYPE *)src;
}

template <class FROM_TYPE, class TO_TYPE>
void UnpackSigned(data_ptr_t dst, data_ptr_t src) {
	*(TO_TYPE *)dst = (TO_TYPE) * (FROM_TYPE *)src;
}

template <class T>
struct BitpackingScanState : public SegmentScanState {
	unique_ptr<BufferHandle> handle;
	PhysicalType compress_type;

	void (*decompress_function)(data_ptr_t, data_ptr_t);

	explicit BitpackingScanState(ColumnSegment &segment) {
		auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
		handle = buffer_manager.Pin(segment.block);

//		D_ASSERT(segment.GetBlockOffset() == 0); // TODO why is this?

		current_ptr = handle->node->buffer + segment.GetBlockOffset() + BitpackingConstants::BITPACKING_HEADER_SIZE;

		type = segment.type.InternalType();

		// load offset to bitpacking widths pointer
		auto bitpacking_widths_offset = Load<idx_t>(handle->node->buffer + segment.GetBlockOffset());
		bitpacking_width_ptr = handle->node->buffer + segment.GetBlockOffset() + bitpacking_widths_offset;

		// load the bitwidth of the first vector
		LoadCurrentBitWidth();
	}

	void LoadCurrentBitWidth() {
		current_width = Load<bitpacking_width_t>(bitpacking_width_ptr);
		LoadDecompressFunction();
	}

	void Skip(ColumnSegment &segment, idx_t skip_count) {

		while (skip_count > 0) {

			if (position_in_group + skip_count < BitpackingConstants::BITPACKING_GROUPING_SIZE) {

				// We're not leaving this bitpacking group, we can perform all skips.
				current_ptr += skip_count * current_width;
				position_in_group += skip_count;
				break;

			} else {
				// The skip crosses the current bitpacking group, we skip the remainder of this group.
				auto skipping = BitpackingConstants::BITPACKING_GROUPING_SIZE - position_in_group;

				position_in_group = 0;
				current_ptr += skipping * current_width;

				// Update width pointer and load new width
				bitpacking_width_ptr -= sizeof(bitpacking_width_t);
				LoadCurrentBitWidth();

				skip_count -= skipping;
			}
		}
	}

	void LoadDecompressFunction() {
		if (NumericLimits<T>::IsSigned()) {
			switch (current_width) {
			case sizeof(int8_t):
				decompress_function = &UnpackSigned<int8_t, T>;
				break;
			case sizeof(int16_t):
				decompress_function = &UnpackSigned<int16_t, T>;
				break;
			case sizeof(int32_t):
				decompress_function = &UnpackSigned<int32_t, T>;
				break;
			case sizeof(int64_t):
				decompress_function = &UnpackSigned<int64_t, T>;
				break;
			default:
				throw InternalException("Incorrect bit width found in bitpacking");
			}
		} else {
			switch (current_width) {
			case sizeof(uint8_t):
				decompress_function = &UnpackSigned<uint8_t, T>;
				break;
			case sizeof(uint16_t):
				decompress_function = &UnpackSigned<uint16_t, T>;
				break;
			case sizeof(uint32_t):
				decompress_function = &UnpackSigned<uint32_t, T>;
				break;
			case sizeof(uint64_t):
				decompress_function = &UnpackSigned<uint64_t, T>;
				break;
			default:
				throw InternalException("Incorrect bit width found in bitpacking");
			}
		}
	}

	idx_t position_in_group = 0;
	data_ptr_t current_ptr;
	data_ptr_t bitpacking_width_ptr;
	bitpacking_width_t current_width;

	PhysicalType type;
};

template <class T>
unique_ptr<SegmentScanState> BitpackingInitScan(ColumnSegment &segment) {
	auto result = make_unique<BitpackingScanState<T>>(segment);
	return move(result);
}

//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//
template <class T>
void BitpackingScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
                           idx_t result_offset) {
	auto &scan_state = (BitpackingScanState<T> &)*state.scan_state;

	T *result_data = FlatVector::GetData<T>(result);
	result.SetVectorType(VectorType::FLAT_VECTOR);
	for (idx_t i = 0; i < scan_count; i++) {
		if (scan_state.position_in_group >= BitpackingConstants::BITPACKING_GROUPING_SIZE) {
			// handled all values in this bitpacking group.
			scan_state.position_in_group = 0;
			scan_state.bitpacking_width_ptr -= sizeof(bitpacking_width_t);
			scan_state.LoadCurrentBitWidth();
		}
		T *current_result_ptr = result_data + result_offset + i;
		scan_state.decompress_function((data_ptr_t)current_result_ptr, scan_state.current_ptr);
		scan_state.current_ptr += scan_state.current_width;

		scan_state.position_in_group++;
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
	BitpackingScanState<T> scan_state(segment);
	scan_state.Skip(segment, row_id);
	auto result_data = FlatVector::GetData<T>(result);
	T *current_result_ptr = result_data + result_idx;
	scan_state.decompress_function((data_ptr_t)current_result_ptr, scan_state.current_ptr);
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
	// TODO support INT128?
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
