#include "duckdb/common/limits.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/function/compression/compression.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/statistics/numeric_statistics.hpp"
#include "duckdb/storage/table/column_data_checkpointer.hpp"
#include "duckdb/storage/table/column_segment.hpp"

#include <bitpackinghelpers.h>
#include <duckdb/storage/segment/uncompressed.hpp>
#include <functional>

namespace duckdb {

using bitpacking_width_t = uint8_t;

struct BitpackingConstants {
	static constexpr const idx_t BITPACKING_HEADER_SIZE = sizeof(uint64_t);

	// needs to be a factor of STANDARD_VECTOR_SIZE
	static constexpr const idx_t BITPACKING_GROUPING_SIZE = STANDARD_VECTOR_SIZE;
	// Needs to be a factor of BITPACKING_GROUPING_SIZE
	static constexpr const idx_t BITPACKING_ALGORITHM_GROUPING = 32;
};

template <class T>
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

template <class T>
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

struct EmptyBitpackingWriter {
	template <class T>
	static void Operation(T *values, bitpacking_width_t width, idx_t count, void *data_ptr) {
	}
};

template <class T>
bitpacking_width_t FindMinBitWidth(T *values, idx_t count) {
	T min_value = NumericLimits<T>::Maximum();
	T max_value = NumericLimits<T>::Minimum();

	for (idx_t i = 0; i < count; i++) {
		if (values[i] > max_value) {
			max_value = values[i];
		}

		if (std::is_signed<T>::value) {
			if (values[i] < min_value) {
				min_value = values[i];
			}
		}
	}

	return MinimumBitWidth<T>(min_value, max_value);
}

template <class T>
struct BitpackingState {

	BitpackingState()
	    : compression_buffer_size(BitpackingConstants::BITPACKING_GROUPING_SIZE), compression_buffer_idx(0),
	      total_size(0), data_ptr(nullptr) {
	}

	T compression_buffer[BitpackingConstants::BITPACKING_GROUPING_SIZE];
	idx_t compression_buffer_size;
	idx_t compression_buffer_idx;
	idx_t total_size;
	void *data_ptr;

public:
	template <class OP>
	void Flush() {
		bitpacking_width_t width = FindMinBitWidth<T>(compression_buffer, compression_buffer_idx);
		OP::Operation(compression_buffer, width, compression_buffer_idx, data_ptr);
		total_size += compression_buffer_size * width + sizeof(bitpacking_width_t);
	}

	template <class OP = EmptyBitpackingWriter>
	void Update(T *data, ValidityMask &validity, idx_t idx) {

		if (validity.RowIsValid(idx)) {
			compression_buffer[compression_buffer_idx++] = data[idx];
		} else {
			// We write zero for easy bitwidth analysis of the compression buffer later
			compression_buffer[compression_buffer_idx++] = 0;
		}

		if (compression_buffer_idx == compression_buffer_size) {
			// calculate bitpacking width;
			Flush<OP>();
			compression_buffer_idx = 0;
		}
	}
};

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
	bitpacking_state.total_size += (idx_t)bitwidth * count + sizeof(bitpacking_width_t);
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
	explicit BitpackingCompressState(ColumnDataCheckpointer &checkpointer) : checkpointer(checkpointer) {
		auto &db = checkpointer.GetDatabase();
		auto &type = checkpointer.GetType();
		auto &config = DBConfig::GetConfig(db);
		function = config.GetCompressionFunction(CompressionType::COMPRESSION_BITPACKING, type.InternalType());
		CreateEmptySegment(checkpointer.GetRowGroup().start);

		state.data_ptr = (void *)this;
	}

	struct BitpackingWriter {
		template <class VALUE_TYPE>
		static void Operation(VALUE_TYPE *values, bitpacking_width_t width, idx_t count, void *data_ptr) {
			auto state = (BitpackingCompressState<T> *)data_ptr;
			state->WriteValues(values, width, count);
		}
	};

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

		// TODO why is this?
		D_ASSERT(current_segment->GetBlockOffset() == 0);

		data_ptr = handle->Ptr() + current_segment->GetBlockOffset() + BitpackingConstants::BITPACKING_HEADER_SIZE;
		width_ptr =
		    handle->Ptr() + current_segment->GetBlockOffset() + Storage::BLOCK_SIZE - sizeof(bitpacking_width_t);
	}

	void Append(VectorData &vdata, idx_t count) {
		auto data = (T *)vdata.data;
		for (idx_t i = 0; i < count; i++) {
			auto idx = vdata.sel->get_index(i);
			state.template Update<BitpackingCompressState<T>::BitpackingWriter>(data, vdata.validity, idx);
			if (vdata.validity.RowIsValid(idx)) {
				NumericStatistics::Update<T>(current_segment->stats, data[idx]);
			}
		}
	}

	void WriteValues(T *values, bitpacking_width_t width, idx_t count) {

		if (count < BitpackingConstants::BITPACKING_GROUPING_SIZE) {
			// TODO writing partial group, special case??
		}

		if (RemainingSize() < width * BitpackingConstants::BITPACKING_GROUPING_SIZE + sizeof(bitpacking_width_t)) {
			// Segment is full
			auto row_start = current_segment->start + current_segment->count;
			FlushSegment();
			CreateEmptySegment(row_start);
		}

		// Todo we might not need to do the whole thing?
		idx_t compress_loops =
		    BitpackingConstants::BITPACKING_GROUPING_SIZE / BitpackingConstants::BITPACKING_ALGORITHM_GROUPING;
		for (idx_t i = 0; i < compress_loops; i++) {
			PackValueGroupedLemire(data_ptr, &values[i * BitpackingConstants::BITPACKING_ALGORITHM_GROUPING], width);
			data_ptr += BitpackingConstants::BITPACKING_ALGORITHM_GROUPING * width;
		}

		Store<bitpacking_width_t>(width, width_ptr);
		width_ptr -= sizeof(bitpacking_width_t);

		current_segment->count += count;
	}

	void PackValueGroupedLemire(data_ptr_t dst, T *values, bitpacking_width_t width) {
		//		duckdb_lemire_bitpacking::fastpack((uint*)values, (uint*)dst, width*8);

		//		if (std::is_same<T, uint8_t>::value || std::is_same<T, int8_t>::value) {
		//			FastPForLib::packblockup<32, uint8_t>((const uint8_t *)values, (uint32_t *)dst, (uint32_t)width *
		// 8);
		//		}
		//		if (std::is_same<T, uint16_t>::value || std::is_same<T, int16_t>::value) {
		//			FastPForLib::packblockup<32, uint16_t>((const uint16_t *)values, (uint32_t *)dst, (uint32_t)width *
		// 8);
		//		}
		if (std::is_same<T, uint32_t>::value || std::is_same<T, int32_t>::value) {
			FastPForLib::packblockup<32, uint32_t>((const uint32_t *)values, (uint32_t *)dst, (uint32_t)width * 8);
		}
		if (std::is_same<T, uint64_t>::value || std::is_same<T, int64_t>::value) {
			FastPForLib::packblockup<32, uint64_t>((const uint64_t *)values, (uint32_t *)dst, (uint32_t)width * 8);
		}
	}
	void PackValueGrouped(data_ptr_t dst, T *values, bitpacking_width_t width) {
		for (idx_t i = 0; i < BitpackingConstants::BITPACKING_ALGORITHM_GROUPING; i++) {
			PackValue(dst + i * width, values[i], width);
		}
	}

	// TODO do switch statement once per vector?
	void PackValue(data_ptr_t dst_ptr, T value, bitpacking_width_t width) {
		switch (width) {
		case 1:
			(*(int8_t *)dst_ptr) = (int8_t)value;
			break;
		case 2:
			(*(int16_t *)dst_ptr) = (int16_t)value;
			break;
		case 4:
			(*(int32_t *)dst_ptr) = (int32_t)value;
			break;
		case 8:
			(*(int64_t *)dst_ptr) = (int64_t)value;
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
		memmove(handle->node->buffer + minimal_widths_offset, width_ptr + 1, widths_size);

		// Store the offset of the first width (which is at the highest address).
		Store<idx_t>(minimal_widths_offset + widths_size - 1, handle->node->buffer);
		handle.reset();

		state.FlushSegment(move(current_segment), total_segment_size);
	}

	void Finalize() {
		state.template Flush<BitpackingCompressState<T>::BitpackingWriter>();

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

	BitpackingState<T> state;
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
template <class FROM_TYPE, class TO_TYPE>
void UnpackUnsigned(data_ptr_t dst, data_ptr_t src) {
	*(TO_TYPE *)dst = (TO_TYPE) * (FROM_TYPE *)src;
}

template <class FROM_TYPE, class TO_TYPE>
void UnpackSigned(data_ptr_t dst, data_ptr_t src) {
	*(TO_TYPE *)dst = (TO_TYPE) * (FROM_TYPE *)src;
}

template <class FROM_TYPE, class TO_TYPE>
void UnpackUnsigned32(data_ptr_t dst, data_ptr_t src, bitpacking_width_t width) {
	for (int i = 0; i < 32; i++) {
		UnpackUnsigned<FROM_TYPE, TO_TYPE>(dst + i * sizeof(TO_TYPE), src + i * sizeof(FROM_TYPE));
	}
}

template <class FROM_TYPE, class TO_TYPE>
void UnpackSigned32(data_ptr_t dst, data_ptr_t src, bitpacking_width_t width) {
	for (int i = 0; i < 32; i++) {
		UnpackSigned<FROM_TYPE, TO_TYPE>(dst + i * sizeof(TO_TYPE), src + i * sizeof(FROM_TYPE));
	}
}

template <class T>
void UnPackLemire(data_ptr_t dst, data_ptr_t src, bitpacking_width_t width) {
	if (std::is_same<T, uint32_t>::value || std::is_same<T, int32_t>::value) {
		FastPForLib::unpackblock<BitpackingConstants::BITPACKING_ALGORITHM_GROUPING, uint32_t>(
		    (const uint32_t *)src, (uint32_t *)dst, (uint32_t)width * 8);
	}
	if (std::is_same<T, uint64_t>::value || std::is_same<T, int64_t>::value) {
		FastPForLib::unpackblock<BitpackingConstants::BITPACKING_ALGORITHM_GROUPING, uint64_t>(
		    (const uint32_t *)src, (uint64_t *)dst, (uint32_t)width * 8);
	}

	// Signed values need this extra step to restore the correct value. In the most significant (sizeof(T) - width) bits
	// need to be set to 1 if the number was negative, which is the case when the (width-1)-th bit of the compressed
	// value is 1
	if (NumericLimits<T>::IsSigned()) {
		idx_t shift = width * 8 - 1;
		for (idx_t i = 0; i < BitpackingConstants::BITPACKING_ALGORITHM_GROUPING; ++i) {
			if (std::is_same<T, int32_t>::value) {
				uint32_t most_significant_compressed_bit = *((uint32_t*)dst + i) >> shift;
				D_ASSERT(most_significant_compressed_bit == 1 || most_significant_compressed_bit == 0);
				if (most_significant_compressed_bit == 1) {
					uint32_t mask = ((uint32_t)-1) << shift;
					*(uint32_t *)(dst + i * sizeof(T)) |= mask;
				}
			}

			if (std::is_same<T, int64_t>::value) {
				uint32_t most_significant_compressed_bit = *((uint64_t*)dst + i) >> shift;
				D_ASSERT(most_significant_compressed_bit == 1 || most_significant_compressed_bit == 0);
				if (most_significant_compressed_bit == 1) {
					uint64_t mask = ((uint64_t)-1) << shift;
					*(uint64_t *)(dst + i * sizeof(T)) |= mask;
				}
			}
		}
	}
}

template <class T>
struct BitpackingScanState : public SegmentScanState {
	unique_ptr<BufferHandle> handle;
	PhysicalType compress_type;

	void (*decompress_function)(data_ptr_t, data_ptr_t, bitpacking_width_t);

	explicit BitpackingScanState(ColumnSegment &segment) {
		auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
		handle = buffer_manager.Pin(segment.block);

		current_group_ptr =
		    handle->node->buffer + segment.GetBlockOffset() + BitpackingConstants::BITPACKING_HEADER_SIZE;

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
				current_group_ptr += skip_count * current_width;
				position_in_group += skip_count;
				break;

			} else {
				// The skip crosses the current bitpacking group, we skip the remainder of this group.
				auto skipping = BitpackingConstants::BITPACKING_GROUPING_SIZE - position_in_group;

				position_in_group = 0;
				current_group_ptr += skipping * current_width;

				// Update width pointer and load new width
				bitpacking_width_ptr -= sizeof(bitpacking_width_t);
				LoadCurrentBitWidth();

				skip_count -= skipping;
			}
		}
	}

	void LoadDecompressFunction() {
		decompress_function = &UnPackLemire<T>;
		return;

		//		if (NumericLimits<T>::IsSigned()) {
		//			switch (current_width) {
		//			case sizeof(int8_t):
		//				decompress_function = &UnpackSigned32<int8_t, T>;
		//				break;
		//			case sizeof(int16_t):
		//				decompress_function = &UnpackSigned32<int16_t, T>;
		//				break;
		//			case sizeof(int32_t):
		//				decompress_function = &UnpackSigned32<int32_t, T>;
		//				break;
		//			case sizeof(int64_t):
		//				decompress_function = &UnpackSigned32<int64_t, T>;
		//				break;
		//			default:
		//				throw InternalException("Incorrect bit width found in bitpacking");
		//			}
		//		} else {
		//			switch (current_width) {
		//			case sizeof(uint8_t):
		//				decompress_function = &UnpackUnsigned32<uint8_t, T>;
		//				break;
		//			case sizeof(uint16_t):
		//				decompress_function = &UnpackUnsigned32<uint16_t, T>;
		//				break;
		//			case sizeof(uint32_t):
		//				decompress_function = &UnpackUnsigned32<uint32_t, T>;
		//				break;
		//			case sizeof(uint64_t):
		//				decompress_function = &UnpackUnsigned32<uint64_t, T>;
		//				break;
		//			default:
		//				throw InternalException("Incorrect bit width found in bitpacking");
		//			}
		//		}
	}

	idx_t position_in_group = 0;
	data_ptr_t current_group_ptr;
	data_ptr_t bitpacking_width_ptr;
	bitpacking_width_t current_width;

	PhysicalType type;

	T decompress_buffer[BitpackingConstants::BITPACKING_ALGORITHM_GROUPING];
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

	idx_t scanned = 0;

	while (scanned < scan_count) {
		// Exhausted this group, move pointers to next group and load bitwidth for next group.
		if (scan_state.position_in_group >= BitpackingConstants::BITPACKING_GROUPING_SIZE) {
			scan_state.position_in_group = 0;
			scan_state.bitpacking_width_ptr -= sizeof(bitpacking_width_t);
			scan_state.current_group_ptr += scan_state.current_width * BitpackingConstants::BITPACKING_GROUPING_SIZE;
			scan_state.LoadCurrentBitWidth();
		}

		idx_t offset_in_compression_group =
		    scan_state.position_in_group % BitpackingConstants::BITPACKING_ALGORITHM_GROUPING;

		idx_t to_scan = MinValue<idx_t>(scan_count - scanned, BitpackingConstants::BITPACKING_ALGORITHM_GROUPING -
		                                                          offset_in_compression_group);

		// TODO we can optimize this to not use the decompression buffer if everything is aligned and we need the
		// whole group
		// TODO naming of compression group and width group is confusing

		// Calculate start of compression algorithm group
		data_ptr_t current_position_ptr =
		    scan_state.current_group_ptr + scan_state.position_in_group * scan_state.current_width;
		data_ptr_t decompression_group_start_pointer =
		    current_position_ptr - offset_in_compression_group * scan_state.current_width;

		// Decompress compression algorithm to buffer
		scan_state.decompress_function((data_ptr_t)scan_state.decompress_buffer, decompression_group_start_pointer,
		                               scan_state.current_width);

		// Copy decompressed result to vector
		T *current_result_ptr = result_data + result_offset + scanned;
		memcpy(current_result_ptr, scan_state.decompress_buffer + offset_in_compression_group, to_scan * sizeof(T));
		scanned += to_scan;
		scan_state.position_in_group += to_scan;
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
	scan_state.decompress_function((data_ptr_t)scan_state.decompress_buffer, scan_state.current_group_ptr,
	                               scan_state.current_width);
	memcpy(current_result_ptr, scan_state.decompress_buffer + result_idx, sizeof(T));
}
template <class T>
void BitpackingSkip(ColumnSegment &segment, ColumnScanState &state, idx_t skip_count) {
	auto &scan_state = (BitpackingScanState<T> &)*state.scan_state;
	scan_state.Skip(segment, skip_count);
}

//===--------------------------------------------------------------------===//
// Get Function
//===--------------------------------------------------------------------===//
template <class T>
CompressionFunction GetBitpackingFunction(PhysicalType data_type) {
	return CompressionFunction(CompressionType::COMPRESSION_BITPACKING, data_type, BitpackingInitAnalyze<T>,
	                           BitpackingAnalyze<T>, BitpackingFinalAnalyze<T>, BitpackingInitCompression<T>,
	                           BitpackingCompress<T>, BitpackingFinalizeCompress<T>, BitpackingInitScan<T>,
	                           BitpackingScan<T>, BitpackingScanPartial<T>, BitpackingFetchRow<T>, BitpackingSkip<T>);
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
