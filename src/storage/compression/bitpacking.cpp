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
#include <functional>

namespace duckdb {

using bitpacking_width_t = uint8_t;

struct BitpackingConstants {
	static constexpr const idx_t BITPACKING_HEADER_SIZE = sizeof(uint64_t);

	// Currently only standard vector size is a supported grouping size
	static constexpr const idx_t BITPACKING_GROUPING_SIZE = STANDARD_VECTOR_SIZE;

	// Needs to be a factor of BITPACKING_GROUPING_SIZE
	static constexpr const idx_t BITPACKING_ALGORITHM_GROUPING = 32;
};

class BitpackingPrimitives {
public:
	// Packs a block of BITPACKING_GROUPING_SIZE values
	template <class T>
	static void PackBlock(data_ptr_t dst, T* src, bitpacking_width_t width) {
		return PackValueGroupedLemire<T>(dst, src, width);
	}

	// Unpacks a block of BITPACKING_GROUPING_SIZE values
	template <class T>
	static void UnpackBlock(data_ptr_t dst, data_ptr_t src, bitpacking_width_t width) {
		return UnPackLemire<T>(dst, src, width);
	}

	// Calculates the minimum required number of bits per value that can store all values
	template <class T>
	static bitpacking_width_t GetMinimumBitWidth(T *values, idx_t count) {
		return FindMinBitWidth<T, false>(values, count);
	}

private:

	template <class T>
	static bitpacking_width_t MinimumBitWidth(T min_value, T max_value) {
		// TODO this function could be optimized most likely, it only runs once per BITPACKING_GROUP_SIZE though
		if (std::is_signed<T>::value) {
			bitpacking_width_t required_bits_min;
			bitpacking_width_t required_bits_max;

			// only consider the max value if its positive: else, the negative value is guaranteed to require more bits
			if (max_value >= 0) {
				required_bits_max = 1;
				while (max_value) {
					required_bits_max++;
					max_value >>= 1;
				}
			} else {
				required_bits_max = 0;
			}

			// only consider the min value if its negative: else, the positive value is guaranteed to require more bits
			if (min_value < 0) {
				typename std::make_unsigned<T>::type min_value_cast;
				typename std::make_unsigned<T>::type min_value_mask;

				Store<T>(min_value, (data_ptr_t)&min_value_cast);
				Store<T>(NumericLimits<T>::Minimum(), (data_ptr_t)&min_value_mask);
				required_bits_min = sizeof(T) * 8;
				while ((min_value_cast <<= 1) & min_value_mask) {
					required_bits_min--;
				}
				return MaxValue<bitpacking_width_t>(required_bits_min, required_bits_max);
			} else {
				return required_bits_max;
			}
		} else {
			bitpacking_width_t required_bits = 0;
			while (max_value) {
				required_bits++;
				max_value >>= 1;
			}
			return required_bits;
		}
	}

	template <class T, bool round_to_next_byte = false>
	static bitpacking_width_t FindMinBitWidth(T *values, idx_t count) {
		T min_value = values[0];
		T max_value = values[0];

		for (idx_t i = 1; i < count; i++) {
			if (values[i] > max_value) {
				max_value = values[i];
			}

			if (std::is_signed<T>::value) {
				if (values[i] < min_value) {
					min_value = values[i];
				}
			}
		}

		bitpacking_width_t calc_width = MinimumBitWidth<T>(std::is_signed<T>::value ? min_value : 0, max_value);

		if (round_to_next_byte) {
			return (calc_width / 8 + (calc_width % 8 != 0)) * 8;
		} else {
			return calc_width;
		}
	}

	template <class T, class T_U = typename std::make_unsigned<T>::type>
	static void UnPackLemire(data_ptr_t dst, data_ptr_t src, bitpacking_width_t width) {
		if (std::is_same<T, uint32_t>::value || std::is_same<T, int32_t>::value) {
			FastPForLib::unpackblock<BitpackingConstants::BITPACKING_ALGORITHM_GROUPING, uint32_t>(
			    (const uint32_t *)src, (uint32_t *)dst, (uint32_t)width);
		}
		else if (std::is_same<T, uint64_t>::value || std::is_same<T, int64_t>::value) {
			FastPForLib::unpackblock<BitpackingConstants::BITPACKING_ALGORITHM_GROUPING, uint64_t>(
			    (const uint32_t *)src, (uint64_t *)dst, (uint32_t)width);
		} else {
			throw InternalException("Unsupported type found in bitpacking.");
		}

		// Sign bit extension
		// TODO check if we can implement faster algorithm, e.g. from http://graphics.stanford.edu/~seander/bithacks.html#FixedSignExtend
		if (NumericLimits<T>::IsSigned() && width > 0) {
			idx_t shift = width - 1;
			for (idx_t i = 0; i < BitpackingConstants::BITPACKING_ALGORITHM_GROUPING; ++i) {
				T_U most_significant_compressed_bit = *((T_U *)dst + i) >> shift;
				D_ASSERT(most_significant_compressed_bit == 1 || most_significant_compressed_bit == 0);

				if (most_significant_compressed_bit == 1) {
					T_U mask = ((T_U)-1) << shift;
					*(T_U *)(dst + i * sizeof(T)) |= mask;
				}
			}
		}
	}

	template <class T>
	static void PackValueGroupedLemire(data_ptr_t dst, T *values, bitpacking_width_t width) {
		// TODO: types smaller than 32bits
		if (std::is_same<T, uint32_t>::value || std::is_same<T, int32_t>::value) {
			FastPForLib::packblockup<32, uint32_t>((const uint32_t *)values, (uint32_t *)dst, (uint32_t)width);
		}
		if (std::is_same<T, uint64_t>::value || std::is_same<T, int64_t>::value) {
			FastPForLib::packblockup<32, uint64_t>((const uint64_t *)values, (uint32_t *)dst, (uint32_t)width);
		}
	}
};

struct EmptyBitpackingWriter {
	template <class T>
	static void Operation(T *values, bitpacking_width_t width, idx_t count, void *data_ptr) {
	}
};

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
		bitpacking_width_t width = BitpackingPrimitives::GetMinimumBitWidth<T>(compression_buffer, compression_buffer_idx);
		OP::Operation(compression_buffer, width, compression_buffer_idx, data_ptr);
		total_size += (compression_buffer_size * width) / 8 + sizeof(bitpacking_width_t);
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

	// TODO use Bitpacking state for this
	bitpacking_width_t bitwidth = BitpackingPrimitives::GetMinimumBitWidth<T>((T *)vdata.data, count);
	bitpacking_state.total_size += (idx_t)(bitwidth * count) / 8 + 1 + sizeof(bitpacking_width_t);
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

		if (RemainingSize() <
		    (width * BitpackingConstants::BITPACKING_GROUPING_SIZE) / 8 + sizeof(bitpacking_width_t)) {
			// Segment is full
			auto row_start = current_segment->start + current_segment->count;
			FlushSegment();
			CreateEmptySegment(row_start);
		}

		// Todo we might not need to do the whole thing?
		idx_t compress_loops =
		    BitpackingConstants::BITPACKING_GROUPING_SIZE / BitpackingConstants::BITPACKING_ALGORITHM_GROUPING;
		for (idx_t i = 0; i < compress_loops; i++) {
			BitpackingPrimitives::PackBlock<T>(data_ptr, &values[i * BitpackingConstants::BITPACKING_ALGORITHM_GROUPING], width);
			data_ptr += (BitpackingConstants::BITPACKING_ALGORITHM_GROUPING * width) / 8;
		}

		Store<bitpacking_width_t>(width, width_ptr);
		width_ptr -= sizeof(bitpacking_width_t);

		current_segment->count += count;
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

template <class T>
struct BitpackingScanState : public SegmentScanState {
	unique_ptr<BufferHandle> handle;
	PhysicalType compress_type;

	void (*decompress_function)(data_ptr_t, data_ptr_t, bitpacking_width_t);

	explicit BitpackingScanState(ColumnSegment &segment) {
		auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
		handle = buffer_manager.Pin(segment.block);

		current_width_group_ptr =
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
				position_in_group += skip_count;
				break;

			} else {
				// The skip crosses the current bitpacking group, we skip the remainder of this group.
				auto skipping = BitpackingConstants::BITPACKING_GROUPING_SIZE - position_in_group;

				position_in_group = 0;
				current_width_group_ptr += (current_width * BitpackingConstants::BITPACKING_GROUPING_SIZE) / 8;

				// Update width pointer and load new width
				bitpacking_width_ptr -= sizeof(bitpacking_width_t);
				LoadCurrentBitWidth();

				skip_count -= skipping;
			}
		}
	}

	void LoadDecompressFunction() {
		decompress_function = &BitpackingPrimitives::UnpackBlock<T>;
		return;
	}

	idx_t position_in_group = 0;
	data_ptr_t current_width_group_ptr;
	data_ptr_t bitpacking_width_ptr;
	bitpacking_width_t current_width;

	PhysicalType type;

	T decompression_buffer[BitpackingConstants::BITPACKING_ALGORITHM_GROUPING];
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
		// Exhausted this width group, move pointers to next group and load bitwidth for next group.
		if (scan_state.position_in_group >= BitpackingConstants::BITPACKING_GROUPING_SIZE) {
			scan_state.position_in_group = 0;
			scan_state.bitpacking_width_ptr -= sizeof(bitpacking_width_t);
			scan_state.current_width_group_ptr +=
			    (scan_state.current_width * BitpackingConstants::BITPACKING_GROUPING_SIZE) / 8;
			scan_state.LoadCurrentBitWidth();
		}

		idx_t offset_in_compression_group =
		    scan_state.position_in_group % BitpackingConstants::BITPACKING_ALGORITHM_GROUPING;

		idx_t to_scan = MinValue<idx_t>(scan_count - scanned, BitpackingConstants::BITPACKING_ALGORITHM_GROUPING -
		                                                          offset_in_compression_group);

		// TODO can be optimized to not use the decompression buffer
		// Calculate start of compression algorithm group
		data_ptr_t current_position_ptr =
		    scan_state.current_width_group_ptr + scan_state.position_in_group * scan_state.current_width / 8;
		data_ptr_t decompression_group_start_pointer =
		    current_position_ptr - offset_in_compression_group * scan_state.current_width / 8;

		// Decompress compression algorithm to buffer
		scan_state.decompress_function((data_ptr_t)scan_state.decompression_buffer, decompression_group_start_pointer,
		                               scan_state.current_width);

		// Copy decompressed result to vector
		T *current_result_ptr = result_data + result_offset + scanned;
		memcpy(current_result_ptr, scan_state.decompression_buffer + offset_in_compression_group, to_scan * sizeof(T));
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

	// Todo clean up, is reused in partialscan
	idx_t offset_in_compression_group =
	    scan_state.position_in_group % BitpackingConstants::BITPACKING_ALGORITHM_GROUPING;

	data_ptr_t decompression_group_start_pointer =
	    scan_state.current_width_group_ptr +
	    (scan_state.position_in_group - offset_in_compression_group) * scan_state.current_width / 8;

	scan_state.decompress_function((data_ptr_t)scan_state.decompression_buffer, decompression_group_start_pointer,
	                               scan_state.current_width);
	memcpy(current_result_ptr, scan_state.decompression_buffer + offset_in_compression_group, sizeof(T));
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
//	case PhysicalType::INT8:
//		return GetBitpackingFunction<int8_t>(type);
//	case PhysicalType::INT16:
//		return GetBitpackingFunction<int16_t>(type);
	case PhysicalType::INT32:
		return GetBitpackingFunction<int32_t>(type);
	case PhysicalType::INT64:
		return GetBitpackingFunction<int64_t>(type);
//	case PhysicalType::UINT8:
//		return GetBitpackingFunction<uint8_t>(type);
//	case PhysicalType::UINT16:
//		return GetBitpackingFunction<uint16_t>(type);
	case PhysicalType::UINT32:
		return GetBitpackingFunction<uint32_t>(type);
	case PhysicalType::UINT64:
		return GetBitpackingFunction<uint64_t>(type);
	default:
		throw InternalException("Unsupported type for Bitpacking");
	}
}

bool BitpackingFun::TypeIsSupported(PhysicalType type) {
	switch (type) {
//	case PhysicalType::INT8:
//	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
//	case PhysicalType::UINT8:
//	case PhysicalType::UINT16:
	case PhysicalType::UINT32:
	case PhysicalType::UINT64:
		return true;
	default:
		return false;
	}
}

} // namespace duckdb
