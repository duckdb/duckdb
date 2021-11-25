#include "bitpackinghelpers.h"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/function/compression/compression.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/statistics/numeric_statistics.hpp"
#include "duckdb/storage/table/column_data_checkpointer.hpp"
#include "duckdb/storage/table/column_segment.hpp"

#include <functional>

namespace duckdb {

using bitpacking_width_t = uint8_t;

static constexpr const idx_t BITPACKING_WIDTH_GROUP_SIZE = 1024;

class BitpackingPrimitives {
public:
	static constexpr const idx_t BITPACKING_ALGORITHM_GROUP_SIZE = 32;
	static constexpr const idx_t BITPACKING_HEADER_SIZE = sizeof(uint64_t);
	static constexpr const bool BYTE_ALIGNED = false;

	// Packs a block of BITPACKING_ALGORITHM_GROUP_SIZE values
	template <class T>
	inline static void PackBlock(data_ptr_t dst, T *src, bitpacking_width_t width) {
		return PackGroup<T>(dst, src, width);
	}

	// Unpacks a block of BITPACKING_ALGORITHM_GROUP_SIZE values
	template <class T>
	inline static void UnPackBlock(data_ptr_t dst, data_ptr_t src, bitpacking_width_t width) {
		return UnPackGroup<T>(dst, src, width);
	}

	// Calculates the minimum required number of bits per value that can store all values
	template <class T>
	inline static bitpacking_width_t MinimumBitWidth(T *values, idx_t count) {
		return FindMinimumBitWidth<T, BYTE_ALIGNED>(values, count);
	}

private:
	template <class T>
	static bitpacking_width_t MinimumBitWidth(T min_value, T max_value) {
		bitpacking_width_t required_bits;

		if (std::is_signed<T>::value) {
			if (min_value == NumericLimits<T>::Minimum()) {
				// handle special case of the minimal value, as it cannot be negated like all other values.
				return sizeof(T) * 8;
			} else {
				max_value = MaxValue(-min_value, max_value);
			}
		}

		if (max_value == 0) {
			return 0;
		}

		if (std::is_signed<T>::value) {
			required_bits = 1;
		} else {
			required_bits = 0;
		}

		while (max_value) {
			required_bits++;
			max_value >>= 1;
		}

		return required_bits;
	}

	template <class T, bool round_to_next_byte = false>
	static bitpacking_width_t FindMinimumBitWidth(T *values, idx_t count) {
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

		// Assert results are correct
#ifdef DEBUG
		if (calc_width < sizeof(T) * 8 && calc_width != 0) {
			if (std::is_signed<T>::value) {
				D_ASSERT((int64_t)max_value <= (int64_t)(1L << (calc_width - 1)) - 1);
				D_ASSERT((int64_t)min_value >= (int64_t)(-1 * ((1L << (calc_width - 1)) - 1) - 1));
			} else {
				D_ASSERT((int64_t)max_value <= (int64_t)(1L << (calc_width)) - 1);
			}
		}
#endif
		if (round_to_next_byte) {
			return (calc_width / 8 + (calc_width % 8 != 0)) * 8;
		} else {
			return calc_width;
		}
	}

	template <class T>
	static void UnPackGroup(data_ptr_t dst, data_ptr_t src, bitpacking_width_t width) {
		if (std::is_same<T, uint32_t>::value || std::is_same<T, int32_t>::value) {
			duckdb_fastpforlib::fastunpack((const uint32_t *)src, (uint32_t *)dst, (uint32_t)width);
		} else if (std::is_same<T, uint64_t>::value || std::is_same<T, int64_t>::value) {
			duckdb_fastpforlib::fastunpack((const uint32_t *)src, (uint64_t *)dst, (uint32_t)width);
		} else {
			throw InternalException("Unsupported type found in bitpacking.");
		}

		if (NumericLimits<T>::IsSigned() && width > 0) {
			SignExtend<T>(dst, width);
		}
	}

	// Sign bit extension
	// TODO check if we can implement faster algorithm, e.g. from
	// TODO http://graphics.stanford.edu/~seander/bithacks.html#FixedSignExtend
	template <class T, class T_U = typename std::make_unsigned<T>::type>
	static void SignExtend(data_ptr_t dst, bitpacking_width_t width) {
		idx_t shift = width - 1;
		for (idx_t i = 0; i < BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE; ++i) {
			T_U most_significant_compressed_bit = *((T_U *)dst + i) >> shift;
			D_ASSERT(most_significant_compressed_bit == 1 || most_significant_compressed_bit == 0);

			if (most_significant_compressed_bit == 1) {
				T_U mask = ((T_U)-1) << shift;
				*(T_U *)(dst + i * sizeof(T)) |= mask;
			}
		}
	}

	template <class T>
	static void PackGroup(data_ptr_t dst, T *values, bitpacking_width_t width) {
		// TODO: types smaller than 32bits
		if (std::is_same<T, uint32_t>::value || std::is_same<T, int32_t>::value) {
			duckdb_fastpforlib::fastpack((const uint32_t *)values, (uint32_t *)dst, (uint32_t)width);
		}
		if (std::is_same<T, uint64_t>::value || std::is_same<T, int64_t>::value) {
			duckdb_fastpforlib::fastpack((const uint64_t *)values, (uint32_t *)dst, (uint32_t)width);
		}
	}
};

struct EmptyBitpackingWriter {
	template <class T>
	static void Operation(T *values, bool *validity, bitpacking_width_t width, idx_t count, void *data_ptr) {
	}
};

template <class T, class PRE_CAST_TYPE>
struct BitpackingState {
	BitpackingState() : compression_buffer_idx(0), total_size(0), data_ptr(nullptr) {
	}

	PRE_CAST_TYPE compression_buffer[BITPACKING_WIDTH_GROUP_SIZE];
	bool compression_buffer_validity[BITPACKING_WIDTH_GROUP_SIZE];
	idx_t compression_buffer_idx;
	idx_t total_size;
	void *data_ptr;

public:
	template <class OP>
	void Flush() {
		bitpacking_width_t width =
		    BitpackingPrimitives::MinimumBitWidth<PRE_CAST_TYPE>(compression_buffer, compression_buffer_idx);
		OP::Operation(compression_buffer, compression_buffer_validity, width, compression_buffer_idx, data_ptr);
		total_size += (BITPACKING_WIDTH_GROUP_SIZE * width) / 8 + sizeof(bitpacking_width_t);
		compression_buffer_idx = 0;
	}

	template <class OP = EmptyBitpackingWriter>
	void Update(T *data, ValidityMask &validity, idx_t idx) {

		if (validity.RowIsValid(idx)) {
			compression_buffer_validity[compression_buffer_idx] = true;
			compression_buffer[compression_buffer_idx++] = (PRE_CAST_TYPE)data[idx];
		} else {
			// We write zero for easy bitwidth analysis of the compression buffer later
			compression_buffer_validity[compression_buffer_idx] = false;
			compression_buffer[compression_buffer_idx++] = 0;
		}

		if (compression_buffer_idx == BITPACKING_WIDTH_GROUP_SIZE) {
			// Calculate bitpacking width;
			Flush<OP>();
		}
	}
};

//===--------------------------------------------------------------------===//
// Analyze
//===--------------------------------------------------------------------===//
template <class T, class PRE_CAST_TYPE>
struct BitpackingAnalyzeState : public AnalyzeState {
	BitpackingState<T, PRE_CAST_TYPE> state;
};

template <class T, class PRE_CAST_TYPE>
unique_ptr<AnalyzeState> BitpackingInitAnalyze(ColumnData &col_data, PhysicalType type) {
	return make_unique<BitpackingAnalyzeState<T, PRE_CAST_TYPE>>();
}

template <class T, class PRE_CAST_TYPE>
bool BitpackingAnalyze(AnalyzeState &state, Vector &input, idx_t count) {
	auto &analyze_state = (BitpackingAnalyzeState<T, PRE_CAST_TYPE> &)state;
	VectorData vdata;
	input.Orrify(count, vdata);

	auto data = (T *)vdata.data;
	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);
		analyze_state.state.template Update<EmptyBitpackingWriter>(data, vdata.validity, idx);
	}

	return true;
}

template <class T, class PRE_CAST_TYPE>
idx_t BitpackingFinalAnalyze(AnalyzeState &state) {
	auto &bitpacking_state = (BitpackingAnalyzeState<T, PRE_CAST_TYPE> &)state;
	bitpacking_state.state.template Flush<EmptyBitpackingWriter>();
	return bitpacking_state.state.total_size;
}

//===--------------------------------------------------------------------===//
// Compress
//===--------------------------------------------------------------------===//
template <class T, class PRE_CAST_TYPE>
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
		static void Operation(VALUE_TYPE *values, bool *validity, bitpacking_width_t width, idx_t count,
		                      void *data_ptr) {
			auto state = (BitpackingCompressState<T, PRE_CAST_TYPE> *)data_ptr;

			if (state->RemainingSize() < (width * BITPACKING_WIDTH_GROUP_SIZE) / 8 + sizeof(bitpacking_width_t)) {
				// Segment is full
				auto row_start = state->current_segment->start + state->current_segment->count;
				state->FlushSegment();
				state->CreateEmptySegment(row_start);
			}

			for (idx_t i = 0; i < count; i++) {
				if (validity[i]) {
					NumericStatistics::Update<T>(state->current_segment->stats, values[i]);
				}
			}

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

		data_ptr = handle->Ptr() + current_segment->GetBlockOffset() + BitpackingPrimitives::BITPACKING_HEADER_SIZE;
		width_ptr =
		    handle->Ptr() + current_segment->GetBlockOffset() + Storage::BLOCK_SIZE - sizeof(bitpacking_width_t);
	}

	void Append(VectorData &vdata, idx_t count) {
		// TODO Optimization: avoid use of compression buffer if we can compress straight to result vector
		auto data = (T *)vdata.data;

		for (idx_t i = 0; i < count; i++) {
			auto idx = vdata.sel->get_index(i);
			state.template Update<BitpackingCompressState<T, PRE_CAST_TYPE>::BitpackingWriter>(data, vdata.validity,
			                                                                                   idx);
		}
	}

	void WriteValues(PRE_CAST_TYPE *values, bitpacking_width_t width, idx_t count) {
		// TODO we can optimize this by stopping early if count < BITPACKING_WIDTH_GROUP_SIZE
		idx_t compress_loops = BITPACKING_WIDTH_GROUP_SIZE / BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;
		for (idx_t i = 0; i < compress_loops; i++) {
			BitpackingPrimitives::PackBlock<PRE_CAST_TYPE>(
			    data_ptr, &values[i * BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE], width);
			data_ptr += (BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE * width) / 8;
		}

		Store<bitpacking_width_t>(width, width_ptr);
		width_ptr -= sizeof(bitpacking_width_t);

		current_segment->count += count;
	}

	void FlushSegment() {
		auto &state = checkpointer.GetCheckpointState();

		// Compact the segment by moving the widths next to the data.
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
		state.template Flush<BitpackingCompressState<T, PRE_CAST_TYPE>::BitpackingWriter>();
		FlushSegment();
		current_segment.reset();
	}

	ColumnDataCheckpointer &checkpointer;
	CompressionFunction *function;
	unique_ptr<ColumnSegment> current_segment;
	unique_ptr<BufferHandle> handle;

	// Ptr to next free spot in segment;
	data_ptr_t data_ptr;
	// Ptr to next free spot for storing bitwidths (growing downwards).
	data_ptr_t width_ptr;

	BitpackingState<T, PRE_CAST_TYPE> state;
};

template <class T, class PRE_CAST_TYPE>
unique_ptr<CompressionState> BitpackingInitCompression(ColumnDataCheckpointer &checkpointer,
                                                       unique_ptr<AnalyzeState> state) {
	return make_unique<BitpackingCompressState<T, PRE_CAST_TYPE>>(checkpointer);
}

template <class T, class PRE_CAST_TYPE>
void BitpackingCompress(CompressionState &state_p, Vector &scan_vector, idx_t count) {
	auto &state = (BitpackingCompressState<T, PRE_CAST_TYPE> &)state_p;
	VectorData vdata;
	scan_vector.Orrify(count, vdata);
	state.Append(vdata, count);
}

template <class T, class PRE_CAST_TYPE>
void BitpackingFinalizeCompress(CompressionState &state_p) {
	auto &state = (BitpackingCompressState<T, PRE_CAST_TYPE> &)state_p;
	state.Finalize();
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
template <class T, class PRE_CAST_TYPE>
struct BitpackingScanState : public SegmentScanState {
	unique_ptr<BufferHandle> handle;
	PhysicalType compress_type;

	void (*decompress_function)(data_ptr_t, data_ptr_t, bitpacking_width_t);

	explicit BitpackingScanState(ColumnSegment &segment) {
		auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
		handle = buffer_manager.Pin(segment.block);

		current_width_group_ptr =
		    handle->node->buffer + segment.GetBlockOffset() + BitpackingPrimitives::BITPACKING_HEADER_SIZE;

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
			if (position_in_group + skip_count < BITPACKING_WIDTH_GROUP_SIZE) {
				// We're not leaving this bitpacking group, we can perform all skips.
				position_in_group += skip_count;
				break;
			} else {
				// The skip crosses the current bitpacking group, we skip the remainder of this group.
				auto skipping = BITPACKING_WIDTH_GROUP_SIZE - position_in_group;
				position_in_group = 0;
				current_width_group_ptr += (current_width * BITPACKING_WIDTH_GROUP_SIZE) / 8;

				// Update width pointer and load new width
				bitpacking_width_ptr -= sizeof(bitpacking_width_t);
				LoadCurrentBitWidth();

				skip_count -= skipping;
			}
		}
	}

	void LoadDecompressFunction() {
		decompress_function = &BitpackingPrimitives::UnPackBlock<PRE_CAST_TYPE>;
		return;
	}

	idx_t position_in_group = 0;
	data_ptr_t current_width_group_ptr;
	data_ptr_t bitpacking_width_ptr;
	bitpacking_width_t current_width;

	PhysicalType type;

	PRE_CAST_TYPE decompression_buffer[BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE];
};

template <class T, class PRE_CAST_TYPE>
unique_ptr<SegmentScanState> BitpackingInitScan(ColumnSegment &segment) {
	auto result = make_unique<BitpackingScanState<T, PRE_CAST_TYPE>>(segment);
	return move(result);
}

//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//
template <class T, class PRE_CAST_TYPE>
void BitpackingScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
                           idx_t result_offset) {
	auto &scan_state = (BitpackingScanState<T, PRE_CAST_TYPE> &)*state.scan_state;

	T *result_data = FlatVector::GetData<T>(result);
	result.SetVectorType(VectorType::FLAT_VECTOR);

	idx_t scanned = 0;

	while (scanned < scan_count) {
		// Exhausted this width group, move pointers to next group and load bitwidth for next group.
		if (scan_state.position_in_group >= BITPACKING_WIDTH_GROUP_SIZE) {
			scan_state.position_in_group = 0;
			scan_state.bitpacking_width_ptr -= sizeof(bitpacking_width_t);
			scan_state.current_width_group_ptr += (scan_state.current_width * BITPACKING_WIDTH_GROUP_SIZE) / 8;
			scan_state.LoadCurrentBitWidth();
		}

		idx_t offset_in_compression_group =
		    scan_state.position_in_group % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;

		idx_t to_scan = MinValue<idx_t>(scan_count - scanned, BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE -
		                                                          offset_in_compression_group);

		// TODO Optimization: avoid use of decompression buffer if we can compress straight to result vector
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

		if (std::is_same<T, PRE_CAST_TYPE>::value) {
			memcpy(current_result_ptr, scan_state.decompression_buffer + offset_in_compression_group,
			       to_scan * sizeof(T));
		} else {
			for (idx_t i = 0; i < to_scan; i++) {
				current_result_ptr[i] =
				    (T) * ((PRE_CAST_TYPE *)(scan_state.decompression_buffer + offset_in_compression_group) + i);
			}
		}

		scanned += to_scan;
		scan_state.position_in_group += to_scan;
	}
}

template <class T, class PRE_CAST_TYPE>
void BitpackingScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result) {
	BitpackingScanPartial<T, PRE_CAST_TYPE>(segment, state, scan_count, result, 0);
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
template <class T, class PRE_CAST_TYPE>
void BitpackingFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result,
                        idx_t result_idx) {
	BitpackingScanState<T, PRE_CAST_TYPE> scan_state(segment);
	scan_state.Skip(segment, row_id);
	auto result_data = FlatVector::GetData<T>(result);
	T *current_result_ptr = result_data + result_idx;

	// Todo clean up, is reused in partialscan
	idx_t offset_in_compression_group =
	    scan_state.position_in_group % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;

	data_ptr_t decompression_group_start_pointer =
	    scan_state.current_width_group_ptr +
	    (scan_state.position_in_group - offset_in_compression_group) * scan_state.current_width / 8;

	scan_state.decompress_function((data_ptr_t)scan_state.decompression_buffer, decompression_group_start_pointer,
	                               scan_state.current_width);

	if (std::is_same<T, PRE_CAST_TYPE>::value) {
		*current_result_ptr = *(T *)(scan_state.decompression_buffer + offset_in_compression_group);
	} else {
		*current_result_ptr = (T) * (PRE_CAST_TYPE *)(scan_state.decompression_buffer + offset_in_compression_group);
	}
}
template <class T, class PRE_CAST_TYPE>
void BitpackingSkip(ColumnSegment &segment, ColumnScanState &state, idx_t skip_count) {
	auto &scan_state = (BitpackingScanState<T, PRE_CAST_TYPE> &)*state.scan_state;
	scan_state.Skip(segment, skip_count);
}

//===--------------------------------------------------------------------===//
// Get Function
//===--------------------------------------------------------------------===//
template <class T, class PRE_CAST_TYPE = T>
CompressionFunction GetBitpackingFunction(PhysicalType data_type) {
	return CompressionFunction(CompressionType::COMPRESSION_BITPACKING, data_type,
	                           BitpackingInitAnalyze<T, PRE_CAST_TYPE>, BitpackingAnalyze<T, PRE_CAST_TYPE>,
	                           BitpackingFinalAnalyze<T, PRE_CAST_TYPE>, BitpackingInitCompression<T, PRE_CAST_TYPE>,
	                           BitpackingCompress<T, PRE_CAST_TYPE>, BitpackingFinalizeCompress<T, PRE_CAST_TYPE>,
	                           BitpackingInitScan<T, PRE_CAST_TYPE>, BitpackingScan<T, PRE_CAST_TYPE>,
	                           BitpackingScanPartial<T, PRE_CAST_TYPE>, BitpackingFetchRow<T, PRE_CAST_TYPE>,
	                           BitpackingSkip<T, PRE_CAST_TYPE>);
}

CompressionFunction BitpackingFun::GetFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::INT8:
		return GetBitpackingFunction<int8_t, int32_t>(type);
	case PhysicalType::INT16:
		return GetBitpackingFunction<int16_t, int32_t>(type);
	case PhysicalType::INT32:
		return GetBitpackingFunction<int32_t>(type);
	case PhysicalType::INT64:
		return GetBitpackingFunction<int64_t>(type);
	case PhysicalType::UINT8:
		return GetBitpackingFunction<uint8_t, uint32_t>(type);
	case PhysicalType::UINT16:
		return GetBitpackingFunction<uint16_t, uint32_t>(type);
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
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
	case PhysicalType::UINT8:
	case PhysicalType::UINT16:
	case PhysicalType::UINT32:
	case PhysicalType::UINT64:
		return true;
	default:
		return false;
	}
}

} // namespace duckdb
