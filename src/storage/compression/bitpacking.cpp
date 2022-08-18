#include "duckdb/common/bitpacking.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/function/compression/compression.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/statistics/numeric_statistics.hpp"
#include "duckdb/storage/table/column_data_checkpointer.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/common/operator/subtract.hpp"

#include <functional>

namespace duckdb {

// Note that optimizations in scanning only work if this value is equal to STANDARD_VECTOR_SIZE, however we keep them
// separated to prevent the code from break on lower vector sizes
static constexpr const idx_t BITPACKING_METADATA_GROUP_SIZE = 1024;

struct EmptyBitpackingWriter {
	template <class T>
	static void Operation(T *values, bool *validity, bitpacking_width_t width, T frame_of_reference, idx_t count,
	                      void *data_ptr) {
	}
};

template <class T>
struct BitpackingState {
public:
	BitpackingState() : compression_buffer_idx(0), total_size(0), data_ptr(nullptr) {
		ResetMinMax();
	}

	T compression_buffer[BITPACKING_METADATA_GROUP_SIZE];
	bool compression_buffer_validity[BITPACKING_METADATA_GROUP_SIZE];
	idx_t compression_buffer_idx;
	idx_t total_size;
	void *data_ptr;

	bool min_max_set;
	T minimum;
	T maximum;

public:
	void SubtractFrameOfReference(const T &frame_of_reference) {
		for (idx_t i = 0; i < compression_buffer_idx; i++) {
			compression_buffer[i] -= frame_of_reference;
		}
	}

	void ResetMinMax() {
		min_max_set = false;
		//! We set these to 0, in case all values are NULL, in which case the min and max will never be set.
		minimum = 0;
		maximum = 0;
	}

	bool TryUpdateMinMax(T value) {
		bool updated = false;
		if (!min_max_set || value < minimum) {
			minimum = value;
			updated = true;
		}
		if (!min_max_set || value > maximum) {
			maximum = value;
			updated = true;
		}
		min_max_set = min_max_set || updated;
		//! Only when either of the values are updated, do we need to test the overflow
		if (updated) {
			T ignore;
			return TrySubtractOperator::Operation(maximum, minimum, ignore);
		}
		return true;
	}

	T GetFrameOfReference() {
		return minimum;
	}
	T Maximum() {
		return maximum;
	}

	template <class OP, class T_U = typename std::make_unsigned<T>::type>
	void Flush() {
		T frame_of_reference = GetFrameOfReference();
		SubtractFrameOfReference(frame_of_reference);

		//! Because of FOR, we can guarantee that all values are positive
		T_U adjusted_maximum = T_U(Maximum() - frame_of_reference);

		bitpacking_width_t width = BitpackingPrimitives::MinimumBitWidth<T_U>((T_U)0, adjusted_maximum);
		OP::template Operation<T>(compression_buffer, compression_buffer_validity, width, frame_of_reference,
		                          compression_buffer_idx, data_ptr);
		total_size += (BITPACKING_METADATA_GROUP_SIZE * width) / 8 + sizeof(bitpacking_width_t) + sizeof(T);
		compression_buffer_idx = 0;
		ResetMinMax();
	}

	template <class OP = EmptyBitpackingWriter>
	bool Update(T *data, ValidityMask &validity, idx_t idx) {

		if (validity.RowIsValid(idx)) {
			compression_buffer_validity[compression_buffer_idx] = true;
			compression_buffer[compression_buffer_idx++] = data[idx];
			if (!TryUpdateMinMax(data[idx])) {
				return false;
			}
		} else {
			// We write zero for easy bitwidth analysis of the compression buffer later
			compression_buffer_validity[compression_buffer_idx] = false;
			compression_buffer[compression_buffer_idx++] = 0;
		}

		if (compression_buffer_idx == BITPACKING_METADATA_GROUP_SIZE) {
			// Calculate bitpacking width;
			Flush<OP>();
		}
		return true;
	}
};

//===--------------------------------------------------------------------===//
// Analyze
//===--------------------------------------------------------------------===//
template <class T>
struct BitpackingAnalyzeState : public AnalyzeState {
	BitpackingState<T> state;
};

template <class T>
unique_ptr<AnalyzeState> BitpackingInitAnalyze(ColumnData &col_data, PhysicalType type) {
	return make_unique<BitpackingAnalyzeState<T>>();
}

template <class T>
bool BitpackingAnalyze(AnalyzeState &state, Vector &input, idx_t count) {
	auto &analyze_state = (BitpackingAnalyzeState<T> &)state;
	UnifiedVectorFormat vdata;
	input.ToUnifiedFormat(count, vdata);

	auto data = (T *)vdata.data;
	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);
		if (!analyze_state.state.template Update<EmptyBitpackingWriter>(data, vdata.validity, idx)) {
			return false;
		}
	}
	return true;
}

template <class T>
idx_t BitpackingFinalAnalyze(AnalyzeState &state) {
	auto &bitpacking_state = (BitpackingAnalyzeState<T> &)state;
	bitpacking_state.state.template Flush<EmptyBitpackingWriter>();
	return bitpacking_state.state.total_size;
}

//===--------------------------------------------------------------------===//
// Compress
//===--------------------------------------------------------------------===//
template <class T>
struct BitpackingCompressState : public CompressionState {
public:
	explicit BitpackingCompressState(ColumnDataCheckpointer &checkpointer) : checkpointer(checkpointer) {
		auto &db = checkpointer.GetDatabase();
		auto &type = checkpointer.GetType();
		auto &config = DBConfig::GetConfig(db);
		function = config.GetCompressionFunction(CompressionType::COMPRESSION_BITPACKING, type.InternalType());
		CreateEmptySegment(checkpointer.GetRowGroup().start);

		state.data_ptr = (void *)this;
	}

	ColumnDataCheckpointer &checkpointer;
	CompressionFunction *function;
	unique_ptr<ColumnSegment> current_segment;
	BufferHandle handle;

	// Ptr to next free spot in segment;
	data_ptr_t data_ptr;
	// Ptr to next free spot for storing bitwidths and frame-of-references (growing downwards).
	data_ptr_t metadata_ptr;

	BitpackingState<T> state;

public:
	struct BitpackingWriter {

		template <class VALUE_TYPE>
		static void Operation(VALUE_TYPE *values, bool *validity, bitpacking_width_t width,
		                      VALUE_TYPE frame_of_reference, idx_t count, void *data_ptr) {
			auto state = (BitpackingCompressState<T> *)data_ptr;
			auto total_bits_needed = (width * BITPACKING_METADATA_GROUP_SIZE);
			D_ASSERT(total_bits_needed % 8 == 0);
			auto total_bytes_needed = total_bits_needed / 8;
			total_bytes_needed += sizeof(bitpacking_width_t);
			total_bytes_needed += sizeof(VALUE_TYPE);

			if (state->RemainingSize() < total_bytes_needed) {
				// Segment is full
				auto row_start = state->current_segment->start + state->current_segment->count;
				state->FlushSegment();
				state->CreateEmptySegment(row_start);
			}

			for (idx_t i = 0; i < count; i++) {
				if (validity[i]) {
					NumericStatistics::Update<T>(state->current_segment->stats, values[i] + frame_of_reference);
				}
			}

			state->WriteValues(values, width, frame_of_reference, count);
		}
	};

	// Space remaining between the metadata_ptr growing down and data ptr growing up
	idx_t RemainingSize() {
		return metadata_ptr - data_ptr;
	}

	void CreateEmptySegment(idx_t row_start) {
		auto &db = checkpointer.GetDatabase();
		auto &type = checkpointer.GetType();
		auto compressed_segment = ColumnSegment::CreateTransientSegment(db, type, row_start);
		compressed_segment->function = function;
		current_segment = move(compressed_segment);
		auto &buffer_manager = BufferManager::GetBufferManager(db);
		handle = buffer_manager.Pin(current_segment->block);

		data_ptr = handle.Ptr() + current_segment->GetBlockOffset() + BitpackingPrimitives::BITPACKING_HEADER_SIZE;
		metadata_ptr =
		    handle.Ptr() + current_segment->GetBlockOffset() + Storage::BLOCK_SIZE - sizeof(bitpacking_width_t);
	}

	void Append(UnifiedVectorFormat &vdata, idx_t count) {
		// TODO Optimization: avoid use of compression buffer if we can compress straight to result vector
		auto data = (T *)vdata.data;

		for (idx_t i = 0; i < count; i++) {
			auto idx = vdata.sel->get_index(i);
			state.template Update<BitpackingCompressState<T>::BitpackingWriter>(data, vdata.validity, idx);
		}
	}

	void WriteValues(T *values, bitpacking_width_t width, T frame_of_reference, idx_t count) {
		// TODO we can optimize this by stopping early if count < BITPACKING_METADATA_GROUP_SIZE
		BitpackingPrimitives::PackBuffer<T, false>(data_ptr, values, count, width);
		data_ptr += (BITPACKING_METADATA_GROUP_SIZE * width) / 8;

		Store<bitpacking_width_t>(width, metadata_ptr);
		metadata_ptr -= sizeof(T);
		Store<T>(frame_of_reference, metadata_ptr);
		metadata_ptr -= sizeof(bitpacking_width_t);

		current_segment->count += count;
	}

	void FlushSegment() {
		auto &state = checkpointer.GetCheckpointState();
		auto dataptr = handle.Ptr();

		// Compact the segment by moving the metadata next to the data.
		idx_t metadata_offset = AlignValue(data_ptr - dataptr);
		idx_t metadata_size = dataptr + Storage::BLOCK_SIZE - metadata_ptr - 1;
		idx_t total_segment_size = metadata_offset + metadata_size;
		memmove(dataptr + metadata_offset, metadata_ptr + 1, metadata_size);

		// Store the offset of the metadata of the first group (which is at the highest address).
		Store<idx_t>(metadata_offset + metadata_size - 1, dataptr);
		handle.Destroy();

		state.FlushSegment(move(current_segment), total_segment_size);
	}

	void Finalize() {
		state.template Flush<BitpackingCompressState<T>::BitpackingWriter>();
		FlushSegment();
		current_segment.reset();
	}
};

template <class T>
unique_ptr<CompressionState> BitpackingInitCompression(ColumnDataCheckpointer &checkpointer,
                                                       unique_ptr<AnalyzeState> state) {
	return make_unique<BitpackingCompressState<T>>(checkpointer);
}

template <class T>
void BitpackingCompress(CompressionState &state_p, Vector &scan_vector, idx_t count) {
	auto &state = (BitpackingCompressState<T> &)state_p;
	UnifiedVectorFormat vdata;
	scan_vector.ToUnifiedFormat(count, vdata);
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
public:
	explicit BitpackingScanState(ColumnSegment &segment) {
		auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
		handle = buffer_manager.Pin(segment.block);
		auto dataptr = handle.Ptr();
		current_metadata_group_ptr = dataptr + segment.GetBlockOffset() + BitpackingPrimitives::BITPACKING_HEADER_SIZE;

		// load offset to bitpacking widths pointer
		auto bitpacking_metadata_offset = Load<idx_t>(dataptr + segment.GetBlockOffset());
		bitpacking_metadata_ptr = dataptr + segment.GetBlockOffset() + bitpacking_metadata_offset;

		// load the metadata of the first vector
		LoadCurrentMetaData();
	}

	BufferHandle handle;

	void (*decompress_function)(data_ptr_t, data_ptr_t, bitpacking_width_t, bool skip_sign_extension);
	T decompression_buffer[BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE];

	idx_t position_in_group = 0;
	data_ptr_t current_metadata_group_ptr;
	data_ptr_t bitpacking_metadata_ptr;
	bitpacking_width_t current_width;
	T current_frame_of_reference;

public:
	//! Loads the current group header, and sets pointer to next header
	void LoadCurrentMetaData() {
		D_ASSERT(bitpacking_metadata_ptr > handle.Ptr() &&
		         bitpacking_metadata_ptr < handle.Ptr() + Storage::BLOCK_SIZE);
		current_width = Load<bitpacking_width_t>(bitpacking_metadata_ptr);
		bitpacking_metadata_ptr -= sizeof(T);
		current_frame_of_reference = Load<T>(bitpacking_metadata_ptr);
		bitpacking_metadata_ptr -= sizeof(bitpacking_width_t);
		LoadDecompressFunction();
	}

	void Skip(ColumnSegment &segment, idx_t skip_count) {
		while (skip_count > 0) {
			if (position_in_group + skip_count < BITPACKING_METADATA_GROUP_SIZE) {
				// We're not leaving this bitpacking group, we can perform all skips.
				position_in_group += skip_count;
				break;
			} else {
				// The skip crosses the current bitpacking group, we skip the remainder of this group.
				auto skipping = BITPACKING_METADATA_GROUP_SIZE - position_in_group;
				position_in_group = 0;
				current_metadata_group_ptr += (current_width * BITPACKING_METADATA_GROUP_SIZE) / 8;

				// Load new width
				LoadCurrentMetaData();

				skip_count -= skipping;
			}
		}
	}

	void LoadDecompressFunction() {
		decompress_function = &BitpackingPrimitives::UnPackBlock<T>;
	}
};

template <class T>
unique_ptr<SegmentScanState> BitpackingInitScan(ColumnSegment &segment) {
	auto result = make_unique<BitpackingScanState<T>>(segment);
	return move(result);
}

template <class T>
static void ApplyFrameOfReference(T *dst, T frame_of_reference, idx_t size) {
	if (!frame_of_reference) {
		return;
	}
	for (idx_t i = 0; i < size; i++) {
		dst[i] += frame_of_reference;
	}
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

	// Fast path for when no compression was used, we can do a single memcopy
	if (STANDARD_VECTOR_SIZE == BITPACKING_METADATA_GROUP_SIZE) {
		if (scan_state.current_frame_of_reference == 0 && scan_state.current_width == sizeof(T) * 8 &&
		    scan_count <= BITPACKING_METADATA_GROUP_SIZE && scan_state.position_in_group == 0) {

			memcpy(result_data + result_offset, scan_state.current_metadata_group_ptr, scan_count * sizeof(T));
			scan_state.current_metadata_group_ptr += scan_count * sizeof(T);
			scan_state.LoadCurrentMetaData();
			return;
		}
	}

	//! Because FOR offsets all our values to be 0 or above, we can always skip sign extension here
	bool skip_sign_extend = true;

	idx_t scanned = 0;

	while (scanned < scan_count) {
		// Exhausted this metadata group, move pointers to next group and load metadata for next group.
		if (scan_state.position_in_group >= BITPACKING_METADATA_GROUP_SIZE) {
			scan_state.position_in_group = 0;
			scan_state.current_metadata_group_ptr += (scan_state.current_width * BITPACKING_METADATA_GROUP_SIZE) / 8;
			scan_state.LoadCurrentMetaData();
		}

		idx_t offset_in_compression_group =
		    scan_state.position_in_group % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;

		idx_t to_scan = MinValue<idx_t>(scan_count - scanned, BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE -
		                                                          offset_in_compression_group);

		// Calculate start of compression algorithm group
		data_ptr_t current_position_ptr =
		    scan_state.current_metadata_group_ptr + scan_state.position_in_group * scan_state.current_width / 8;
		data_ptr_t decompression_group_start_pointer =
		    current_position_ptr - offset_in_compression_group * scan_state.current_width / 8;

		T *current_result_ptr = result_data + result_offset + scanned;

		if (to_scan == BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE && offset_in_compression_group == 0) {
			// Decompress directly into result vector
			scan_state.decompress_function((data_ptr_t)current_result_ptr, decompression_group_start_pointer,
			                               scan_state.current_width, skip_sign_extend);
		} else {
			// Decompress compression algorithm to buffer
			scan_state.decompress_function((data_ptr_t)scan_state.decompression_buffer,
			                               decompression_group_start_pointer, scan_state.current_width,
			                               skip_sign_extend);

			memcpy(current_result_ptr, scan_state.decompression_buffer + offset_in_compression_group,
			       to_scan * sizeof(T));
		}
		ApplyFrameOfReference((T *)current_result_ptr, scan_state.current_frame_of_reference, to_scan);
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

	// TODO clean up, is reused in partialscan
	idx_t offset_in_compression_group =
	    scan_state.position_in_group % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;

	data_ptr_t decompression_group_start_pointer =
	    scan_state.current_metadata_group_ptr +
	    (scan_state.position_in_group - offset_in_compression_group) * scan_state.current_width / 8;

	//! Because FOR offsets all our values to be 0 or above, we can always skip sign extension here
	bool skip_sign_extend = true;

	scan_state.decompress_function((data_ptr_t)scan_state.decompression_buffer, decompression_group_start_pointer,
	                               scan_state.current_width, skip_sign_extend);

	*current_result_ptr = *(T *)(scan_state.decompression_buffer + offset_in_compression_group);
	//! Apply FOR to result
	*current_result_ptr += scan_state.current_frame_of_reference;
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
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return GetBitpackingFunction<int8_t>(type);
	case PhysicalType::INT16:
		return GetBitpackingFunction<int16_t>(type);
	case PhysicalType::INT32:
		return GetBitpackingFunction<int32_t>(type);
	case PhysicalType::INT64:
		return GetBitpackingFunction<int64_t>(type);
	case PhysicalType::UINT8:
		return GetBitpackingFunction<uint8_t>(type);
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
	switch (type) {
	case PhysicalType::BOOL:
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
