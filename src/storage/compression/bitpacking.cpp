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
#include "duckdb/storage/compression/bitpacking.hpp"

#include <functional>
#include <iostream>

namespace duckdb {

static constexpr const idx_t BITPACKING_METADATA_GROUP_SIZE = STANDARD_VECTOR_SIZE > 512 ? STANDARD_VECTOR_SIZE : 2048;

BitpackingMode BitpackingModeFromString(const string &str) {
	auto mode = StringUtil::Lower(str);

	if (mode == "auto") {
		return BitpackingMode::AUTO;
	} else if (mode == "constant") {
		return BitpackingMode::CONSTANT;
	} else if (mode == "constant_delta") {
		return BitpackingMode::CONSTANT_DELTA;
	} else if (mode == "delta_for") {
		return BitpackingMode::DELTA_FOR;
	} else if (mode == "for") {
		return BitpackingMode::FOR;
	} else {
		return BitpackingMode::AUTO;
	}
}

string BitpackingModeToString(const BitpackingMode &mode) {
	switch(mode) {
	case (BitpackingMode::AUTO):
		return "auto";
	case (BitpackingMode::CONSTANT):
		return "constant";
	case (BitpackingMode::CONSTANT_DELTA):
		return "constant_delta";
	case (BitpackingMode::DELTA_FOR):
		return "delta_for";
	case (BitpackingMode::FOR):
		return "for";
	}
}

struct EmptyBitpackingWriter {
	template <class T>
	static void WriteConstant(T constant, idx_t count, void* data_ptr, bool all_invalid) {}
	template <class T, class T_S=typename std::make_signed<T>::type>
	static void WriteConstantDelta(T_S constant, T frame_of_reference, idx_t count, T* values, bool* validity, void* data_ptr) {}
	template <class T, class T_S=typename std::make_signed<T>::type>
	static void WriteDeltaFor(T *values, bool *validity, bitpacking_width_t width, T frame_of_reference, T_S delta_offset, T* original_values, idx_t count, void *data_ptr) {}
	template <class T>
	static void WriteFor(T *values, bool *validity, bitpacking_width_t width, T frame_of_reference, idx_t count, void *data_ptr) {}
};

typedef struct {
	BitpackingMode mode;
	uint32_t offset;
} bitpacking_metadata_t;

typedef uint32_t bitpacking_metadata_encoded_t;
static bitpacking_metadata_encoded_t encode_meta(bitpacking_metadata_t metadata) {
	D_ASSERT(metadata.offset <= 16777215); // max uint24_t
    bitpacking_metadata_encoded_t encoded_value = metadata.offset;
	encoded_value |= (uint8_t)metadata.mode << 24;
	return encoded_value;
}
static bitpacking_metadata_t decode_meta(bitpacking_metadata_encoded_t metadata_encoded) {
	bitpacking_metadata_t metadata;
	metadata.mode = Load<BitpackingMode>((data_ptr_t)(&metadata_encoded)+3);
	metadata.offset = metadata_encoded &= 0x00FFFFFF;
	return metadata;
}

template <class T, class T_U = typename std::make_unsigned<T>::type, class T_S = typename std::make_signed<T>::type>
struct BitpackingState {
public:
	BitpackingState() : compression_buffer_idx(0), total_size(0), data_ptr(nullptr) {
		compression_buffer_internal[0] = (T)0;
		compression_buffer = &compression_buffer_internal[1];
		Reset();
	}

	// this allows -1 index of compression_buffer to be 0 for easy delta encoding
	T compression_buffer_internal[BITPACKING_METADATA_GROUP_SIZE+1];
	T* compression_buffer;
	T_S delta_buffer[BITPACKING_METADATA_GROUP_SIZE];
	bool compression_buffer_validity[BITPACKING_METADATA_GROUP_SIZE];

	idx_t compression_buffer_idx;
	idx_t total_size;
	void *data_ptr;

	T minimum;
	T maximum;
	T min_max_diff;
	T_S minimum_delta;
	T_S maximum_delta;
	T_S min_max_delta_diff;
	T_S delta_offset;
	bool all_valid;
	bool all_invalid;
	bool can_do_delta;
	bool can_do_for;

	// Used to force a specific mode, useful in testing
	// Note that forcing CONSTANT_DELTA or CONSTANT does not actually do anything: if we can use those they are chosen.
	BitpackingMode mode = BitpackingMode::AUTO;

public:
	void Reset() {
		minimum = NumericLimits<T>::Maximum();
		minimum_delta = NumericLimits<T_S>::Maximum();
		maximum = NumericLimits<T>::Minimum();
		maximum_delta = NumericLimits<T_S>::Minimum();
		delta_offset = 0;
		all_valid = true;
		all_invalid = true;
		can_do_delta = false;
		can_do_for = false;
		compression_buffer_idx = 0;
		min_max_diff = 0;
		min_max_delta_diff =0;
	}

	void CalculateFORStats() {
		can_do_for = TrySubtractOperator::Operation(maximum, minimum, min_max_diff);
	}

	void CalculateDeltaStats() {
		T_S limit1 = NumericLimits<T_S>::Maximum();
		T limit = (T)limit1;

		// TODO: currently we dont support delta compression of values above NumericLimits<T_S>::Maximum(),
		// 		 we could support this with some clever substract trickery?
		if (maximum > limit){
			return;
		}

		D_ASSERT(compression_buffer_idx >= 2);

		// TODO: handle NULLS here?
		// Currently we cannot handle nulls because we would need an additional step of patching for this.
		// we could for example copy the last value on a null insert. This would help a bit, but not be optimal for large deltas
		// since theres suddenly a zero then. Ideally we would insert a value that leads to a delta within the current domain of deltas
		// however we dont know that domain here yet
		if (!all_valid) {
			return;
		}

		for (int64_t i = 0; i < (int64_t)compression_buffer_idx; i++) {
			if (!compression_buffer_validity[i] || !compression_buffer_validity[i]){
				continue;
			}
			auto success = TrySubtractOperator::Operation((T_S)(compression_buffer[i]), (T_S)(compression_buffer[i-1]), delta_buffer[i]);
			if (!success) {
				return;
			}
		}

		can_do_delta = true;

		// Note: skips the first as it will be corrected with the for
		for (int64_t i = 1; i < (int64_t)compression_buffer_idx; i++) {
			maximum_delta = MaxValue<T_S>(maximum_delta, delta_buffer[i]);
			minimum_delta = MinValue<T_S>(minimum_delta, delta_buffer[i]);
		}

		// Since we can set the first value arbitrarily, we want to pick one from the current domain, note that
		// we will store the original first value - this offset as the  delta_offset to be able to decode this again.
		delta_buffer[0] = minimum_delta;

		can_do_delta = can_do_delta && TrySubtractOperator::Operation(maximum_delta, minimum_delta, min_max_delta_diff);
		can_do_delta = can_do_delta && TrySubtractOperator::Operation((T_S)(compression_buffer[0]), minimum_delta, delta_offset);
	}

	template <class T_INNER>
	void SubtractFrameOfReference(T_INNER* buffer, T_INNER frame_of_reference) {
		for (idx_t i = 0; i < compression_buffer_idx; i++) {
			buffer[i] -= frame_of_reference;
		}
	}

	// Prevents null values from ruining our fun by setting them to optimally compressible value
	void PatchNullValues(T patch_value) {
		if (all_valid) {
			return;
		}

		for (idx_t i = 0; i < compression_buffer_idx; i++) {
			if (!compression_buffer_validity[i]) {
				compression_buffer[i] = minimum;
			}
		}
	}

	template <class OP>
	bool Flush() {
		if (compression_buffer_idx == 0) {
			return true;
		}

		if ((all_invalid || maximum == minimum) && (mode == BitpackingMode::AUTO || mode == BitpackingMode::CONSTANT)) {
			OP::WriteConstant(maximum, compression_buffer_idx, data_ptr, all_invalid);
			total_size += sizeof(T) + sizeof(bitpacking_metadata_encoded_t);
			return true;
		}

		CalculateFORStats();
		CalculateDeltaStats();

		if (can_do_delta) {
			if (maximum_delta == minimum_delta && mode != BitpackingMode::FOR && mode != BitpackingMode::DELTA_FOR) {
				idx_t frame_of_reference = compression_buffer[0];
				OP::WriteConstantDelta((T_S)maximum_delta, (T)frame_of_reference, compression_buffer_idx, (T*)compression_buffer, (bool*)compression_buffer_validity, data_ptr);
				total_size += sizeof(T) + sizeof(T) + sizeof(bitpacking_metadata_encoded_t);
				return true;
			}

			// Check if delta has benefit
			auto delta_required_bitwidth = BitpackingPrimitives::MinimumBitWidth<T_U>(min_max_delta_diff);
			auto regular_required_bitwidth = BitpackingPrimitives::MinimumBitWidth(min_max_diff);

			if (delta_required_bitwidth < regular_required_bitwidth && mode != BitpackingMode::FOR) {
				SubtractFrameOfReference(delta_buffer, minimum_delta);

				OP::WriteDeltaFor((T*)delta_buffer, compression_buffer_validity, delta_required_bitwidth, (T)minimum_delta, delta_offset, (T*)compression_buffer,
				             compression_buffer_idx, data_ptr);

				total_size += BitpackingPrimitives::GetRequiredSize(compression_buffer_idx, delta_required_bitwidth);
				total_size += sizeof(T); // FOR value
				total_size += sizeof(T); // Delta offset value
				total_size += AlignValue(sizeof(bitpacking_width_t)); // FOR value

				return true;
			}
		}

		if (can_do_for) {
			auto width = BitpackingPrimitives::MinimumBitWidth<T_U>(min_max_diff);
			PatchNullValues(minimum);
			SubtractFrameOfReference(compression_buffer, minimum);
			OP::WriteFor(compression_buffer, compression_buffer_validity, width, minimum,
			                          compression_buffer_idx, data_ptr);

			total_size += BitpackingPrimitives::GetRequiredSize(compression_buffer_idx, width);
			total_size += sizeof(T); // FOR value
			total_size += AlignValue(sizeof(bitpacking_width_t));

			return true;
		}

		return false;
	}

	template <class OP = EmptyBitpackingWriter>
	bool Update(T value, bool is_valid) {
		compression_buffer_validity[compression_buffer_idx] = is_valid;
		all_valid = all_valid && is_valid;
		all_invalid = all_invalid && !is_valid;

		if (is_valid) {
			compression_buffer[compression_buffer_idx] = value;
			minimum = MinValue<T>(minimum, value);
			maximum = MaxValue<T>(maximum, value);
		}

		compression_buffer_idx++;

		if (compression_buffer_idx == BITPACKING_METADATA_GROUP_SIZE) {
			bool success = Flush<OP>();
			Reset();
			return success;
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
	auto& config =  DBConfig::GetConfig(col_data.GetDatabase());

	auto state = make_unique<BitpackingAnalyzeState<T>>();
	state->state.mode = config.options.force_bitpacking_mode;

	return std::move(state);
}

template <class T>
bool BitpackingAnalyze(AnalyzeState &state, Vector &input, idx_t count) {
	auto &analyze_state = (BitpackingAnalyzeState<T> &)state;
	UnifiedVectorFormat vdata;
	input.ToUnifiedFormat(count, vdata);

	auto data = (T *)vdata.data;
	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);
		if (!analyze_state.state.template Update<EmptyBitpackingWriter>(data[idx], vdata.validity.RowIsValid(idx))) {
			return false;
		}
	}
	return true;
}

template <class T>
idx_t BitpackingFinalAnalyze(AnalyzeState &state) {
	auto &bitpacking_state = (BitpackingAnalyzeState<T> &)state;
	auto flush_result = bitpacking_state.state.template Flush<EmptyBitpackingWriter>();
	if (!flush_result) {
		return DConstants::INVALID_INDEX;
	}
	return bitpacking_state.state.total_size;
}

//===--------------------------------------------------------------------===//
// Compress
//===--------------------------------------------------------------------===//
template <class T, class T_S = typename std::make_signed<T>::type>
struct BitpackingCompressState : public CompressionState {
public:
	explicit BitpackingCompressState(ColumnDataCheckpointer &checkpointer) : checkpointer(checkpointer) {
		auto &db = checkpointer.GetDatabase();
		auto &type = checkpointer.GetType();
		auto &config = DBConfig::GetConfig(db);
		function = config.GetCompressionFunction(CompressionType::COMPRESSION_BITPACKING, type.InternalType());
		CreateEmptySegment(checkpointer.GetRowGroup().start);

		state.data_ptr = (void *)this;

		state.mode = config.options.force_bitpacking_mode;
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
		static void WriteConstant(T constant, idx_t count, void* data_ptr, bool all_invalid) {
			auto state = (BitpackingCompressState<T> *)data_ptr;
			idx_t total_bytes_needed = AlignValue<T>(sizeof(bitpacking_metadata_encoded_t)) + sizeof(T);
			state->FlushAndCreateSegmentIfFull(total_bytes_needed);
			D_ASSERT(total_bytes_needed <= state->RemainingSize());

			// Write data/meta
			*(T*)state->data_ptr = constant;

			state->metadata_ptr -= sizeof(bitpacking_metadata_encoded_t)-1;
			Store<bitpacking_metadata_encoded_t>(encode_meta(bitpacking_metadata_t{BitpackingMode::CONSTANT, (uint32_t)(state->data_ptr - state->handle.Ptr())}), state->metadata_ptr);
			state->metadata_ptr -= 1;

			state->data_ptr += sizeof(T);
			state->current_segment->count += count;

			if (!all_invalid) {
				NumericStatistics::Update<T>(state->current_segment->stats, constant);
			}
		}

		static void WriteConstantDelta(T_S constant, T frame_of_reference, idx_t count, T* values, bool* validity, void* data_ptr) {
			auto state = (BitpackingCompressState<T> *)data_ptr;
			idx_t total_bytes_needed = AlignValue<T>(sizeof(bitpacking_metadata_encoded_t)) + sizeof(T);
			state->FlushAndCreateSegmentIfFull(total_bytes_needed);
			D_ASSERT(total_bytes_needed <= state->RemainingSize());

			// Meta
			state->metadata_ptr -= sizeof(bitpacking_metadata_encoded_t)-1;
			Store<bitpacking_metadata_encoded_t>(encode_meta(bitpacking_metadata_t{BitpackingMode::CONSTANT_DELTA, (uint32_t)(state->data_ptr - state->handle.Ptr())}), state->metadata_ptr);
			state->metadata_ptr -= 1;

			// Data
			*(T*)state->data_ptr = frame_of_reference;
			state->data_ptr += sizeof(T);
			*(T*)state->data_ptr = constant;
			state->data_ptr += sizeof(T);

			state->current_segment->count += count;

			// Update Validity
			for (idx_t i = 0; i < count; i++) {
				if (validity[i]) {
					NumericStatistics::Update<T>(state->current_segment->stats, values[i]);
				}
			}
		}

		static void WriteDeltaFor(T *values, bool *validity, bitpacking_width_t width, T frame_of_reference, T_S delta_offset, T* original_values, idx_t count, void *data_ptr) {
			auto state = (BitpackingCompressState<T> *)data_ptr;
			auto bits_required_for_bp = (width * BITPACKING_METADATA_GROUP_SIZE);
			D_ASSERT(bits_required_for_bp % 8 == 0);
			auto data_bytes = bits_required_for_bp / 8 + sizeof(T) + sizeof(T) + AlignValue(sizeof(bitpacking_width_t));
			auto meta_bytes = AlignValue<T>(sizeof(bitpacking_metadata_encoded_t));

//			std::cout << "Writing DELTA-FOR: (for:" << to_string(frame_of_reference) << ", delta_offset:" << to_string(delta_offset) << ", minimum:" << to_string(state->state.minimum_delta) << ", maximum:" <<  to_string(state->state.minimum_delta) << ", width:" << to_string(width) << ", typesize:" << to_string(sizeof(T)) << ", is_signed:" << to_string(std::is_signed<T>()) << ")\n";

			state->FlushAndCreateSegmentIfFull(data_bytes + meta_bytes);

			bitpacking_metadata_t metadata{BitpackingMode::DELTA_FOR, (uint32_t)(state->data_ptr - state->handle.Ptr())};

			*(T*)state->data_ptr = frame_of_reference;
			state->data_ptr += sizeof(T);
			*(T *)state->data_ptr = width;
			state->data_ptr += sizeof(T);
			*(T*)state->data_ptr = delta_offset;
			state->data_ptr += sizeof(T);

			BitpackingPrimitives::PackBuffer<T, false>(state->data_ptr, values, count, width);
			state->data_ptr += (BITPACKING_METADATA_GROUP_SIZE * width) / 8;

			state->metadata_ptr -= sizeof(bitpacking_metadata_encoded_t)-1;
			Store<bitpacking_metadata_encoded_t>(encode_meta(metadata), state->metadata_ptr);
			state->metadata_ptr -= 1;

			state->current_segment->count += count;

			for (idx_t i = 0; i < count; i++) {
				if (validity[i]) {
					NumericStatistics::Update<T>(state->current_segment->stats, original_values[i]);
				}
			}
		}

		static void WriteFor(T *values, bool *validity, bitpacking_width_t width, T frame_of_reference, idx_t count, void *data_ptr) {
				auto state = (BitpackingCompressState<T> *)data_ptr;
				auto bits_required_for_bp = (width * BITPACKING_METADATA_GROUP_SIZE);
				D_ASSERT(bits_required_for_bp % 8 == 0);
				auto data_bytes = bits_required_for_bp / 8 + sizeof(T) + AlignValue(sizeof(bitpacking_width_t));
				auto meta_bytes = AlignValue<T>(sizeof(bitpacking_metadata_encoded_t));
//			    std::cout << "Writing FOR: (for:" << to_string(frame_of_reference) << ", minimum:" << to_string(state->state.minimum) << ", maximum:" <<  to_string(state->state.maximum) << ", width:" << to_string(width) << ", typesize:" << to_string(sizeof(T)) << ", is_signed:" << to_string(std::is_signed<T>()) << ")\n";
				state->FlushAndCreateSegmentIfFull(data_bytes + meta_bytes);

			    bitpacking_metadata_t metadata{BitpackingMode::FOR, (uint32_t)(state->data_ptr - state->handle.Ptr())};

			    *(T*)state->data_ptr = frame_of_reference;
			    state->data_ptr += sizeof(T);
			    *(T *)state->data_ptr = width;
			    state->data_ptr += sizeof(T);

			    BitpackingPrimitives::PackBuffer<T, false>(state->data_ptr, values, count, width);
			    state->data_ptr += (BITPACKING_METADATA_GROUP_SIZE * width) / 8;

				state->metadata_ptr -= sizeof(bitpacking_metadata_encoded_t)-1;
				Store<bitpacking_metadata_encoded_t>(encode_meta(metadata), state->metadata_ptr);
			    state->metadata_ptr -= 1;

			    state->current_segment->count += count;

				for (idx_t i = 0; i < count; i++) {
					if (validity[i]) {
						NumericStatistics::Update<T>(state->current_segment->stats, values[i] + frame_of_reference);
					}
				}
		}
	};

	// Space remaining between the metadata_ptr growing down and data ptr growing up
	idx_t RemainingSize() {
		return metadata_ptr - data_ptr + 1;
	}

	void CreateEmptySegment(idx_t row_start) {
		auto &db = checkpointer.GetDatabase();
		auto &type = checkpointer.GetType();
		auto compressed_segment = ColumnSegment::CreateTransientSegment(db, type, row_start);
		compressed_segment->function = function;
		current_segment = move(compressed_segment);
		auto &buffer_manager = BufferManager::GetBufferManager(db);
		handle = buffer_manager.Pin(current_segment->block);

		data_ptr = handle.Ptr() + BitpackingPrimitives::BITPACKING_HEADER_SIZE;
		metadata_ptr = handle.Ptr() + Storage::BLOCK_SIZE - 1;
	}

	void Append(UnifiedVectorFormat &vdata, idx_t count) {
		// TODO: Optimization: avoid use of compression buffer if we can compress straight to result vector
		auto data = (T *)vdata.data;

		for (idx_t i = 0; i < count; i++) {
			auto idx = vdata.sel->get_index(i);
			state.template Update<BitpackingCompressState<T, T_S>::BitpackingWriter>(data[idx], vdata.validity.RowIsValid(idx));
		}
	}

	void FlushAndCreateSegmentIfFull(idx_t required_space) {
		if (RemainingSize() < required_space) {
			auto row_start = current_segment->start + current_segment->count;
			FlushSegment();
			CreateEmptySegment(row_start);
		}
	}

	void FlushSegment() {
		auto &state = checkpointer.GetCheckpointState();
		auto base_ptr = handle.Ptr();

		// Compact the segment by moving the metadata next to the data.
		idx_t metadata_offset = AlignValue(data_ptr - base_ptr);
		idx_t metadata_size = base_ptr + Storage::BLOCK_SIZE - metadata_ptr - 1;
		idx_t total_segment_size = metadata_offset + metadata_size;
		memmove(base_ptr + metadata_offset, metadata_ptr + 1, metadata_size);

		// Store the offset of the metadata of the first group (which is at the highest address).
		Store<idx_t>(metadata_offset + metadata_size, base_ptr);
		handle.Destroy();

		state.FlushSegment(move(current_segment), total_segment_size);
	}

	void Finalize() {
		state.template Flush<BitpackingCompressState<T, T_S>::BitpackingWriter>();
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
static void ApplyFrameOfReference(T *dst, T frame_of_reference, idx_t size) {
	if (!frame_of_reference) {
		return;
	}
	for (idx_t i = 0; i < size; i++) {
		dst[i] += frame_of_reference;
	}
}


// TODO there's probably some highly efficient variant for this one
template <class T>
static T ApplyDelta(T *dst, T previous_value, idx_t size) {
	D_ASSERT(size >= 1);

	dst[0] += previous_value;

	for (idx_t i = 1; i < size; i++) {
		dst[i] += dst[i-1];
	}

	return dst[size-1];
}

template <class T, class T_S = typename std::make_signed<T>::type>
struct BitpackingScanState : public SegmentScanState {
public:
	explicit BitpackingScanState(ColumnSegment &segment): current_segment(segment) {
		auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
		handle = buffer_manager.Pin(segment.block);
		auto dataptr = handle.Ptr();

		// load offset to bitpacking widths pointer
		auto bitpacking_metadata_offset = Load<idx_t>(dataptr + segment.GetBlockOffset());
		bitpacking_metadata_ptr = dataptr + segment.GetBlockOffset() + bitpacking_metadata_offset - sizeof(bitpacking_metadata_encoded_t);

		// load the first group
		LoadNextGroup();
	}

	BufferHandle handle;
	ColumnSegment &current_segment;

	T decompression_buffer[BITPACKING_METADATA_GROUP_SIZE];

	bitpacking_metadata_t current_group;

	bitpacking_width_t current_width;
	T current_frame_of_reference;
	T current_constant;
	T current_delta_offset;

	idx_t current_group_offset = 0;
	data_ptr_t current_group_ptr;
	data_ptr_t bitpacking_metadata_ptr;

public:
	//! Loads the metadata for the current metadata group. This will set bitpacking_metadata_ptr to the next group.
	//! this will also load any metadata that is at the start of a compressed buffer (e.g. the width, for, or constant value)
	//! depending on the bitpacking mode for that group
	void LoadNextGroup() {
		D_ASSERT(bitpacking_metadata_ptr > handle.Ptr() &&
		         bitpacking_metadata_ptr < handle.Ptr() + Storage::BLOCK_SIZE);
		current_group_offset = 0;
		current_group = decode_meta(*(bitpacking_metadata_encoded_t*)bitpacking_metadata_ptr);

		bitpacking_metadata_ptr -= sizeof(bitpacking_metadata_encoded_t);
		current_group_ptr = GetPtr(current_group);

		// Read first meta value
		switch (current_group.mode) {
			case BitpackingMode::CONSTANT:
			    current_constant = *(T*)(current_group_ptr);
			    current_group_ptr += sizeof(T);
			    break;
		    case BitpackingMode::FOR:
		    case BitpackingMode::CONSTANT_DELTA:
		    case BitpackingMode::DELTA_FOR:
			    current_frame_of_reference = *(T*)(current_group_ptr);
			    current_group_ptr += sizeof(T);
			    break;
		    case BitpackingMode::AUTO:
			    throw InternalException("Invalid bitpacking mode");
		}

		// Read second meta value
		switch (current_group.mode) {
		case BitpackingMode::CONSTANT_DELTA:
			current_constant = *(T*)(current_group_ptr);
			current_group_ptr += sizeof(T);
			break;
		case BitpackingMode::FOR:
		case BitpackingMode::DELTA_FOR:
			current_width = (bitpacking_width_t)*(T*)(current_group_ptr);
			current_group_ptr += MaxValue(sizeof(T), sizeof(bitpacking_width_t));
			break;
		case BitpackingMode::CONSTANT:
			break;
		case BitpackingMode::AUTO:
			throw InternalException("Invalid bitpacking mode");
		}

		// Read third meta value
		switch (current_group.mode) {
		case BitpackingMode::DELTA_FOR:
			current_delta_offset = *(T*)(current_group_ptr);
			current_group_ptr += sizeof(T);
			break;
		case BitpackingMode::CONSTANT_DELTA:
		case BitpackingMode::FOR:
		case BitpackingMode::CONSTANT:
			break;
		case BitpackingMode::AUTO:
			throw InternalException("Invalid bitpacking mode");
		}

//		std::cout << "Reading (" << BitpackingModeToString(current_group.mode) << "): (for:" << to_string(current_frame_of_reference) << ", delta_offset:" << to_string(current_delta_offset) << ", width:" << to_string(current_width) << ", typesize:" << to_string(sizeof(T)) << ", is_signed:" << to_string(std::is_signed<T>()) << ")\n";
	}

	void Skip(ColumnSegment &segment, idx_t skip_count) {
		while (skip_count > 0) {
			if (current_group_offset + skip_count < BITPACKING_METADATA_GROUP_SIZE) {
				// Skipping Delta FOR requires a bit of decoding to figure out the new delta
				if(current_group.mode == BitpackingMode::DELTA_FOR) {
					// if current_group_offset points into the middle of a  BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE
					// for some reason, we need to scan a few values before current_group_offset to align with the algorithm groups
					idx_t extra_count = current_group_offset % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;

					// Calculate total offset and count to bitunpack
					idx_t base_decompress_count = BitpackingPrimitives::RoundUpToAlgorithmGroupSize(skip_count);
					idx_t decompress_count = base_decompress_count + extra_count;
					idx_t decompress_offset = current_group_offset - extra_count;
					bool skip_sign_extension = true;

					BitpackingPrimitives::UnPackBuffer<T>((data_ptr_t)decompression_buffer, current_group_ptr + decompress_offset, decompress_count, current_width, skip_sign_extension);

					ApplyFrameOfReference<T_S>((T_S*)&decompression_buffer[extra_count], current_frame_of_reference, skip_count);
					ApplyDelta<T_S>((T_S*)&decompression_buffer[extra_count], (T_S)current_delta_offset, (idx_t)skip_count);
					current_delta_offset = decompression_buffer[extra_count + skip_count - 1];

					current_group_offset += skip_count;
				} else {
					// For all other modes skipping withing the group is trivial
					current_group_offset += skip_count;
				}
				break;
			} else {
				auto left_in_this_group = BITPACKING_METADATA_GROUP_SIZE - current_group_offset;
				auto number_of_groups_to_skip = (skip_count - left_in_this_group) / BITPACKING_METADATA_GROUP_SIZE;

				current_group_offset = 0;
				bitpacking_metadata_ptr -= number_of_groups_to_skip * sizeof(bitpacking_metadata_encoded_t);

				LoadNextGroup();

				skip_count -= left_in_this_group;
				skip_count -= number_of_groups_to_skip * BITPACKING_METADATA_GROUP_SIZE;
			}
		}
	}

	data_ptr_t GetPtr(bitpacking_metadata_t group) {
		return handle.Ptr() + current_segment.GetBlockOffset() + group.offset;
	}
};

template <class T>
unique_ptr<SegmentScanState> BitpackingInitScan(ColumnSegment &segment) {
	auto result = make_unique<BitpackingScanState<T>>(segment);
	return move(result);
}

//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//
template <class T, class T_S = typename std::make_signed<T>::type>
void BitpackingScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
                           idx_t result_offset) {
	auto &scan_state = (BitpackingScanState<T> &)*state.scan_state;

	T *result_data = FlatVector::GetData<T>(result);
	result.SetVectorType(VectorType::FLAT_VECTOR);

	//! Because FOR offsets all our values to be 0 or above, we can always skip sign extension here
	bool skip_sign_extend = true;

	idx_t scanned = 0;

	while (scanned < scan_count) {
		// Exhausted this metadata group, move pointers to next group and load metadata for next group.
		if (scan_state.current_group_offset >= BITPACKING_METADATA_GROUP_SIZE) {
			scan_state.LoadNextGroup();
		}

		idx_t offset_in_compression_group =
		    scan_state.current_group_offset % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;

		if (scan_state.current_group.mode == BitpackingMode::CONSTANT) {
			idx_t remaining = scan_count-scanned;
			idx_t to_scan = MinValue(remaining, BITPACKING_METADATA_GROUP_SIZE - scan_state.current_group_offset);
			T* begin = result_data + result_offset + scanned;
			T* end = begin + remaining;
			std::fill(begin, end, scan_state.current_constant);
			scanned += to_scan;
			scan_state.current_group_offset += to_scan;
			continue;
		}
		if (scan_state.current_group.mode == BitpackingMode::CONSTANT_DELTA) {
			idx_t remaining = scan_count-scanned;
			idx_t to_scan = MinValue(remaining, BITPACKING_METADATA_GROUP_SIZE - scan_state.current_group_offset);
			T* target_ptr = result_data + result_offset + scanned;

			for (idx_t i = 0; i < to_scan; i++){
				target_ptr[i] = ((scan_state.current_group_offset + i) * scan_state.current_constant) + scan_state.current_frame_of_reference;
			}

			scanned += to_scan;
			scan_state.current_group_offset += to_scan;
			continue;
		}
		D_ASSERT(scan_state.current_group.mode == BitpackingMode::FOR || scan_state.current_group.mode == BitpackingMode::DELTA_FOR);

		idx_t to_scan = MinValue<idx_t>(scan_count - scanned, BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE -
		                                                          offset_in_compression_group);
		// Calculate start of compression algorithm group
		data_ptr_t current_position_ptr =
		    scan_state.current_group_ptr + scan_state.current_group_offset * scan_state.current_width / 8;
		data_ptr_t decompression_group_start_pointer =
		    current_position_ptr - offset_in_compression_group * scan_state.current_width / 8;

		T *current_result_ptr = result_data + result_offset + scanned;

		if (to_scan == BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE && offset_in_compression_group == 0) {
			// Decompress directly into result vector
			BitpackingPrimitives::UnPackBlock<T>((data_ptr_t)current_result_ptr, decompression_group_start_pointer,
			                                     scan_state.current_width, skip_sign_extend);
		} else {
			// Decompress compression algorithm to buffer
			BitpackingPrimitives::UnPackBlock<T>((data_ptr_t)scan_state.decompression_buffer,
			                                     decompression_group_start_pointer, scan_state.current_width,
			                                     skip_sign_extend);

			memcpy(current_result_ptr, scan_state.decompression_buffer + offset_in_compression_group,
			       to_scan * sizeof(T));
		}

		if (scan_state.current_group.mode == BitpackingMode::DELTA_FOR) {
			ApplyFrameOfReference<T_S>((T_S *)current_result_ptr, (T_S)scan_state.current_frame_of_reference, to_scan);
			ApplyDelta<T_S>((T_S *)current_result_ptr, (T_S)scan_state.current_delta_offset, to_scan);
		} else {
			ApplyFrameOfReference<T>(current_result_ptr, scan_state.current_frame_of_reference, to_scan);
		}

		scanned += to_scan;
		scan_state.current_group_offset += to_scan;
		scan_state.current_delta_offset = ((T*)current_result_ptr)[to_scan-1];
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

	idx_t offset_in_compression_group =
	    scan_state.current_group_offset % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;

	data_ptr_t decompression_group_start_pointer =
	    scan_state.current_group_ptr +
	    (scan_state.current_group_offset - offset_in_compression_group) * scan_state.current_width / 8;

	//! Because FOR offsets all our values to be 0 or above, we can always skip sign extension here
	bool skip_sign_extend = true;

	if (scan_state.current_group.mode == BitpackingMode::CONSTANT) {
		*current_result_ptr = scan_state.current_constant;
		return;
	}

	if (scan_state.current_group.mode == BitpackingMode::CONSTANT_DELTA) {
		*current_result_ptr = ((scan_state.current_group_offset) * scan_state.current_constant) + scan_state.current_frame_of_reference;
		return;
	}

	D_ASSERT(scan_state.current_group.mode == BitpackingMode::FOR || scan_state.current_group.mode == BitpackingMode::DELTA_FOR);

	BitpackingPrimitives::UnPackBlock<T>((data_ptr_t)scan_state.decompression_buffer, decompression_group_start_pointer,
	                                     scan_state.current_width, skip_sign_extend);

	*current_result_ptr = *(T *)(scan_state.decompression_buffer + offset_in_compression_group);
	*current_result_ptr += scan_state.current_frame_of_reference;

	if (scan_state.current_group.mode == BitpackingMode::DELTA_FOR) {
		*current_result_ptr += scan_state.current_delta_offset;
	}
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
