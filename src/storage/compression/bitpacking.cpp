#include "duckdb/common/bitpacking.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/function/compression/compression.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/storage/buffer_manager.hpp"

#include "duckdb/storage/table/column_data_checkpointer.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/common/operator/subtract.hpp"
#include "duckdb/common/operator/multiply.hpp"
#include "duckdb/common/operator/add.hpp"
#include "duckdb/storage/compression/bitpacking.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/common/numeric_utils.hpp"

#include <functional>

namespace duckdb {

static constexpr const idx_t BITPACKING_METADATA_GROUP_SIZE = STANDARD_VECTOR_SIZE > 512 ? STANDARD_VECTOR_SIZE : 2048;

BitpackingMode BitpackingModeFromString(const string &str) {
	auto mode = StringUtil::Lower(str);
	if (mode == "auto" || mode == "none") {
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
		return BitpackingMode::INVALID;
	}
}

string BitpackingModeToString(const BitpackingMode &mode) {
	switch (mode) {
	case BitpackingMode::AUTO:
		return "auto";
	case BitpackingMode::CONSTANT:
		return "constant";
	case BitpackingMode::CONSTANT_DELTA:
		return "constant_delta";
	case BitpackingMode::DELTA_FOR:
		return "delta_for";
	case BitpackingMode::FOR:
		return "for";
	default:
		throw NotImplementedException("Unknown bitpacking mode: " + to_string((uint8_t)mode) + "\n");
	}
}

typedef struct {
	BitpackingMode mode;
	uint32_t offset;
} bitpacking_metadata_t;

typedef uint32_t bitpacking_metadata_encoded_t;

static bitpacking_metadata_encoded_t EncodeMeta(bitpacking_metadata_t metadata) {
	D_ASSERT(metadata.offset <= 16777215); // max uint24_t
	bitpacking_metadata_encoded_t encoded_value = metadata.offset;
	encoded_value |= (uint8_t)metadata.mode << 24;
	return encoded_value;
}
static bitpacking_metadata_t DecodeMeta(bitpacking_metadata_encoded_t *metadata_encoded) {
	bitpacking_metadata_t metadata;
	metadata.mode = Load<BitpackingMode>(data_ptr_cast(metadata_encoded) + 3);
	metadata.offset = *metadata_encoded & 0x00FFFFFF;
	return metadata;
}

struct EmptyBitpackingWriter {
	template <class T>
	static void WriteConstant(T constant, idx_t count, void *data_ptr, bool all_invalid) {
	}
	template <class T, class T_S = typename MakeSigned<T>::type>
	static void WriteConstantDelta(T_S constant, T frame_of_reference, idx_t count, T *values, bool *validity,
	                               void *data_ptr) {
	}
	template <class T, class T_S = typename MakeSigned<T>::type>
	static void WriteDeltaFor(T *values, bool *validity, bitpacking_width_t width, T frame_of_reference,
	                          T_S delta_offset, T *original_values, idx_t count, void *data_ptr) {
	}
	template <class T>
	static void WriteFor(T *values, bool *validity, bitpacking_width_t width, T frame_of_reference, idx_t count,
	                     void *data_ptr) {
	}
};

template <class T, class T_S = typename MakeSigned<T>::type>
struct BitpackingState {
public:
	BitpackingState() : compression_buffer_idx(0), total_size(0), data_ptr(nullptr) {
		compression_buffer_internal[0] = T(0);
		compression_buffer = &compression_buffer_internal[1];
		Reset();
	}

	// Extra val for delta encoding
	T compression_buffer_internal[BITPACKING_METADATA_GROUP_SIZE + 1];
	T *compression_buffer;
	T_S delta_buffer[BITPACKING_METADATA_GROUP_SIZE];
	bool compression_buffer_validity[BITPACKING_METADATA_GROUP_SIZE];
	idx_t compression_buffer_idx;
	idx_t total_size;

	// Used to pass CompressionState ptr through the Bitpacking writer
	void *data_ptr;

	// Stats on current compression buffer
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
		min_max_delta_diff = 0;
	}

	void CalculateFORStats() {
		can_do_for = TrySubtractOperator::Operation(maximum, minimum, min_max_diff);
	}

	void CalculateDeltaStats() {
		// TODO: currently we dont support delta compression of values above NumericLimits<T_S>::Maximum(),
		// 		 we could support this with some clever substract trickery?
		if (maximum > static_cast<T>(NumericLimits<T_S>::Maximum())) {
			return;
		}

		// Don't delta encoding 1 value makes no sense
		if (compression_buffer_idx < 2) {
			return;
		}

		// TODO: handle NULLS here?
		// Currently we cannot handle nulls because we would need an additional step of patching for this.
		// we could for example copy the last value on a null insert. This would help a bit, but not be optimal for
		// large deltas since theres suddenly a zero then. Ideally we would insert a value that leads to a delta within
		// the current domain of deltas however we dont know that domain here yet
		if (!all_valid) {
			return;
		}

		// Note: since we dont allow any values over NumericLimits<T_S>::Maximum(), all subtractions for unsigned types
		// are guaranteed not to overflow
		bool can_do_all = true;
		if (NumericLimits<T>::IsSigned()) {
			T_S bogus;
			can_do_all = TrySubtractOperator::Operation(static_cast<T_S>(minimum), static_cast<T_S>(maximum), bogus) &&
			             TrySubtractOperator::Operation(static_cast<T_S>(maximum), static_cast<T_S>(minimum), bogus);
		}

		// Calculate delta's
		// compression_buffer pointer points one element ahead of the internal buffer making the use of signed index
		// integer (-1) possible
		D_ASSERT(compression_buffer_idx <= NumericLimits<int64_t>::Maximum());
		if (can_do_all) {
			for (int64_t i = 0; i < static_cast<int64_t>(compression_buffer_idx); i++) {
				delta_buffer[i] = static_cast<T_S>(compression_buffer[i]) - static_cast<T_S>(compression_buffer[i - 1]);
			}
		} else {
			for (int64_t i = 0; i < static_cast<int64_t>(compression_buffer_idx); i++) {
				auto success =
				    TrySubtractOperator::Operation(static_cast<T_S>(compression_buffer[i]),
				                                   static_cast<T_S>(compression_buffer[i - 1]), delta_buffer[i]);
				if (!success) {
					return;
				}
			}
		}

		can_do_delta = true;

		for (idx_t i = 1; i < compression_buffer_idx; i++) {
			maximum_delta = MaxValue<T_S>(maximum_delta, delta_buffer[i]);
			minimum_delta = MinValue<T_S>(minimum_delta, delta_buffer[i]);
		}

		// Since we can set the first value arbitrarily, we want to pick one from the current domain, note that
		// we will store the original first value - this offset as the  delta_offset to be able to decode this again.
		delta_buffer[0] = minimum_delta;

		can_do_delta = can_do_delta && TrySubtractOperator::Operation(maximum_delta, minimum_delta, min_max_delta_diff);
		can_do_delta = can_do_delta && TrySubtractOperator::Operation(static_cast<T_S>(compression_buffer[0]),
		                                                              minimum_delta, delta_offset);
	}

	template <class T_INNER>
	void SubtractFrameOfReference(T_INNER *buffer, T_INNER frame_of_reference) {
		static_assert(IsIntegral<T_INNER>::value, "Integral type required.");
		for (idx_t i = 0; i < compression_buffer_idx; i++) {
			buffer[i] -= static_cast<typename MakeUnsigned<T_INNER>::type>(frame_of_reference);
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
				// FOR needs to be T (considering hugeint is bigger than idx_t)
				T frame_of_reference = compression_buffer[0];

				OP::WriteConstantDelta(maximum_delta, static_cast<T>(frame_of_reference), compression_buffer_idx,
				                       compression_buffer, compression_buffer_validity, data_ptr);
				total_size += sizeof(T) + sizeof(T) + sizeof(bitpacking_metadata_encoded_t);
				return true;
			}

			// Check if delta has benefit
			// bitwidth is calculated differently between signed and unsigned values, but considering we do not have
			// an unsigned version of hugeint, we need to explicitly specify (through boolean) that we wish to calculate
			// the unsigned minimum bit-width instead of relying on MakeUnsigned and IsSigned
			auto delta_required_bitwidth = BitpackingPrimitives::MinimumBitWidth<T, false>(min_max_delta_diff);
			auto regular_required_bitwidth = BitpackingPrimitives::MinimumBitWidth(min_max_diff);

			if (delta_required_bitwidth < regular_required_bitwidth && mode != BitpackingMode::FOR) {
				SubtractFrameOfReference(delta_buffer, minimum_delta);

				OP::WriteDeltaFor(reinterpret_cast<T *>(delta_buffer), compression_buffer_validity,
				                  delta_required_bitwidth, static_cast<T>(minimum_delta), delta_offset,
				                  compression_buffer, compression_buffer_idx, data_ptr);

				total_size += BitpackingPrimitives::GetRequiredSize(compression_buffer_idx, delta_required_bitwidth);
				total_size += sizeof(T);                              // FOR value
				total_size += sizeof(T);                              // Delta offset value
				total_size += AlignValue(sizeof(bitpacking_width_t)); // FOR value

				return true;
			}
		}

		if (can_do_for) {
			auto width = BitpackingPrimitives::MinimumBitWidth<T, false>(min_max_diff);
			SubtractFrameOfReference(compression_buffer, minimum);
			OP::WriteFor(compression_buffer, compression_buffer_validity, width, minimum, compression_buffer_idx,
			             data_ptr);

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
	auto &config = DBConfig::GetConfig(col_data.GetDatabase());

	auto state = make_uniq<BitpackingAnalyzeState<T>>();
	state->state.mode = config.options.force_bitpacking_mode;

	return std::move(state);
}

template <class T>
bool BitpackingAnalyze(AnalyzeState &state, Vector &input, idx_t count) {
	auto &analyze_state = static_cast<BitpackingAnalyzeState<T> &>(state);
	UnifiedVectorFormat vdata;
	input.ToUnifiedFormat(count, vdata);

	auto data = UnifiedVectorFormat::GetData<T>(vdata);
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
	auto &bitpacking_state = static_cast<BitpackingAnalyzeState<T> &>(state);
	auto flush_result = bitpacking_state.state.template Flush<EmptyBitpackingWriter>();
	if (!flush_result) {
		return DConstants::INVALID_INDEX;
	}
	return bitpacking_state.state.total_size;
}

//===--------------------------------------------------------------------===//
// Compress
//===--------------------------------------------------------------------===//
template <class T, bool WRITE_STATISTICS, class T_S = typename MakeSigned<T>::type>
struct BitpackingCompressState : public CompressionState {
public:
	explicit BitpackingCompressState(ColumnDataCheckpointer &checkpointer)
	    : checkpointer(checkpointer),
	      function(checkpointer.GetCompressionFunction(CompressionType::COMPRESSION_BITPACKING)) {
		CreateEmptySegment(checkpointer.GetRowGroup().start);

		state.data_ptr = reinterpret_cast<void *>(this);

		auto &config = DBConfig::GetConfig(checkpointer.GetDatabase());
		state.mode = config.options.force_bitpacking_mode;
	}

	ColumnDataCheckpointer &checkpointer;
	CompressionFunction &function;
	unique_ptr<ColumnSegment> current_segment;
	BufferHandle handle;

	// Ptr to next free spot in segment;
	data_ptr_t data_ptr;
	// Ptr to next free spot for storing bitwidths and frame-of-references (growing downwards).
	data_ptr_t metadata_ptr;

	BitpackingState<T> state;

public:
	struct BitpackingWriter {
		static void WriteConstant(T constant, idx_t count, void *data_ptr, bool all_invalid) {
			auto state = reinterpret_cast<BitpackingCompressState<T, WRITE_STATISTICS> *>(data_ptr);

			ReserveSpace(state, sizeof(T));
			WriteMetaData(state, BitpackingMode::CONSTANT);
			WriteData(state->data_ptr, constant);

			UpdateStats(state, count);
		}

		static void WriteConstantDelta(T_S constant, T frame_of_reference, idx_t count, T *values, bool *validity,
		                               void *data_ptr) {
			auto state = reinterpret_cast<BitpackingCompressState<T, WRITE_STATISTICS> *>(data_ptr);

			ReserveSpace(state, 2 * sizeof(T));
			WriteMetaData(state, BitpackingMode::CONSTANT_DELTA);
			WriteData(state->data_ptr, frame_of_reference);
			WriteData(state->data_ptr, constant);

			UpdateStats(state, count);
		}
		static void WriteDeltaFor(T *values, bool *validity, bitpacking_width_t width, T frame_of_reference,
		                          T_S delta_offset, T *original_values, idx_t count, void *data_ptr) {
			auto state = reinterpret_cast<BitpackingCompressState<T, WRITE_STATISTICS> *>(data_ptr);

			auto bp_size = BitpackingPrimitives::GetRequiredSize(count, width);
			ReserveSpace(state, bp_size + 3 * sizeof(T));

			WriteMetaData(state, BitpackingMode::DELTA_FOR);
			WriteData(state->data_ptr, frame_of_reference);
			WriteData(state->data_ptr, static_cast<T>(width));
			WriteData(state->data_ptr, delta_offset);

			BitpackingPrimitives::PackBuffer<T, false>(state->data_ptr, values, count, width);
			state->data_ptr += bp_size;

			UpdateStats(state, count);
		}

		static void WriteFor(T *values, bool *validity, bitpacking_width_t width, T frame_of_reference, idx_t count,
		                     void *data_ptr) {
			auto state = reinterpret_cast<BitpackingCompressState<T, WRITE_STATISTICS> *>(data_ptr);

			auto bp_size = BitpackingPrimitives::GetRequiredSize(count, width);
			ReserveSpace(state, bp_size + 2 * sizeof(T));

			WriteMetaData(state, BitpackingMode::FOR);
			WriteData(state->data_ptr, frame_of_reference);
			WriteData(state->data_ptr, (T)width);

			BitpackingPrimitives::PackBuffer<T, false>(state->data_ptr, values, count, width);
			state->data_ptr += bp_size;

			UpdateStats(state, count);
		}

		template <class T_OUT>
		static void WriteData(data_ptr_t &ptr, T_OUT val) {
			*reinterpret_cast<T_OUT *>(ptr) = val;
			ptr += sizeof(T_OUT);
		}

		static void WriteMetaData(BitpackingCompressState<T, WRITE_STATISTICS> *state, BitpackingMode mode) {
			bitpacking_metadata_t metadata {mode, (uint32_t)(state->data_ptr - state->handle.Ptr())};
			state->metadata_ptr -= sizeof(bitpacking_metadata_encoded_t);
			Store<bitpacking_metadata_encoded_t>(EncodeMeta(metadata), state->metadata_ptr);
		}

		static void ReserveSpace(BitpackingCompressState<T, WRITE_STATISTICS> *state, idx_t data_bytes) {
			idx_t meta_bytes = sizeof(bitpacking_metadata_encoded_t);
			state->FlushAndCreateSegmentIfFull(data_bytes, meta_bytes);
			D_ASSERT(state->CanStore(data_bytes, meta_bytes));
		}

		static void UpdateStats(BitpackingCompressState<T, WRITE_STATISTICS> *state, idx_t count) {
			state->current_segment->count += count;

			if (WRITE_STATISTICS && !state->state.all_invalid) {
				NumericStats::Update<T>(state->current_segment->stats.statistics, state->state.minimum);
				NumericStats::Update<T>(state->current_segment->stats.statistics, state->state.maximum);
			}
		}
	};

	bool CanStore(idx_t data_bytes, idx_t meta_bytes) {
		auto required_data_bytes = AlignValue<idx_t>((data_ptr + data_bytes) - data_ptr);
		auto required_meta_bytes = Storage::BLOCK_SIZE - (metadata_ptr - data_ptr) + meta_bytes;

		return required_data_bytes + required_meta_bytes <=
		       Storage::BLOCK_SIZE - BitpackingPrimitives::BITPACKING_HEADER_SIZE;
	}

	void CreateEmptySegment(idx_t row_start) {
		auto &db = checkpointer.GetDatabase();
		auto &type = checkpointer.GetType();
		auto compressed_segment = ColumnSegment::CreateTransientSegment(db, type, row_start);
		compressed_segment->function = function;
		current_segment = std::move(compressed_segment);
		auto &buffer_manager = BufferManager::GetBufferManager(db);
		handle = buffer_manager.Pin(current_segment->block);

		data_ptr = handle.Ptr() + BitpackingPrimitives::BITPACKING_HEADER_SIZE;
		metadata_ptr = handle.Ptr() + Storage::BLOCK_SIZE;
	}

	void Append(UnifiedVectorFormat &vdata, idx_t count) {
		auto data = UnifiedVectorFormat::GetData<T>(vdata);

		for (idx_t i = 0; i < count; i++) {
			idx_t idx = vdata.sel->get_index(i);
			state.template Update<BitpackingCompressState<T, WRITE_STATISTICS, T_S>::BitpackingWriter>(
			    data[idx], vdata.validity.RowIsValid(idx));
		}
	}

	void FlushAndCreateSegmentIfFull(idx_t required_data_bytes, idx_t required_meta_bytes) {
		if (!CanStore(required_data_bytes, required_meta_bytes)) {
			idx_t row_start = current_segment->start + current_segment->count;
			FlushSegment();
			CreateEmptySegment(row_start);
		}
	}

	void FlushSegment() {
		auto &state = checkpointer.GetCheckpointState();
		auto base_ptr = handle.Ptr();

		// Compact the segment by moving the metadata next to the data.
		idx_t metadata_offset = AlignValue(data_ptr - base_ptr);
		idx_t metadata_size = base_ptr + Storage::BLOCK_SIZE - metadata_ptr;
		idx_t total_segment_size = metadata_offset + metadata_size;

		// Asserting things are still sane here
		if (!CanStore(0, 0)) {
			throw InternalException("Error in bitpacking size calculation");
		}

		memmove(base_ptr + metadata_offset, metadata_ptr, metadata_size);

		// Store the offset of the metadata of the first group (which is at the highest address).
		Store<idx_t>(metadata_offset + metadata_size, base_ptr);
		handle.Destroy();

		state.FlushSegment(std::move(current_segment), total_segment_size);
	}

	void Finalize() {
		state.template Flush<BitpackingCompressState<T, WRITE_STATISTICS, T_S>::BitpackingWriter>();
		FlushSegment();
		current_segment.reset();
	}
};

template <class T, bool WRITE_STATISTICS>
unique_ptr<CompressionState> BitpackingInitCompression(ColumnDataCheckpointer &checkpointer,
                                                       unique_ptr<AnalyzeState> state) {
	return make_uniq<BitpackingCompressState<T, WRITE_STATISTICS>>(checkpointer);
}

template <class T, bool WRITE_STATISTICS>
void BitpackingCompress(CompressionState &state_p, Vector &scan_vector, idx_t count) {
	auto &state = static_cast<BitpackingCompressState<T, WRITE_STATISTICS> &>(state_p);
	UnifiedVectorFormat vdata;
	scan_vector.ToUnifiedFormat(count, vdata);
	state.Append(vdata, count);
}

template <class T, bool WRITE_STATISTICS>
void BitpackingFinalizeCompress(CompressionState &state_p) {
	auto &state = static_cast<BitpackingCompressState<T, WRITE_STATISTICS> &>(state_p);
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

// Based on https://github.com/lemire/FastPFor (Apache License 2.0)
template <class T>
static T DeltaDecode(T *data, T previous_value, const size_t size) {
	D_ASSERT(size >= 1);

	data[0] += previous_value;

	const size_t UnrollQty = 4;
	const size_t sz0 = (size / UnrollQty) * UnrollQty; // equal to 0, if size < UnrollQty
	size_t i = 1;
	if (sz0 >= UnrollQty) {
		T a = data[0];
		for (; i < sz0 - UnrollQty; i += UnrollQty) {
			a = data[i] += a;
			a = data[i + 1] += a;
			a = data[i + 2] += a;
			a = data[i + 3] += a;
		}
	}
	for (; i != size; ++i) {
		data[i] += data[i - 1];
	}

	return data[size - 1];
}

template <class T, class T_S = typename MakeSigned<T>::type>
struct BitpackingScanState : public SegmentScanState {
public:
	explicit BitpackingScanState(ColumnSegment &segment) : current_segment(segment) {
		auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
		handle = buffer_manager.Pin(segment.block);
		auto dataptr = handle.Ptr();

		// load offset to bitpacking widths pointer
		auto bitpacking_metadata_offset = Load<idx_t>(dataptr + segment.GetBlockOffset());
		bitpacking_metadata_ptr =
		    dataptr + segment.GetBlockOffset() + bitpacking_metadata_offset - sizeof(bitpacking_metadata_encoded_t);

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
	//! this will also load any metadata that is at the start of a compressed buffer (e.g. the width, for, or constant
	//! value) depending on the bitpacking mode for that group
	void LoadNextGroup() {
		D_ASSERT(bitpacking_metadata_ptr > handle.Ptr() &&
		         bitpacking_metadata_ptr < handle.Ptr() + Storage::BLOCK_SIZE);
		current_group_offset = 0;
		current_group = DecodeMeta(reinterpret_cast<bitpacking_metadata_encoded_t *>(bitpacking_metadata_ptr));

		bitpacking_metadata_ptr -= sizeof(bitpacking_metadata_encoded_t);
		current_group_ptr = GetPtr(current_group);

		// Read first value
		switch (current_group.mode) {
		case BitpackingMode::CONSTANT:
			current_constant = *reinterpret_cast<T *>(current_group_ptr);
			current_group_ptr += sizeof(T);
			break;
		case BitpackingMode::FOR:
		case BitpackingMode::CONSTANT_DELTA:
		case BitpackingMode::DELTA_FOR:
			current_frame_of_reference = *reinterpret_cast<T *>(current_group_ptr);
			current_group_ptr += sizeof(T);
			break;
		default:
			throw InternalException("Invalid bitpacking mode");
		}

		// Read second value
		switch (current_group.mode) {
		case BitpackingMode::CONSTANT_DELTA:
			current_constant = *reinterpret_cast<T *>(current_group_ptr);
			current_group_ptr += sizeof(T);
			break;
		case BitpackingMode::FOR:
		case BitpackingMode::DELTA_FOR:
			current_width = (bitpacking_width_t)(*reinterpret_cast<T *>(current_group_ptr));
			current_group_ptr += MaxValue(sizeof(T), sizeof(bitpacking_width_t));
			break;
		case BitpackingMode::CONSTANT:
			break;
		default:
			throw InternalException("Invalid bitpacking mode");
		}

		// Read third value
		if (current_group.mode == BitpackingMode::DELTA_FOR) {
			current_delta_offset = *reinterpret_cast<T *>(current_group_ptr);
			current_group_ptr += sizeof(T);
		}
	}

	void Skip(ColumnSegment &segment, idx_t skip_count) {
		bool skip_sign_extend = true;

		idx_t skipped = 0;
		while (skipped < skip_count) {
			// Exhausted this metadata group, move pointers to next group and load metadata for next group.
			if (current_group_offset >= BITPACKING_METADATA_GROUP_SIZE) {
				LoadNextGroup();
			}

			idx_t offset_in_compression_group =
			    current_group_offset % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;

			if (current_group.mode == BitpackingMode::CONSTANT) {
				idx_t remaining = skip_count - skipped;
				idx_t to_skip = MinValue(remaining, BITPACKING_METADATA_GROUP_SIZE - current_group_offset);
				skipped += to_skip;
				current_group_offset += to_skip;
				continue;
			}
			if (current_group.mode == BitpackingMode::CONSTANT_DELTA) {
				idx_t remaining = skip_count - skipped;
				idx_t to_skip = MinValue(remaining, BITPACKING_METADATA_GROUP_SIZE - current_group_offset);
				skipped += to_skip;
				current_group_offset += to_skip;
				continue;
			}
			D_ASSERT(current_group.mode == BitpackingMode::FOR || current_group.mode == BitpackingMode::DELTA_FOR);

			idx_t to_skip =
			    MinValue<idx_t>(skip_count - skipped,
			                    BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE - offset_in_compression_group);
			// Calculate start of compression algorithm group
			if (current_group.mode == BitpackingMode::DELTA_FOR) {
				data_ptr_t current_position_ptr = current_group_ptr + current_group_offset * current_width / 8;
				data_ptr_t decompression_group_start_pointer =
				    current_position_ptr - offset_in_compression_group * current_width / 8;

				BitpackingPrimitives::UnPackBlock<T>(data_ptr_cast(decompression_buffer),
				                                     decompression_group_start_pointer, current_width,
				                                     skip_sign_extend);

				T *decompression_ptr = decompression_buffer + offset_in_compression_group;
				ApplyFrameOfReference<T_S>(reinterpret_cast<T_S *>(decompression_ptr),
				                           static_cast<T_S>(current_frame_of_reference), to_skip);
				DeltaDecode<T_S>(reinterpret_cast<T_S *>(decompression_ptr), static_cast<T_S>(current_delta_offset),
				                 to_skip);
				current_delta_offset = decompression_ptr[to_skip - 1];
			}

			skipped += to_skip;
			current_group_offset += to_skip;
		}
	}

	data_ptr_t GetPtr(bitpacking_metadata_t group) {
		return handle.Ptr() + current_segment.GetBlockOffset() + group.offset;
	}
};

template <class T>
unique_ptr<SegmentScanState> BitpackingInitScan(ColumnSegment &segment) {
	auto result = make_uniq<BitpackingScanState<T>>(segment);
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//
template <class T, class T_S = typename MakeSigned<T>::type>
void BitpackingScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
                           idx_t result_offset) {
	auto &scan_state = static_cast<BitpackingScanState<T> &>(*state.scan_state);

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
			idx_t remaining = scan_count - scanned;
			idx_t to_scan = MinValue(remaining, BITPACKING_METADATA_GROUP_SIZE - scan_state.current_group_offset);
			T *begin = result_data + result_offset + scanned;
			T *end = begin + remaining;
			std::fill(begin, end, scan_state.current_constant);
			scanned += to_scan;
			scan_state.current_group_offset += to_scan;
			continue;
		}
		if (scan_state.current_group.mode == BitpackingMode::CONSTANT_DELTA) {
			idx_t remaining = scan_count - scanned;
			idx_t to_scan = MinValue(remaining, BITPACKING_METADATA_GROUP_SIZE - scan_state.current_group_offset);
			T *target_ptr = result_data + result_offset + scanned;

			for (idx_t i = 0; i < to_scan; i++) {
				target_ptr[i] = (static_cast<T>(scan_state.current_group_offset + i) * scan_state.current_constant) +
				                scan_state.current_frame_of_reference;
			}

			scanned += to_scan;
			scan_state.current_group_offset += to_scan;
			continue;
		}
		D_ASSERT(scan_state.current_group.mode == BitpackingMode::FOR ||
		         scan_state.current_group.mode == BitpackingMode::DELTA_FOR);

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
			BitpackingPrimitives::UnPackBlock<T>(data_ptr_cast(current_result_ptr), decompression_group_start_pointer,
			                                     scan_state.current_width, skip_sign_extend);
		} else {
			// Decompress compression algorithm to buffer
			BitpackingPrimitives::UnPackBlock<T>(data_ptr_cast(scan_state.decompression_buffer),
			                                     decompression_group_start_pointer, scan_state.current_width,
			                                     skip_sign_extend);

			memcpy(current_result_ptr, scan_state.decompression_buffer + offset_in_compression_group,
			       to_scan * sizeof(T));
		}

		if (scan_state.current_group.mode == BitpackingMode::DELTA_FOR) {
			ApplyFrameOfReference<T_S>(reinterpret_cast<T_S *>(current_result_ptr),
			                           static_cast<T_S>(scan_state.current_frame_of_reference), to_scan);
			DeltaDecode<T_S>(reinterpret_cast<T_S *>(current_result_ptr),
			                 static_cast<T_S>(scan_state.current_delta_offset), to_scan);
			scan_state.current_delta_offset = current_result_ptr[to_scan - 1];
		} else {
			ApplyFrameOfReference<T>(current_result_ptr, scan_state.current_frame_of_reference, to_scan);
		}

		scanned += to_scan;
		scan_state.current_group_offset += to_scan;
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
	T *result_data = FlatVector::GetData<T>(result);
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
#ifdef DEBUG
		// overflow check
		T result;
		bool multiply = TryMultiplyOperator::Operation(static_cast<T>(scan_state.current_group_offset),
		                                               scan_state.current_constant, result);
		bool add = TryAddOperator::Operation(result, scan_state.current_frame_of_reference, result);
		D_ASSERT(multiply && add);
#endif
		*current_result_ptr = (static_cast<T>(scan_state.current_group_offset) * scan_state.current_constant) +
		                      scan_state.current_frame_of_reference;
		return;
	}

	D_ASSERT(scan_state.current_group.mode == BitpackingMode::FOR ||
	         scan_state.current_group.mode == BitpackingMode::DELTA_FOR);

	BitpackingPrimitives::UnPackBlock<T>(data_ptr_cast(scan_state.decompression_buffer),
	                                     decompression_group_start_pointer, scan_state.current_width, skip_sign_extend);

	*current_result_ptr = scan_state.decompression_buffer[offset_in_compression_group];
	*current_result_ptr += scan_state.current_frame_of_reference;

	if (scan_state.current_group.mode == BitpackingMode::DELTA_FOR) {
		*current_result_ptr += scan_state.current_delta_offset;
	}
}
template <class T>
void BitpackingSkip(ColumnSegment &segment, ColumnScanState &state, idx_t skip_count) {
	auto &scan_state = static_cast<BitpackingScanState<T> &>(*state.scan_state);
	scan_state.Skip(segment, skip_count);
}

//===--------------------------------------------------------------------===//
// Get Function
//===--------------------------------------------------------------------===//
template <class T, bool WRITE_STATISTICS = true>
CompressionFunction GetBitpackingFunction(PhysicalType data_type) {
	return CompressionFunction(CompressionType::COMPRESSION_BITPACKING, data_type, BitpackingInitAnalyze<T>,
	                           BitpackingAnalyze<T>, BitpackingFinalAnalyze<T>,
	                           BitpackingInitCompression<T, WRITE_STATISTICS>, BitpackingCompress<T, WRITE_STATISTICS>,
	                           BitpackingFinalizeCompress<T, WRITE_STATISTICS>, BitpackingInitScan<T>,
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
	case PhysicalType::INT128:
		return GetBitpackingFunction<hugeint_t>(type);
	case PhysicalType::LIST:
		return GetBitpackingFunction<uint64_t, false>(type);
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
	case PhysicalType::LIST:
	case PhysicalType::INT128:
		return true;
	default:
		return false;
	}
}

} // namespace duckdb
