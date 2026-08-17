#include "duckdb/common/autovec.hpp"
#include "duckdb/common/bitpacking.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/operator/multiply.hpp"
#include "duckdb/common/operator/subtract.hpp"
#include "duckdb/common/vector/for_vector.hpp"
#include "duckdb/function/compression/compression.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/compression/bitpacking.hpp"
#include "duckdb/storage/compression/standard_compression_state.hpp"
#include "duckdb/storage/table/column_data_checkpointer.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/storage/table/scan_state.hpp"

#include <functional>

namespace duckdb {

constexpr const idx_t BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;
static constexpr const idx_t BITPACKING_METADATA_GROUP_SIZE = STANDARD_VECTOR_SIZE > 512 ? STANDARD_VECTOR_SIZE : 2048;

typedef struct {
	BitpackingMode mode;
	uint32_t offset;
} bitpacking_metadata_t;

typedef uint32_t bitpacking_metadata_encoded_t;

static bitpacking_metadata_encoded_t EncodeMeta(bitpacking_metadata_t metadata) {
	D_ASSERT(metadata.offset <= 0x00FFFFFF); // max uint24_t
	bitpacking_metadata_encoded_t encoded_value = metadata.offset;
	encoded_value |= UnsafeNumericCast<bitpacking_metadata_encoded_t>((uint8_t)metadata.mode << 24);
	return encoded_value;
}
static bitpacking_metadata_t DecodeMeta(bitpacking_metadata_encoded_t *metadata_encoded) {
	bitpacking_metadata_t metadata;
	metadata.mode = static_cast<BitpackingMode>((*metadata_encoded >> 24) & 0xFF);
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

	bool has_valid;
	bool has_invalid;

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
		has_valid = false;
		has_invalid = false;
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
		// 		 we could support this with some clever subtract trickery?
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
		static_assert(NumericLimits<T_INNER>::IsIntegral(), "Integral type required.");

		using T_U = typename MakeUnsigned<T_INNER>::type;

		for (idx_t i = 0; i < compression_buffer_idx; i++) {
			reinterpret_cast<T_U *>(buffer)[i] -= static_cast<T_U>(frame_of_reference);
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
			auto delta_required_bitwidth =
			    BitpackingPrimitives::MinimumBitWidth<T, false>(static_cast<T>(min_max_delta_diff));
			auto regular_required_bitwidth = BitpackingPrimitives::MinimumBitWidth(min_max_diff);

			//! `min_max_diff` is uninitialized if `can_do_for` isn't true
			bool prefer_for = can_do_for && delta_required_bitwidth >= regular_required_bitwidth;

			if (!prefer_for && mode != BitpackingMode::FOR) {
				SubtractFrameOfReference(delta_buffer, minimum_delta);

				OP::WriteDeltaFor(reinterpret_cast<T *>(delta_buffer), compression_buffer_validity,
				                  delta_required_bitwidth, static_cast<T>(minimum_delta), delta_offset,
				                  compression_buffer, compression_buffer_idx, data_ptr);

				// FOR (frame of reference).
				total_size += sizeof(T);
				// Aligned bitpacking width.
				total_size += AlignValue(sizeof(bitpacking_width_t));
				// Delta offset.
				total_size += sizeof(T);
				// Compressed data size.
				total_size += BitpackingPrimitives::GetRequiredSize(compression_buffer_idx, delta_required_bitwidth);

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
	bool Update(typename VectorIterator<T>::ValueEntry val) {
		auto is_valid = val.IsValid();
		compression_buffer_validity[compression_buffer_idx] = is_valid;
		has_valid = has_valid || is_valid;
		has_invalid = has_invalid || !is_valid;
		all_valid = all_valid && is_valid;
		all_invalid = all_invalid && !is_valid;

		if (is_valid) {
			auto value = val.GetValue();
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
	explicit BitpackingAnalyzeState(BlockManager &block_manager) : AnalyzeState(block_manager) {};
	BitpackingState<T> state;
};

template <class T>
unique_ptr<AnalyzeState> BitpackingInitAnalyze(ColumnData &col_data, PhysicalType type) {
	auto state = make_uniq<BitpackingAnalyzeState<T>>(col_data.GetBlockManager());
	state->state.mode = Settings::Get<ForceBitpackingModeSetting>(col_data.GetDatabase());

	return std::move(state);
}

template <class T>
bool BitpackingAnalyze(AnalyzeState &state, const Vector &input) {
	// We use BITPACKING_METADATA_GROUP_SIZE tuples, which can exceed the block size.
	// In that case, we disable bitpacking.
	// we are conservative here by multiplying by 2
	auto type_size = GetTypeIdSize(input.GetType().InternalType());
	if (type_size * BITPACKING_METADATA_GROUP_SIZE * 2 > state.info.GetBlockSize()) {
		return false;
	}

	auto &analyze_state = state.Cast<BitpackingAnalyzeState<T>>();
	for (auto entry : input.Values<T>()) {
		if (!analyze_state.state.template Update<EmptyBitpackingWriter>(entry)) {
			return false;
		}
	}
	return true;
}

template <class T>
idx_t BitpackingFinalAnalyze(AnalyzeState &state) {
	auto &bitpacking_state = state.Cast<BitpackingAnalyzeState<T>>();
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
struct BitpackingCompressionState : public StandardCompressionState {
public:
	explicit BitpackingCompressionState(ColumnDataCheckpointData &checkpoint_data)
	    : StandardCompressionState(checkpoint_data, CompressionType::COMPRESSION_BITPACKING) {
		CreateEmptySegment();

		state.data_ptr = reinterpret_cast<void *>(this);
		state.mode = Settings::Get<ForceBitpackingModeSetting>(checkpoint_data.GetDatabase());
	}

	StatsWriter<T> stats_writer;
	// Ptr to next free spot in segment;
	data_ptr_t data_ptr;
	// Ptr to next free spot for storing bitwidths and frame-of-references (growing downwards).
	data_ptr_t metadata_ptr;

	BitpackingState<T> state;

public:
	struct BitpackingWriter {
		static void WriteConstant(T constant, idx_t count, void *data_ptr, bool all_invalid) {
			auto state = reinterpret_cast<BitpackingCompressionState<T, WRITE_STATISTICS> *>(data_ptr);

			ReserveSpace(state, sizeof(T));
			WriteMetaData(state, BitpackingMode::CONSTANT);
			WriteData(state->data_ptr, constant);

			UpdateStats(state, count);
		}

		static void WriteConstantDelta(T_S constant, T frame_of_reference, idx_t count, T *values, bool *validity,
		                               void *data_ptr) {
			auto state = reinterpret_cast<BitpackingCompressionState<T, WRITE_STATISTICS> *>(data_ptr);

			ReserveSpace(state, 2 * sizeof(T));
			WriteMetaData(state, BitpackingMode::CONSTANT_DELTA);
			WriteData(state->data_ptr, frame_of_reference);
			WriteData(state->data_ptr, constant);

			UpdateStats(state, count);
		}
		static void WriteDeltaFor(T *values, bool *validity, bitpacking_width_t width, T frame_of_reference,
		                          T_S delta_offset, T *original_values, idx_t count, void *data_ptr) {
			auto state = reinterpret_cast<BitpackingCompressionState<T, WRITE_STATISTICS> *>(data_ptr);

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
			auto state = reinterpret_cast<BitpackingCompressionState<T, WRITE_STATISTICS> *>(data_ptr);

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

		static void WriteMetaData(BitpackingCompressionState<T, WRITE_STATISTICS> *state, BitpackingMode mode) {
			bitpacking_metadata_t metadata {mode, (uint32_t)(state->data_ptr - state->handle.GetDataMutable())};
			state->metadata_ptr -= sizeof(bitpacking_metadata_encoded_t);
			Store<bitpacking_metadata_encoded_t>(EncodeMeta(metadata), state->metadata_ptr);
		}

		static void ReserveSpace(BitpackingCompressionState<T, WRITE_STATISTICS> *state, idx_t data_bytes) {
			idx_t meta_bytes = sizeof(bitpacking_metadata_encoded_t);
			state->FlushAndCreateSegmentIfFull(data_bytes, meta_bytes);
			D_ASSERT(state->CanStore(data_bytes, meta_bytes));
		}

		static void UpdateStats(BitpackingCompressionState<T, WRITE_STATISTICS> *state, idx_t count) {
			state->current_segment->count += count;

			if (WRITE_STATISTICS) {
				auto &stats_writer = state->stats_writer;
				if (state->state.has_valid) {
					stats_writer.SetHasValid();
					stats_writer.UpdateMinMax(state->state.minimum);
					stats_writer.UpdateMinMax(state->state.maximum);
				}
				if (state->state.has_invalid) {
					stats_writer.SetHasNull();
				}
			}
		}
	};

	bool CanStore(idx_t data_bytes, idx_t meta_bytes) {
		auto required_data_bytes = AlignValue<idx_t>(UnsafeNumericCast<idx_t>((data_ptr + data_bytes) - data_ptr));
		auto required_meta_bytes = info.GetBlockSize() - UnsafeNumericCast<idx_t>(metadata_ptr - data_ptr) + meta_bytes;

		return required_data_bytes + required_meta_bytes <=
		       info.GetBlockSize() - BitpackingPrimitives::BITPACKING_HEADER_SIZE;
	}

	void CreateEmptySegment() {
		CreateAndPinNewSegment();

		data_ptr = handle.GetDataMutable() + BitpackingPrimitives::BITPACKING_HEADER_SIZE;
		metadata_ptr = handle.GetDataMutable() + info.GetBlockSize();
	}

	void Append(const Vector &input) {
		for (auto entry : input.Values<T>()) {
			state.template Update<BitpackingWriter>(entry);
		}
	}

	void FlushAndCreateSegmentIfFull(idx_t required_data_bytes, idx_t required_meta_bytes) {
		if (!CanStore(required_data_bytes, required_meta_bytes)) {
			FlushSegment();
			CreateEmptySegment();
		}
	}

	void FlushSegment() {
		auto base_ptr = handle.GetDataMutable();

		// Compact the segment by moving the metadata next to the data.

		idx_t unaligned_offset = NumericCast<idx_t>(data_ptr - base_ptr);
		idx_t metadata_offset = AlignValue(unaligned_offset);
		idx_t metadata_size = NumericCast<idx_t>(base_ptr + info.GetBlockSize() - metadata_ptr);
		idx_t total_segment_size = metadata_offset + metadata_size;

		// Asserting things are still sane here
		if (!CanStore(0, 0)) {
			throw InternalException("Error in bitpacking size calculation");
		}

		if (unaligned_offset != metadata_offset) {
			// zero initialize any padding bits
			memset(base_ptr + unaligned_offset, 0, metadata_offset - unaligned_offset);
		}
		memmove(base_ptr + metadata_offset, metadata_ptr, metadata_size);

		// Store the offset of the metadata of the first group (which is at the highest address).
		Store<idx_t>(metadata_offset + metadata_size, base_ptr);
		FlushCurrentSegment(stats_writer, total_segment_size);
	}

	void Finalize() {
		state.template Flush<BitpackingCompressionState<T, WRITE_STATISTICS, T_S>::BitpackingWriter>();
		FlushSegment();
		current_segment.reset();
	}
};

template <class T, bool WRITE_STATISTICS>
unique_ptr<CompressionState> BitpackingInitCompression(ColumnDataCheckpointData &checkpoint_data,
                                                       unique_ptr<AnalyzeState> state) {
	return make_uniq<BitpackingCompressionState<T, WRITE_STATISTICS>>(checkpoint_data);
}

template <class T, bool WRITE_STATISTICS>
void BitpackingCompress(CompressionState &state_p, const Vector &scan_vector) {
	auto &state = state_p.Cast<BitpackingCompressionState<T, WRITE_STATISTICS>>();
	state.Append(scan_vector);
}

template <class T, bool WRITE_STATISTICS>
void BitpackingFinalizeCompress(CompressionState &state_p) {
	auto &state = state_p.Cast<BitpackingCompressionState<T, WRITE_STATISTICS>>();
	state.Finalize();
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
template <class T>
static T DeltaDecode(T *data, T previous_value, const size_t size) {
	D_ASSERT(size >= 1);
#if DUCKDB_AUTOVEC_X86
	// 4-lane prefix sum: one add+splat per 4 values instead of per value (1.6x on x86; a loss on NEON)
	if constexpr (sizeof(T) == 4) {
		auto lanes = reinterpret_cast<uint32_t *>(data);
		duckdb_av_u32x4 run = duckdb_av_u32x4 {} + static_cast<uint32_t>(previous_value);
		size_t k = 0;
		for (; k + 4 <= size; k += 4) {
			duckdb_av_u32x4 v;
			memcpy(&v, lanes + k, 16);
			v = PrefixSum(v) + run;
			memcpy(lanes + k, &v, 16);
			run = duckdb_av_u32x4 {} + v[3];
		}
		uint32_t prev = run[0];
		for (; k < size; k++) {
			prev = lanes[k] += prev;
		}
		return static_cast<T>(prev);
	}
#endif
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
	explicit BitpackingScanState(const QueryContext &context, ColumnSegment &segment) : current_segment(segment) {
		auto &buffer_manager = BufferManager::GetBufferManager(segment.GetDatabase());
		handle = buffer_manager.Pin(context, segment.GetBlockHandle());
		auto data_ptr = handle.GetDataMutable();

		// load offset to bitpacking widths pointer
		auto bitpacking_metadata_offset = Load<idx_t>(data_ptr + segment.GetBlockOffset());
		bitpacking_metadata_ptr =
		    data_ptr + segment.GetBlockOffset() + bitpacking_metadata_offset - sizeof(bitpacking_metadata_encoded_t);
		if (bitpacking_metadata_ptr >= handle.GetDataMutable() + current_segment.GetBlockSize()) {
			throw InternalException("Bitpacking offset is out of range at block \"%llu\" - corrupt database file",
			                        segment.GetBlockHandle()->BlockId());
		}

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

	//! Keepalive: set once a chunk's FOR payload went unexploited, stopping FOR emission from then on

public:
	//! Loads the metadata for the current metadata group. This will set bitpacking_metadata_ptr to the next group.
	//! It also loads any metadata at the start of a compressed buffer (e.g. the width, for, or constant value)
	//! depending on the bitpacking mode of that group.
	void LoadNextGroup() {
		D_ASSERT(bitpacking_metadata_ptr > handle.GetDataMutable() &&
		         (bitpacking_metadata_ptr < handle.GetDataMutable() + current_segment.GetBlockSize()));
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
		idx_t initial_group_offset = current_group_offset;

		// This skips straight to the correct metadata group
		idx_t meta_groups_to_skip = (skip_count + current_group_offset) / BITPACKING_METADATA_GROUP_SIZE;
		if (meta_groups_to_skip) {
			// bitpacking_metadata_ptr points to the next metadata: this means we need to advance the pointer by n-1
			bitpacking_metadata_ptr -= (meta_groups_to_skip - 1) * sizeof(bitpacking_metadata_encoded_t);
			LoadNextGroup();
			// The first (partial) group we skipped
			skipped += BITPACKING_METADATA_GROUP_SIZE - initial_group_offset;
			// The remaining groups that were skipped
			skipped += (meta_groups_to_skip - 1) * BITPACKING_METADATA_GROUP_SIZE;
		}

		// Assert we can are in the correct metadata group
		idx_t remaining_to_skip = skip_count - skipped;
		D_ASSERT(current_group_offset + remaining_to_skip < BITPACKING_METADATA_GROUP_SIZE);

		if (current_group.mode == BitpackingMode::CONSTANT || current_group.mode == BitpackingMode::CONSTANT_DELTA ||
		    current_group.mode == BitpackingMode::FOR) {
			// Skipping within a constant or constant delta is done by increasing the current_group_offset
			skipped += remaining_to_skip;
			current_group_offset += remaining_to_skip;
		} else {
			// For DELTA we actually need to decompress from the current_group_offset up until the row we want to skip
			// to this is because we need that delta to be able to continue scanning from here
			D_ASSERT(current_group.mode == BitpackingMode::DELTA_FOR);

			while (skipped < skip_count) {
				// Calculate compression group offset and pointer
				idx_t offset_in_compression_group =
				    current_group_offset % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;
				data_ptr_t current_position_ptr = current_group_ptr + current_group_offset * current_width / 8;
				data_ptr_t decompression_group_start_pointer =
				    current_position_ptr - offset_in_compression_group * current_width / 8;

				idx_t skipping_this_algorithm_group =
				    MinValue(remaining_to_skip,
				             BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE - offset_in_compression_group);

				BitpackingPrimitives::UnPackBlock<T>(data_ptr_cast(decompression_buffer),
				                                     decompression_group_start_pointer, current_width,
				                                     skip_sign_extend);

				T *decompression_ptr = decompression_buffer + offset_in_compression_group;
				BitpackingPrimitives::ApplyFrameOfReference<T_S>(reinterpret_cast<T_S *>(decompression_ptr),
				                                                 static_cast<T_S>(current_frame_of_reference),
				                                                 skipping_this_algorithm_group);
				DeltaDecode<T_S>(reinterpret_cast<T_S *>(decompression_ptr), static_cast<T_S>(current_delta_offset),
				                 skipping_this_algorithm_group);
				current_delta_offset = decompression_ptr[skipping_this_algorithm_group - 1];

				skipped += skipping_this_algorithm_group;
				current_group_offset += skipping_this_algorithm_group;
				remaining_to_skip -= skipping_this_algorithm_group;
			}
		}

		D_ASSERT(skipped == skip_count);
	}

	data_ptr_t GetPtr(bitpacking_metadata_t group) {
		return handle.GetDataMutable() + current_segment.GetBlockOffset() + group.offset;
	}
};

template <class T>
unique_ptr<SegmentScanState> BitpackingInitScan(const QueryContext &context, ColumnSegment &segment) {
	auto result = make_uniq<BitpackingScanState<T>>(context, segment);
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//
//! Byte width that holds this group's absolute values, or 0 when nothing is narrower than T.
//! The frame is folded into the payload, so the narrow type must cover frame + the widest delta.
template <class T>
static idx_t FORStoredSize(T frame, bitpacking_width_t width) {
	if (sizeof(T) > sizeof(uint64_t) || width == 0 || width >= sizeof(T) * 8 || frame < 0) {
		return 0; // 16-byte payloads stay on the wide path
	}
	const uint64_t max_delta = (uint64_t(1) << width) - 1;
	const auto unsigned_frame = static_cast<uint64_t>(frame);
	if (unsigned_frame > NumericLimits<uint64_t>::Maximum() - max_delta) {
		return 0;
	}
	const uint64_t group_max = unsigned_frame + max_delta;
	const idx_t size = group_max <= 0xFF ? 1 : (group_max <= 0xFFFF ? 2 : (group_max <= 0xFFFFFFFF ? 4 : 8));
	return size < sizeof(T) ? size : 0;
}

//! FOR emission state: accumulates a narrow payload across metadata groups, straight into the result's own buffer.
//! Groups usually agree on the stored width; a group that does not fit //! abandons FOR by widening what was written so
//! far.
template <class T, class T_S = typename MakeSigned<T>::type>
struct FORScanTarget {
	FORScanTarget(BitpackingScanState<T> &scan_state_p, Vector &result_p, idx_t result_offset, idx_t scan_count,
	              bool allow_for)
	    : scan_state(scan_state_p), result(result_p) {
		active = allow_for && result_offset == 0 && sizeof(T) > 1 && CpuBenefitsFromAutoVec() &&
		         scan_count <= STANDARD_VECTOR_SIZE && result.GetBufferRef()->cache_owned &&
		         ForVector::TokenSet(result);
	}

	//! Can this group's run join the narrow payload? Fixes the stored width on the first accepted group.
	bool Accept(idx_t offset_in_compression_group, idx_t remaining) {
		if (!active || offset_in_compression_group != 0) {
			return false; // mid-algorithm-group runs would need a scratch decode: not worth the code
		}
		if (remaining % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE != 0) {
			return false; // the unpack kernel only decodes whole algorithm groups; a partial tail needs the scratch
			              // path
		}
		const auto size = FORStoredSize<T>(scan_state.current_frame_of_reference, scan_state.current_width);
		if (size == 0 || (stored_size != 0 && size != stored_size)) {
			return false;
		}
		stored_size = size;
		const uint64_t group_max = static_cast<uint64_t>(scan_state.current_frame_of_reference) +
		                           ((uint64_t(1) << scan_state.current_width) - 1);
		max_stored = MaxValue(max_stored, group_max);
		return true;
	}

	//! The same unpack kernel, instantiated at the stored width, with the frame folded in
	void Decode(data_ptr_t payload, idx_t written, idx_t count) {
		auto src = scan_state.current_group_ptr + scan_state.current_group_offset * scan_state.current_width / 8;
		auto dst = payload + written * stored_size;
		const auto width = scan_state.current_width;
		const auto frame = static_cast<uint64_t>(scan_state.current_frame_of_reference);
		switch (stored_size) {
		case 1:
			return BitpackingPrimitives::UnPackBuffer<uint8_t>(dst, src, count, width, true,
			                                                   static_cast<uint8_t>(frame));
		case 2:
			return BitpackingPrimitives::UnPackBuffer<uint16_t>(dst, src, count, width, true,
			                                                    static_cast<uint16_t>(frame));
		default:
			return BitpackingPrimitives::UnPackBuffer<uint32_t>(dst, src, count, width, true,
			                                                    static_cast<uint32_t>(frame));
		}
	}

	//! A delta group only narrows to 4 bytes: that is the width for a SIMD prefix sum. Check if cannot exceed this
	//! bound
	bool AcceptDelta(idx_t offset_in_compression_group, idx_t remaining) {
		// exactly 8 bytes: 16-byte payloads use a different packing layout, and 4 bytes has nothing to narrow to
		if (!active || offset_in_compression_group != 0 || sizeof(T) != 8) {
			return false;
		}
		if (remaining % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE != 0) {
			return false;
		}
		if (stored_size != 0 && stored_size != 4) {
			return false; // cannot join a payload already fixed at a different width
		}
		const auto base = static_cast<T_S>(scan_state.current_delta_offset);
		const auto frame = static_cast<T_S>(scan_state.current_frame_of_reference);
		if (base < 0 || frame < 0 || scan_state.current_width >= 32) {
			return false;
		}
		const auto horizon = static_cast<T_S>(BITPACKING_METADATA_GROUP_SIZE - scan_state.current_group_offset);
		T_S max_delta, span, upper;
		if (!TryAddOperator::Operation(frame, static_cast<T_S>((uint64_t(1) << scan_state.current_width) - 1),
		                               max_delta) ||
		    !TryMultiplyOperator::Operation(horizon, max_delta, span) ||
		    !TryAddOperator::Operation(base, span, upper) ||
		    upper > static_cast<T_S>(NumericLimits<uint32_t>::Maximum())) {
			return false;
		}
		stored_size = 4;
		return true;
	}

	//! Unpack at 4 bytes and run the prefix sum there. Values are monotonic  (frame >= 0): last one is the group's max
	void DecodeDelta(data_ptr_t payload, idx_t written, idx_t count) {
		auto src = scan_state.current_group_ptr + scan_state.current_group_offset * scan_state.current_width / 8;
		auto dst = reinterpret_cast<uint32_t *>(payload + written * stored_size);
		BitpackingPrimitives::UnPackBuffer<uint32_t>(data_ptr_cast(dst), src, count, scan_state.current_width, true,
		                                             static_cast<uint32_t>(scan_state.current_frame_of_reference));
		const auto last = DeltaDecode<uint32_t>(dst, static_cast<uint32_t>(scan_state.current_delta_offset), count);
		scan_state.current_delta_offset = static_cast<T>(last);
		max_stored = MaxValue<uint64_t>(max_stored, last);
	}

	void Abandon(data_ptr_t payload, idx_t written) {
		if (stored_size != 0) { // Give up on FOR: widen everything written so far in place and stop trying
			ForVector::WidenInPlace(payload, StoredPhysicalType(), GetTypeId<T>(), written);
			stored_size = 0;
		}
		active = false;
	}

	void Publish(idx_t count) {
		if (stored_size) { // Mark the result as FOR, leaving the narrow payload where it was written
			ForVector::Create(result, StoredPhysicalType(), max_stored, count);
		}
	}

	PhysicalType StoredPhysicalType() const {
		return stored_size == 1 ? PhysicalType::UINT8
		                        : (stored_size == 2 ? PhysicalType::UINT16 : PhysicalType::UINT32);
	}

	BitpackingScanState<T> &scan_state;
	Vector &result;
	bool active;
	idx_t stored_size = 0;
	uint64_t max_stored = 0;
};

template <class T, class T_S = typename MakeSigned<T>::type, class T_U = typename MakeUnsigned<T>::type>
void BitpackingScanImpl(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
                        idx_t result_offset, bool allow_for) {
	auto &scan_state = state.scan_state->Cast<BitpackingScanState<T>>();

	ForVector::Widen(result); // a payload left in a reused result must go before we write at the logical stride

	T *result_data = FlatVector::GetDataMutable<T>(result);
	result.SetVectorType(VectorType::FLAT_VECTOR);
	FORScanTarget<T, T_S> for_target(scan_state, result, result_offset, scan_count, allow_for);
	auto payload = data_ptr_cast(result_data);

	bool skip_sign_extend = true; //! FOR values are >= 0, so we can always skip sign extension here

	idx_t scanned = 0;
	while (scanned < scan_count) {
		D_ASSERT(scan_state.current_group_offset <= BITPACKING_METADATA_GROUP_SIZE);

		if (scan_state.current_group_offset == BITPACKING_METADATA_GROUP_SIZE) {
			scan_state.LoadNextGroup(); // Exhausted this group, move pointers and load metadata for next group.
		}

		idx_t offset_in_compression_group =
		    scan_state.current_group_offset % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;

		if (scan_state.current_group.mode == BitpackingMode::CONSTANT) {
			for_target.Abandon(payload, scanned);
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
			for_target.Abandon(payload, scanned);
			idx_t remaining = scan_count - scanned;
			idx_t to_scan = MinValue(remaining, BITPACKING_METADATA_GROUP_SIZE - scan_state.current_group_offset);
			T *target_ptr = result_data + result_offset + scanned;

			for (idx_t i = 0; i < to_scan; i++) {
				idx_t multiplier = scan_state.current_group_offset + i;
				// intended static casts to unsigned and back for defined wrapping of integers
				target_ptr[i] = static_cast<T>((static_cast<T_U>(scan_state.current_constant) * multiplier) +
				                               static_cast<T_U>(scan_state.current_frame_of_reference));
			}

			scanned += to_scan;
			scan_state.current_group_offset += to_scan;
			continue;
		}
		D_ASSERT(scan_state.current_group.mode == BitpackingMode::FOR ||
		         scan_state.current_group.mode == BitpackingMode::DELTA_FOR);

		T *current_result_ptr = result_data + result_offset + scanned;
		auto remaining =
		    MinValue<idx_t>(scan_count - scanned, BITPACKING_METADATA_GROUP_SIZE - scan_state.current_group_offset);
		idx_t to_scan = remaining - (remaining % BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE);
		if (remaining > 0) {
			// FOR emission in scan: the narrow payload directly goes downstream instead of being widened here
			if (scan_state.current_group.mode == BitpackingMode::FOR &&
			    for_target.Accept(offset_in_compression_group, remaining)) {
				for_target.Decode(payload, scanned, remaining);
				scanned += remaining;
				scan_state.current_group_offset += remaining;
				continue;
			}
			if (scan_state.current_group.mode == BitpackingMode::DELTA_FOR &&
			    for_target.AcceptDelta(offset_in_compression_group, remaining)) {
				for_target.DecodeDelta(payload, scanned, remaining);
				scanned += remaining;
				scan_state.current_group_offset += remaining;
				continue;
			}
		}
		for_target.Abandon(payload, scanned);
		if (offset_in_compression_group == 0 && to_scan > 0) {
			// Decompress whole algorithm groups straight into the result, adding the frame inline
			BitpackingPrimitives::UnPackBuffer<T>(
			    data_ptr_cast(current_result_ptr),
			    scan_state.current_group_ptr + scan_state.current_group_offset * scan_state.current_width / 8, to_scan,
			    scan_state.current_width, skip_sign_extend, scan_state.current_frame_of_reference);
		} else {
			// Decompress one algorithm group to buffer, copy out the requested slice
			to_scan = MinValue<idx_t>(scan_count - scanned, BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE -
			                                                    offset_in_compression_group);
			BitpackingPrimitives::UnPackBlock<T>(data_ptr_cast(scan_state.decompression_buffer),
			                                     scan_state.current_group_ptr +
			                                         (scan_state.current_group_offset - offset_in_compression_group) *
			                                             scan_state.current_width / 8,
			                                     scan_state.current_width, skip_sign_extend);
			memcpy(current_result_ptr, scan_state.decompression_buffer + offset_in_compression_group,
			       to_scan * sizeof(T));
			BitpackingPrimitives::ApplyFrameOfReference<T>(current_result_ptr, scan_state.current_frame_of_reference,
			                                               to_scan);
		}

		if (scan_state.current_group.mode == BitpackingMode::DELTA_FOR) {
			DeltaDecode<T_S>(reinterpret_cast<T_S *>(current_result_ptr),
			                 static_cast<T_S>(scan_state.current_delta_offset), to_scan);
			scan_state.current_delta_offset = current_result_ptr[to_scan - 1];
		}

		scanned += to_scan;
		scan_state.current_group_offset += to_scan;
	}

	for_target.Publish(scan_count);
}

template <class T>
void BitpackingScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
                           idx_t result_offset) {
	BitpackingScanImpl<T>(segment, state, scan_count, result, result_offset, false); // partial scan never emits FOR
}

template <class T>
void BitpackingScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result) {
	BitpackingScanImpl<T>(segment, state, scan_count, result, 0, true); // only entire-vector scan below may publish FOR
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
template <class T>
void BitpackingFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result,
                        idx_t result_idx) {
	BitpackingScanState<T> scan_state(state.context, segment);
	scan_state.Skip(segment, NumericCast<idx_t>(row_id));

	D_ASSERT(scan_state.current_group_offset < BITPACKING_METADATA_GROUP_SIZE);

	D_ASSERT(result.GetVectorType() == VectorType::FLAT_VECTOR);
	T *result_data = FlatVector::GetDataMutable<T>(result);
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
		T multiplier;
		auto cast = TryCast::Operation<idx_t, T>(scan_state.current_group_offset, multiplier);
		(void)cast;
		D_ASSERT(cast);
#ifdef DEBUG
		// overflow check
		T result;
		bool multiply = TryMultiplyOperator::Operation(multiplier, scan_state.current_constant, result);
		bool add = TryAddOperator::Operation(result, scan_state.current_frame_of_reference, result);
		D_ASSERT(multiply && add);
#endif
		*current_result_ptr = (multiplier * scan_state.current_constant) + scan_state.current_frame_of_reference;
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
// GetSegmentInfo
//===--------------------------------------------------------------------===//
template <class T>
InsertionOrderPreservingMap<string> BitpackingGetSegmentInfo(QueryContext context, ColumnSegment &segment) {
	map<BitpackingMode, idx_t> counts;
	auto tuple_count = segment.count.load();
	BitpackingScanState<T> scan_state(context, segment);
	for (idx_t i = 0; i < tuple_count; i += BITPACKING_METADATA_GROUP_SIZE) {
		if (i) {
			scan_state.LoadNextGroup();
		}
		counts[scan_state.current_group.mode]++;
	}

	InsertionOrderPreservingMap<string> result;
	for (auto &it : counts) {
		auto &mode = it.first;
		auto &count = it.second;
		result[EnumUtil::ToString(mode)] = StringUtil::Format("%d", count);
	}
	return result;
}

//===--------------------------------------------------------------------===//
// Get Function
//===--------------------------------------------------------------------===//
template <class T, bool WRITE_STATISTICS = true>
CompressionFunction GetBitpackingFunction(PhysicalType data_type) {
	auto bitpacking = CompressionFunction(
	    CompressionType::COMPRESSION_BITPACKING, data_type, BitpackingInitAnalyze<T>, BitpackingAnalyze<T>,
	    BitpackingFinalAnalyze<T>, BitpackingInitCompression<T, WRITE_STATISTICS>,
	    BitpackingCompress<T, WRITE_STATISTICS>, BitpackingFinalizeCompress<T, WRITE_STATISTICS>, BitpackingInitScan<T>,
	    BitpackingScan<T>, BitpackingScanPartial<T>, BitpackingFetchRow<T>, BitpackingSkip<T>);
	bitpacking.get_segment_info = BitpackingGetSegmentInfo<T>;
	return bitpacking;
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
	case PhysicalType::UINT128:
		return GetBitpackingFunction<uhugeint_t>(type);
	case PhysicalType::LIST:
		return GetBitpackingFunction<uint64_t, false>(type);
	default:
		throw InternalException("Unsupported type for Bitpacking");
	}
}

bool BitpackingFun::TypeIsSupported(const PhysicalType physical_type) {
	switch (physical_type) {
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
	case PhysicalType::UINT128:
		return true;
	default:
		return false;
	}
}

} // namespace duckdb
