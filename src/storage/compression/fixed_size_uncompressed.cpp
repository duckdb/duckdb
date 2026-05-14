#include "duckdb/common/bit_utils.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/checkpoint/write_overflow_strings_to_disk.hpp"
#include "duckdb/storage/segment/uncompressed.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/column_data_checkpointer.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/storage/table/scan_state.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// Analyze
//===--------------------------------------------------------------------===//
struct FixedSizeAnalyzeState : public AnalyzeState {
	explicit FixedSizeAnalyzeState(const CompressionInfo &info) : AnalyzeState(info), count(0) {
	}

	idx_t count;
};

unique_ptr<AnalyzeState> FixedSizeInitAnalyze(ColumnData &col_data, PhysicalType type) {
	CompressionInfo info(col_data.GetBlockManager());
	return make_uniq<FixedSizeAnalyzeState>(info);
}

bool FixedSizeAnalyze(AnalyzeState &state_p, Vector &input, idx_t count) {
	auto &state = state_p.Cast<FixedSizeAnalyzeState>();
	state.count += count;
	return true;
}

template <class T>
idx_t FixedSizeFinalAnalyze(AnalyzeState &state_p) {
	auto &state = state_p.template Cast<FixedSizeAnalyzeState>();
	return sizeof(T) * state.count;
}

//===--------------------------------------------------------------------===//
// Compress
//===--------------------------------------------------------------------===//
struct UncompressedCompressState : public CompressionState {
public:
	UncompressedCompressState(ColumnDataCheckpointData &checkpoint_data, const CompressionInfo &info);

public:
	virtual void CreateEmptySegment();
	void FlushSegment(idx_t segment_size);
	void Finalize(idx_t segment_size);

public:
	ColumnDataCheckpointData &checkpoint_data;
	const CompressionFunction &function;
	unique_ptr<ColumnSegment> current_segment;
	ColumnAppendState append_state;
};

UncompressedCompressState::UncompressedCompressState(ColumnDataCheckpointData &checkpoint_data,
                                                     const CompressionInfo &info)
    : CompressionState(info), checkpoint_data(checkpoint_data),
      function(checkpoint_data.GetCompressionFunction(CompressionType::COMPRESSION_UNCOMPRESSED)) {
	UncompressedCompressState::CreateEmptySegment();
}

void UncompressedCompressState::CreateEmptySegment() {
	auto &db = checkpoint_data.GetDatabase();
	auto &type = checkpoint_data.GetType();

	auto compressed_segment =
	    ColumnSegment::CreateTransientSegment(db, function, type, info.GetBlockSize(), info.GetBlockManager());
	if (type.InternalType() == PhysicalType::VARCHAR) {
		auto &state = compressed_segment->GetSegmentState()->Cast<UncompressedStringSegmentState>();
		auto &storage_manager = checkpoint_data.GetStorageManager();
		if (!storage_manager.InMemory()) {
			auto &partial_block_manager = checkpoint_data.GetCheckpointState().GetPartialBlockManager();
			state.block_manager = partial_block_manager.GetBlockManager();
			state.overflow_writer = make_uniq<WriteOverflowStringsToDisk>(partial_block_manager);
		}
	}
	current_segment = std::move(compressed_segment);
	current_segment->InitializeAppend(append_state);
}

void UncompressedCompressState::FlushSegment(idx_t segment_size) {
	auto &state = checkpoint_data.GetCheckpointState();
	if (current_segment->type.InternalType() == PhysicalType::VARCHAR) {
		auto &segment_state = current_segment->GetSegmentState()->Cast<UncompressedStringSegmentState>();
		if (segment_state.overflow_writer) {
			segment_state.overflow_writer->Flush();
			segment_state.overflow_writer.reset();
		}
	}
	append_state.child_appends.clear();
	append_state.append_state.reset();
	append_state.lock.reset();
	state.FlushSegmentInternal(std::move(current_segment), segment_size);
}

void UncompressedCompressState::Finalize(idx_t segment_size) {
	FlushSegment(segment_size);
	current_segment.reset();
}

unique_ptr<CompressionState> UncompressedFunctions::InitCompression(ColumnDataCheckpointData &checkpoint_data,
                                                                    unique_ptr<AnalyzeState> state) {
	return make_uniq<UncompressedCompressState>(checkpoint_data, state->info);
}

void UncompressedFunctions::Compress(CompressionState &state_p, Vector &data, idx_t count) {
	auto &state = state_p.Cast<UncompressedCompressState>();
	UnifiedVectorFormat vdata;
	data.ToUnifiedFormat(vdata);

	idx_t offset = 0;
	while (count > 0) {
		idx_t appended = state.current_segment->Append(state.append_state, vdata, offset, count);
		if (appended == count) {
			// appended everything: finished
			return;
		}
		// the segment is full: flush it to disk
		state.FlushSegment(state.current_segment->FinalizeAppend(state.append_state));

		// now create a new segment and continue appending
		state.CreateEmptySegment();
		offset += appended;
		count -= appended;
	}
}

void UncompressedFunctions::FinalizeCompress(CompressionState &state_p) {
	auto &state = state_p.Cast<UncompressedCompressState>();
	state.Finalize(state.current_segment->FinalizeAppend(state.append_state));
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
struct FixedSizeScanState : public SegmentScanState {
	BufferHandle handle;
};

unique_ptr<SegmentScanState> FixedSizeInitScan(const QueryContext &context, ColumnSegment &segment) {
	auto result = make_uniq<FixedSizeScanState>();
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	result->handle = buffer_manager.Pin(context, segment.block);
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//
template <class T>
void FixedSizeScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
                          idx_t result_offset) {
	auto &scan_state = state.scan_state->Cast<FixedSizeScanState>();
	auto start = state.GetPositionInSegment();

	auto data = scan_state.handle.GetDataMutable() + segment.GetBlockOffset();
	auto source_data = data + start * sizeof(T);

	// copy the data from the base table
	result.SetVectorType(VectorType::FLAT_VECTOR);
	memcpy(FlatVector::GetDataMutable(result) + result_offset * sizeof(T), source_data, scan_count * sizeof(T));
}

template <class T>
void FixedSizeScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result) {
	auto &scan_state = state.scan_state->template Cast<FixedSizeScanState>();
	auto start = state.GetPositionInSegment();

	auto data = scan_state.handle.GetDataMutable() + segment.GetBlockOffset();
	auto source_data = data + start * sizeof(T);

	result.SetVectorType(VectorType::FLAT_VECTOR);
	FlatVector::SetData(result, source_data, count_t(scan_count));
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
template <class T>
void FixedSizeFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result,
                       idx_t result_idx) {
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	auto handle = buffer_manager.Pin(segment.block);

	// first fetch the data from the base table
	auto data_ptr = handle.GetDataMutable() + segment.GetBlockOffset() + NumericCast<idx_t>(row_id) * sizeof(T);

	memcpy(FlatVector::GetDataMutable(result) + result_idx * sizeof(T), data_ptr, sizeof(T));
}

//===--------------------------------------------------------------------===//
// Append
//===--------------------------------------------------------------------===//
static unique_ptr<CompressionAppendState> FixedSizeInitAppend(ColumnSegment &segment) {
	auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
	auto handle = buffer_manager.Pin(segment.block);
	return make_uniq<CompressionAppendState>(std::move(handle));
}

struct FixedSizeGenericContiguousAppendTag {};
struct FixedSizePrimitiveIntegerContiguousAppendTag {};
struct FixedSizeFloatingPointContiguousAppendTag {};

template <class T>
struct FixedSizeContiguousAppendTag {
	using type = FixedSizeGenericContiguousAppendTag;
};

#define DUCKDB_FIXED_SIZE_PRIMITIVE_INTEGER_APPEND_TYPE(T)                                                             \
	template <>                                                                                                        \
	struct FixedSizeContiguousAppendTag<T> {                                                                           \
		using type = FixedSizePrimitiveIntegerContiguousAppendTag;                                                     \
	};

DUCKDB_FIXED_SIZE_PRIMITIVE_INTEGER_APPEND_TYPE(int8_t)
DUCKDB_FIXED_SIZE_PRIMITIVE_INTEGER_APPEND_TYPE(int16_t)
DUCKDB_FIXED_SIZE_PRIMITIVE_INTEGER_APPEND_TYPE(int32_t)
DUCKDB_FIXED_SIZE_PRIMITIVE_INTEGER_APPEND_TYPE(int64_t)
DUCKDB_FIXED_SIZE_PRIMITIVE_INTEGER_APPEND_TYPE(uint8_t)
DUCKDB_FIXED_SIZE_PRIMITIVE_INTEGER_APPEND_TYPE(uint16_t)
DUCKDB_FIXED_SIZE_PRIMITIVE_INTEGER_APPEND_TYPE(uint32_t)
DUCKDB_FIXED_SIZE_PRIMITIVE_INTEGER_APPEND_TYPE(uint64_t)

#undef DUCKDB_FIXED_SIZE_PRIMITIVE_INTEGER_APPEND_TYPE

template <>
struct FixedSizeContiguousAppendTag<float> {
	using type = FixedSizeFloatingPointContiguousAppendTag;
};

template <>
struct FixedSizeContiguousAppendTag<double> {
	using type = FixedSizeFloatingPointContiguousAppendTag;
};

template <class T>
struct FloatingStatsKeyTraits;

template <>
struct FloatingStatsKeyTraits<float> {
	using WORD = uint32_t;
	static constexpr idx_t EXPONENT_BITS = 8;
	static constexpr idx_t FRACTION_BITS = 23;
};

template <>
struct FloatingStatsKeyTraits<double> {
	using WORD = uint64_t;
	static constexpr idx_t EXPONENT_BITS = 11;
	static constexpr idx_t FRACTION_BITS = 52;
};

template <class T>
struct FloatingStatsKey {
	using TRAITS = FloatingStatsKeyTraits<T>;
	using WORD = typename TRAITS::WORD;

	// Derive the masks from the IEEE 754 binary32/binary64 field widths.
	// sign bit, exponent field, fraction field
	static constexpr WORD SIGN_BIT = WORD(1) << (sizeof(WORD) * 8 - 1);
	static constexpr WORD ABS_MASK = SIGN_BIT - 1;
	static constexpr WORD INF_BITS = ((WORD(1) << TRAITS::EXPONENT_BITS) - 1) << TRAITS::FRACTION_BITS;

	static inline WORD Mask(bool value) {
		return WORD(0) - static_cast<WORD>(value);
	}

	static inline WORD EncodeBits(WORD bits) {
		const auto sign = bits >> (sizeof(WORD) * 8 - 1);
		// Depending on the sign get 0x0000... or 0xFFFF...
		const auto sign_mask = WORD(0) - sign;

		// Positive numbers just have the sign bit flipped, moving them into the upper
		// part of unsigned integer space.
		// Negative numbers have every bit inverted, moving them into the lower part
		// of unsigned integer space with reversed order, so higher magnitude negative
		// values sort before lower magnitude negative values.
		auto key = bits ^ (SIGN_BIT | sign_mask);

		// Clear the sign bit, leaving the exponent and fractional parts
		const auto abs = bits & ABS_MASK;

		// +0.0 and -0.0 are treated the same and will be reconstructed as +0.0
		const auto zero_mask = Mask(abs == 0);
		key = (key & ~zero_mask) | (SIGN_BIT & zero_mask);

		// NaNs sort above everything, so return 0xFFFF... if it's a NaN
		// Note that the particular NaN payload is destroyed
		const auto nan_mask = Mask(abs > INF_BITS);
		return key | nan_mask;
	}

	static inline WORD Encode(T value) {
		return EncodeBits(Load<WORD>(const_data_ptr_cast(&value)));
	}

	static inline T Decode(WORD key) {
		WORD bits;
		if (key & SIGN_BIT) {
			// Positive numbers just need the sign bit flipped back
			bits = key ^ SIGN_BIT;
		} else {
			// Negative numbers need every bit inverted back
			bits = ~key;
		}
		T result;
		Store<WORD>(bits, data_ptr_cast(&result));
		return result;
	}
};

template <class T>
static inline void AppendContiguousFixedSizeValues(BaseStatistics &stats, T *__restrict target,
                                                   const T *__restrict source, idx_t count,
                                                   FixedSizeGenericContiguousAppendTag) {
	for (idx_t i = 0; i < count; i++) {
		const auto value = source[i];
		target[i] = value;
		stats.UpdateNumericStats<T>(value);
	}
}

template <class T>
static inline void AppendContiguousFixedSizeValues(BaseStatistics &stats, T *__restrict target,
                                                   const T *__restrict source, idx_t count,
                                                   FixedSizePrimitiveIntegerContiguousAppendTag) {
	if (count == 0) {
		return;
	}

	T min = NumericLimits<T>::Maximum();
	T max = NumericLimits<T>::Minimum();
	for (idx_t i = 0; i < count; i++) {
		auto value = source[i];
		target[i] = value;
		min = value < min ? value : min;
		max = value > max ? value : max;
	}

	stats.UpdateNumericStats<T>(min);
	stats.UpdateNumericStats<T>(max);
}

template <class T>
static inline void AppendContiguousFixedSizeValues(BaseStatistics &stats, T *__restrict target,
                                                   const T *__restrict source, idx_t count,
                                                   FixedSizeFloatingPointContiguousAppendTag) {
	if (count == 0) {
		return;
	}

	using K = FloatingStatsKey<T>;
	using WORD = typename K::WORD;

	auto min_key = K::Encode(NumericLimits<T>::Maximum());
	auto max_key = K::Encode(NumericLimits<T>::Minimum());
	for (idx_t i = 0; i < count; i++) {
		// Load and store the original float bits
		const auto bits = Load<WORD>(const_data_ptr_cast(source + i));
		Store<WORD>(bits, data_ptr_cast(target + i));

		// Derive a sortable integer key that preserves DuckDB's float ordering and NaN handling.
		const auto key = K::EncodeBits(bits);
		min_key = MinValue<WORD>(min_key, key);
		max_key = MaxValue<WORD>(max_key, key);
	}

	// Decode the reduced min/max keys back to floats for the existing stats API.
	stats.UpdateNumericStats<T>(K::Decode(min_key));
	stats.UpdateNumericStats<T>(K::Decode(max_key));
}

template <class T>
static inline void FillNullValues(T *__restrict target, idx_t count) {
	for (idx_t i = 0; i < count; i++) {
		target[i] = NullValue<T>();
	}
}

template <class T, class TAG>
static inline void AppendContiguousNullableFixedSizeValues(BaseStatistics &stats, T *__restrict target,
                                                           const T *__restrict source, const ValidityMask &validity,
                                                           idx_t source_offset, idx_t count, TAG tag) {
	idx_t local_offset = 0;
	// Process a validity-mask word at a time.
	while (local_offset < count) {
		idx_t entry_idx;
		idx_t idx_in_entry;
		ValidityMask::GetEntryIndex(source_offset + local_offset, entry_idx, idx_in_entry);

		const auto entry = validity.GetValidityEntryUnsafe(entry_idx);

		// Limit the validity bitmask slice to the bits left in the current validity word
		// and the rows left in the append.
		const auto bits_in_entry = MinValue<idx_t>(ValidityMask::BITS_PER_VALUE - idx_in_entry, count - local_offset);
		const auto active_mask = ValidityMask::EntryWithValidBits(bits_in_entry);

		// Shift the current validity bitmask slice down to bit 0 and mask off rows outside the slice.
		const auto valid_bits = (entry >> idx_in_entry) & active_mask;

		if (valid_bits == active_mask) {
			stats.SetHasNoNullFast();
			AppendContiguousFixedSizeValues<T>(stats, target + local_offset, source + local_offset, bits_in_entry, tag);
		} else if (valid_bits == 0) {
			stats.SetHasNullFast();
			FillNullValues<T>(target + local_offset, bits_in_entry);
		} else {
			stats.SetHasNullFast();
			stats.SetHasNoNullFast();

			// Process valid runs between null bits.
			//
			// rows:      0 1 2 3 4 5 6 7
			// valid:     Y Y N Y Y Y N Y
			// null_bits: 0 0 1 0 0 0 1 0
			// iteration: [ 1 ] [  2  ] ^ leftover final valid run

			auto null_bits = (~valid_bits) & active_mask;
			idx_t cursor = 0;
			while (null_bits) {
				// Find the lowest set bit
				const auto null_pos = CountZeros<validity_t>::Trailing(null_bits);
				if (null_pos > cursor) {
					AppendContiguousFixedSizeValues<T>(stats, target + local_offset + cursor,
					                                   source + local_offset + cursor, null_pos - cursor, tag);
				}
				// Insert a NullValue<T> in the null gap for debuggability
				// This value should never be used or read anywhere
				target[local_offset + null_pos] = NullValue<T>();
				// Clear the null bit just processed.
				null_bits &= null_bits - 1;
				// Increment the cursor
				cursor = null_pos + 1;
			}
			if (cursor < bits_in_entry) {
				// Process the final valid run
				AppendContiguousFixedSizeValues<T>(stats, target + local_offset + cursor,
				                                   source + local_offset + cursor, bits_in_entry - cursor, tag);
			}
		}

		local_offset += bits_in_entry;
	}
}

struct StandardFixedSizeAppend {
	template <class T>
	static void Append(SegmentStatistics &stats, data_ptr_t target, idx_t target_offset, UnifiedVectorFormat &adata,
	                   idx_t offset, idx_t count) {
		auto sdata = UnifiedVectorFormat::GetData<T>(adata);
		auto tdata = reinterpret_cast<T *>(target);
		if (!adata.sel->IsSet()) {
			// Contiguous buffer fast path
			auto source = sdata + offset;
			auto target_data = tdata + target_offset;
			if (adata.validity.CannotHaveNull()) {
				stats.statistics.SetHasNoNullFast();
				AppendContiguousFixedSizeValues<T>(stats.statistics, target_data, source, count,
				                                   typename FixedSizeContiguousAppendTag<T>::type());
			} else {
				AppendContiguousNullableFixedSizeValues<T>(stats.statistics, target_data, source, adata.validity,
				                                           offset, count,
				                                           typename FixedSizeContiguousAppendTag<T>::type());
			}
			return;
		} else if (adata.validity.CanHaveNull()) {
			for (idx_t i = 0; i < count; i++) {
				auto source_idx = adata.sel->get_index(offset + i);
				auto target_idx = target_offset + i;
				bool is_null = !adata.validity.RowIsValid(source_idx);
				if (!is_null) {
					stats.statistics.SetHasNoNullFast();
					stats.statistics.UpdateNumericStats<T>(sdata[source_idx]);
					tdata[target_idx] = sdata[source_idx];
				} else {
					stats.statistics.SetHasNullFast();
					// we insert a NullValue<T> in the null gap for debuggability
					// this value should never be used or read anywhere
					tdata[target_idx] = NullValue<T>();
				}
			}
		} else {
			stats.statistics.SetHasNoNullFast();
			for (idx_t i = 0; i < count; i++) {
				auto source_idx = adata.sel->get_index(offset + i);
				auto target_idx = target_offset + i;
				stats.statistics.UpdateNumericStats<T>(sdata[source_idx]);
				tdata[target_idx] = sdata[source_idx];
			}
		}
	}
};

struct ListFixedSizeAppend {
	template <class T>
	static void Append(SegmentStatistics &stats, data_ptr_t target, idx_t target_offset, UnifiedVectorFormat &adata,
	                   idx_t offset, idx_t count) {
		auto sdata = UnifiedVectorFormat::GetData<uint64_t>(adata);
		auto tdata = reinterpret_cast<uint64_t *>(target);
		for (idx_t i = 0; i < count; i++) {
			auto source_idx = adata.sel->get_index(offset + i);
			auto target_idx = target_offset + i;
			tdata[target_idx] = sdata[source_idx];
		}
	}
};

template <class T, class OP>
idx_t FixedSizeAppend(CompressionAppendState &append_state, ColumnSegment &segment, SegmentStatistics &stats,
                      UnifiedVectorFormat &data, idx_t offset, idx_t count) {
	D_ASSERT(segment.GetBlockOffset() == 0);

	auto target_ptr = append_state.handle.GetDataMutable();
	idx_t max_tuple_count = segment.SegmentSize() / sizeof(T);
	idx_t copy_count = MinValue<idx_t>(count, max_tuple_count - segment.count);

	OP::template Append<T>(stats, target_ptr, segment.count, data, offset, copy_count);
	segment.count += copy_count;
	return copy_count;
}

template <class T>
idx_t FixedSizeFinalizeAppend(ColumnSegment &segment, SegmentStatistics &stats) {
	return segment.count * sizeof(T);
}

//===--------------------------------------------------------------------===//
// Get Function
//===--------------------------------------------------------------------===//
template <class T, class APPENDER = StandardFixedSizeAppend>
CompressionFunction FixedSizeGetFunction(PhysicalType data_type) {
	return CompressionFunction(CompressionType::COMPRESSION_UNCOMPRESSED, data_type, FixedSizeInitAnalyze,
	                           FixedSizeAnalyze, FixedSizeFinalAnalyze<T>, UncompressedFunctions::InitCompression,
	                           UncompressedFunctions::Compress, UncompressedFunctions::FinalizeCompress,
	                           FixedSizeInitScan, FixedSizeScan<T>, FixedSizeScanPartial<T>, FixedSizeFetchRow<T>,
	                           UncompressedFunctions::EmptySkip, nullptr, FixedSizeInitAppend,
	                           FixedSizeAppend<T, APPENDER>, FixedSizeFinalizeAppend<T>);
}

CompressionFunction FixedSizeUncompressed::GetFunction(PhysicalType data_type) {
	switch (data_type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		return FixedSizeGetFunction<int8_t>(data_type);
	case PhysicalType::INT16:
		return FixedSizeGetFunction<int16_t>(data_type);
	case PhysicalType::INT32:
		return FixedSizeGetFunction<int32_t>(data_type);
	case PhysicalType::INT64:
		return FixedSizeGetFunction<int64_t>(data_type);
	case PhysicalType::UINT8:
		return FixedSizeGetFunction<uint8_t>(data_type);
	case PhysicalType::UINT16:
		return FixedSizeGetFunction<uint16_t>(data_type);
	case PhysicalType::UINT32:
		return FixedSizeGetFunction<uint32_t>(data_type);
	case PhysicalType::UINT64:
		return FixedSizeGetFunction<uint64_t>(data_type);
	case PhysicalType::INT128:
		return FixedSizeGetFunction<hugeint_t>(data_type);
	case PhysicalType::UINT128:
		return FixedSizeGetFunction<uhugeint_t>(data_type);
	case PhysicalType::FLOAT:
		return FixedSizeGetFunction<float>(data_type);
	case PhysicalType::DOUBLE:
		return FixedSizeGetFunction<double>(data_type);
	case PhysicalType::INTERVAL:
		return FixedSizeGetFunction<interval_t>(data_type);
	case PhysicalType::LIST:
		return FixedSizeGetFunction<uint64_t, ListFixedSizeAppend>(data_type);
	default:
		throw InternalException("Unsupported type for FixedSizeUncompressed::GetFunction");
	}
}

} // namespace duckdb
