#include "duckdb/common/helper.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/validity_executor.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/checkpoint/write_overflow_strings_to_disk.hpp"
#include "duckdb/storage/segment/uncompressed.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/statistics/numeric_stats_traits.hpp"
#include "duckdb/storage/statistics/stats_writer.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/column_data_checkpointer.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/storage/table/scan_state.hpp"

#include <type_traits>

namespace duckdb {

//===--------------------------------------------------------------------===//
// Analyze
//===--------------------------------------------------------------------===//
struct FixedSizeAnalyzeState : public AnalyzeState {
	explicit FixedSizeAnalyzeState(BlockManager &block_manager) : AnalyzeState(block_manager), count(0) {
	}

	idx_t count;
};

unique_ptr<AnalyzeState> FixedSizeInitAnalyze(ColumnData &col_data, PhysicalType type) {
	return make_uniq<FixedSizeAnalyzeState>(col_data.GetBlockManager());
}

bool FixedSizeAnalyze(AnalyzeState &state_p, const Vector &input) {
	auto &state = state_p.Cast<FixedSizeAnalyzeState>();
	state.count += input.size();
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
	explicit UncompressedCompressState(ColumnDataCheckpointData &checkpoint_data);

public:
	virtual void CreateEmptySegment();
	void FlushSegment(idx_t segment_size);
	void Finalize(idx_t segment_size);
	idx_t FinalizeAppend();

public:
	unique_ptr<ColumnSegment> current_segment;
	ColumnAppendState append_state;
};

UncompressedCompressState::UncompressedCompressState(ColumnDataCheckpointData &checkpoint_data)
    : CompressionState(checkpoint_data, CompressionType::COMPRESSION_UNCOMPRESSED) {
	UncompressedCompressState::CreateEmptySegment();
}

void UncompressedCompressState::CreateEmptySegment() {
	auto &type = checkpoint_data.GetType();

	auto compressed_segment = CreateNewSegment();
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
	append_state.InitializeStats(type);
}

void UncompressedCompressState::FlushSegment(idx_t segment_size) {
	auto &state = checkpoint_data.GetCheckpointState();
	if (current_segment->GetType().InternalType() == PhysicalType::VARCHAR) {
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

idx_t UncompressedCompressState::FinalizeAppend() {
	current_segment->GetStatsMutable().Merge(*append_state.append_stats);
	return current_segment->FinalizeAppend(append_state);
}

unique_ptr<CompressionState> UncompressedFunctions::InitCompression(ColumnDataCheckpointData &checkpoint_data,
                                                                    unique_ptr<AnalyzeState> state) {
	return make_uniq<UncompressedCompressState>(checkpoint_data);
}

void UncompressedFunctions::Compress(CompressionState &state_p, const Vector &data) {
	auto &state = state_p.Cast<UncompressedCompressState>();
	UnifiedVectorFormat vdata;
	data.ToUnifiedFormat(vdata);

	idx_t offset = 0;
	idx_t remaining = data.size();
	while (remaining > 0) {
		idx_t appended = state.current_segment->Append(state.append_state, vdata, offset, remaining);
		if (appended == remaining) {
			// appended everything: finished
			return;
		}
		// the segment is full: flush it to disk
		state.FlushSegment(state.FinalizeAppend());

		// now create a new segment and continue appending
		state.CreateEmptySegment();
		offset += appended;
		remaining -= appended;
	}
}

void UncompressedFunctions::FinalizeCompress(CompressionState &state_p) {
	auto &state = state_p.Cast<UncompressedCompressState>();
	state.Finalize(state.FinalizeAppend());
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
struct FixedSizeScanState : public SegmentScanState {
	BufferHandle handle;
};

unique_ptr<SegmentScanState> FixedSizeInitScan(const QueryContext &context, ColumnSegment &segment) {
	auto result = make_uniq<FixedSizeScanState>();
	auto &buffer_manager = BufferManager::GetBufferManager(segment.GetDatabase());
	result->handle = buffer_manager.Pin(context, segment.GetBlockHandle());
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
	auto &buffer_manager = BufferManager::GetBufferManager(segment.GetDatabase());
	auto handle = buffer_manager.Pin(segment.GetBlockHandle());

	// first fetch the data from the base table
	auto data_ptr = handle.GetDataMutable() + segment.GetBlockOffset() + NumericCast<idx_t>(row_id) * sizeof(T);

	memcpy(FlatVector::GetDataMutable(result) + result_idx * sizeof(T), data_ptr, sizeof(T));
}

//===--------------------------------------------------------------------===//
// Append
//===--------------------------------------------------------------------===//
static unique_ptr<CompressionAppendState> FixedSizeInitAppend(ColumnSegment &segment) {
	auto &buffer_manager = BufferManager::GetBufferManager(segment.GetDatabase());
	auto handle = buffer_manager.Pin(segment.GetBlockHandle());
	return make_uniq<CompressionAppendState>(std::move(handle));
}

template <class T>
struct FixedSizeStatsWriterTraits {
	using TYPE = typename std::remove_cv<T>::type;
	static constexpr bool SUPPORTS_NUMERIC_STATS_WRITER =
	    std::is_integral<TYPE>::value || std::is_floating_point<TYPE>::value || std::is_same<TYPE, hugeint_t>::value ||
	    std::is_same<TYPE, uhugeint_t>::value;
};

template <class T>
using FixedSizeStatsWriter = typename std::conditional<FixedSizeStatsWriterTraits<T>::SUPPORTS_NUMERIC_STATS_WRITER,
                                                       StatsWriter<T>, StatsWriter<void>>::type;

static constexpr idx_t FIXED_SIZE_UNCOMPRESSED_VALIDITY_EXTRACT_RUN_LENGTH = 16;

template <class T>
static inline void AppendContiguousValidFixedSizeValues(BaseStatistics &stats, FixedSizeStatsWriter<T> &writer,
                                                        T *__restrict target, const T *__restrict source, idx_t count) {
	D_ASSERT(count > 0);
	if constexpr (FixedSizeStatsWriterTraits<T>::SUPPORTS_NUMERIC_STATS_WRITER) {
		using OPERATIONS = NumericStatsTraits<T>;
		FixedSizeStatsWriter<T> local_writer;
		local_writer.SetHasValid();
		for (idx_t i = 0; i < count; i++) {
			const auto input = OPERATIONS::LoadInput(source + i);
			OPERATIONS::StoreInput(target + i, input);
			local_writer.UpdateMinMaxFromInput(input);
		}
		local_writer.Merge(writer);
	} else {
		writer.SetHasValid();
		for (idx_t i = 0; i < count; i++) {
			const auto value = source[i];
			target[i] = value;
			stats.UpdateNumericStats<T>(value);
		}
	}
}

template <class T>
static inline void AppendContiguousFixedSizeValues(BaseStatistics &stats, T *__restrict target,
                                                   const T *__restrict source, idx_t count) {
	FixedSizeStatsWriter<T> writer;
	AppendContiguousValidFixedSizeValues<T>(stats, writer, target, source, count);
	writer.Merge(stats);
}

template <class T>
static inline void AppendContiguousInvalidFixedSizeValues(FixedSizeStatsWriter<T> &writer, T *__restrict target,
                                                          idx_t count) {
	D_ASSERT(count > 0);
	writer.SetHasNull();
	if constexpr (FixedSizeStatsWriterTraits<T>::SUPPORTS_NUMERIC_STATS_WRITER) {
		using OPERATIONS = NumericStatsTraits<T>;
		const auto null_input = OPERATIONS::NullInput();
		for (idx_t i = 0; i < count; i++) {
			OPERATIONS::StoreInput(target + i, null_input);
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			target[i] = NullValue<T>();
		}
	}
}

// Validity slices are word-local fragments from ValidityExecutor. Numeric fixed-size types use NumericStatsTraits
// for local min/max reduction; other fixed-size types update validity through StatsWriter<void>.
template <class T>
static inline void AppendContiguousValiditySlice(BaseStatistics &stats, FixedSizeStatsWriter<T> &writer,
                                                 T *__restrict target, const T *__restrict source,
                                                 const ValidityWordSlice &word) {
	const auto slice_count = word.Count();
	const auto has_valid = word.HasValid();
	const auto has_invalid = word.HasInvalid();
	if constexpr (FixedSizeStatsWriterTraits<T>::SUPPORTS_NUMERIC_STATS_WRITER) {
		using OPERATIONS = NumericStatsTraits<T>;
		FixedSizeStatsWriter<T> local_writer;
		if (has_valid) {
			local_writer.SetHasValid();
		}
		if (has_invalid) {
			local_writer.SetHasNull();
		}
		for (idx_t i = 0; i < slice_count; i++) {
			if (word.RowIsValid(i)) {
				const auto input = OPERATIONS::LoadInput(source + i);
				OPERATIONS::StoreInput(target + i, input);
				local_writer.UpdateMinMaxFromInput(input);
			} else {
				OPERATIONS::StoreInput(target + i, OPERATIONS::NullInput());
			}
		}
		local_writer.Merge(writer);
	} else {
		if (has_valid) {
			writer.SetHasValid();
		}
		if (has_invalid) {
			writer.SetHasNull();
		}
		for (idx_t i = 0; i < slice_count; i++) {
			if (word.RowIsValid(i)) {
				const auto value = source[i];
				target[i] = value;
				stats.UpdateNumericStats<T>(value);
			} else {
				target[i] = NullValue<T>();
			}
		}
	}
}

template <class T>
static inline void AppendContiguousNullableFixedSizeValues(BaseStatistics &stats, T *__restrict target,
                                                           const T *__restrict source, const ValidityMask &validity,
                                                           idx_t source_offset, idx_t count) {
	FixedSizeStatsWriter<T> writer;
	auto valid_func = [&](idx_t local_offset, idx_t append_count) {
		AppendContiguousValidFixedSizeValues<T>(stats, writer, target + local_offset, source + local_offset,
		                                        append_count);
	};
	auto invalid_func = [&](idx_t local_offset, idx_t append_count) {
		AppendContiguousInvalidFixedSizeValues<T>(writer, target + local_offset, append_count);
	};
	auto validity_slice_func = [&](idx_t local_offset, ValidityWordSlice word) {
		AppendContiguousValiditySlice<T>(stats, writer, target + local_offset, source + local_offset, word);
	};

	ValidityExecutor::Execute<FIXED_SIZE_UNCOMPRESSED_VALIDITY_EXTRACT_RUN_LENGTH>(
	    validity, source_offset, count, valid_func, invalid_func, validity_slice_func);
	writer.Merge(stats);
}

template <class T>
static inline void AppendSelectedValidFixedSizeValues(BaseStatistics &stats, T *__restrict target,
                                                      const T *__restrict source, const SelectionVector &sel,
                                                      idx_t source_offset, idx_t count) {
	D_ASSERT(count > 0);
	if constexpr (FixedSizeStatsWriterTraits<T>::SUPPORTS_NUMERIC_STATS_WRITER) {
		using OPERATIONS = NumericStatsTraits<T>;
		StatsWriter<T> writer;
		writer.SetHasValid();
		for (idx_t i = 0; i < count; i++) {
			const auto source_idx = sel.get_index(source_offset + i);
			const auto input = OPERATIONS::LoadInput(source + source_idx);
			OPERATIONS::StoreInput(target + i, input);
			writer.UpdateMinMaxFromInput(input);
		}
		writer.Merge(stats);
	} else {
		stats.SetHasNoNullFast();
		for (idx_t i = 0; i < count; i++) {
			const auto source_idx = sel.get_index(source_offset + i);
			const auto value = source[source_idx];
			target[i] = value;
			stats.UpdateNumericStats<T>(value);
		}
	}
}

template <class T>
static inline void AppendSelectedNullableFixedSizeValues(BaseStatistics &stats, T *__restrict target,
                                                         const T *__restrict source, const SelectionVector &sel,
                                                         const ValidityMask &validity, idx_t source_offset,
                                                         idx_t count) {
	D_ASSERT(count > 0);
	if constexpr (FixedSizeStatsWriterTraits<T>::SUPPORTS_NUMERIC_STATS_WRITER) {
		using OPERATIONS = NumericStatsTraits<T>;
		StatsWriter<T> writer;
		bool has_valid = false;
		bool has_null = false;
		for (idx_t i = 0; i < count; i++) {
			const auto source_idx = sel.get_index(source_offset + i);
			if (validity.RowIsValid(source_idx)) {
				has_valid = true;
				const auto input = OPERATIONS::LoadInput(source + source_idx);
				OPERATIONS::StoreInput(target + i, input);
				writer.UpdateMinMaxFromInput(input);
			} else {
				has_null = true;
				OPERATIONS::StoreInput(target + i, OPERATIONS::NullInput());
			}
		}
		if (has_valid) {
			writer.SetHasValid();
		}
		if (has_null) {
			writer.SetHasNull();
		}
		writer.Merge(stats);
	} else {
		bool has_valid = false;
		bool has_null = false;
		for (idx_t i = 0; i < count; i++) {
			const auto source_idx = sel.get_index(source_offset + i);
			if (validity.RowIsValid(source_idx)) {
				has_valid = true;
				const auto value = source[source_idx];
				target[i] = value;
				stats.UpdateNumericStats<T>(value);
			} else {
				has_null = true;
				target[i] = NullValue<T>();
			}
		}
		if (has_valid) {
			stats.SetHasNoNullFast();
		}
		if (has_null) {
			stats.SetHasNullFast();
		}
	}
}

struct StandardFixedSizeAppend {
	template <class T>
	static void Append(BaseStatistics &stats, data_ptr_t target, idx_t target_offset, UnifiedVectorFormat &adata,
	                   idx_t offset, idx_t count) {
		auto sdata = UnifiedVectorFormat::GetData<T>(adata);
		auto tdata = reinterpret_cast<T *>(target);
		if (!adata.sel->IsSet()) {
			// Contiguous buffer fast path
			auto source = sdata + offset;
			auto target_data = tdata + target_offset;
			if (adata.validity.CannotHaveNull()) {
				AppendContiguousFixedSizeValues<T>(stats, target_data, source, count);
			} else {
				AppendContiguousNullableFixedSizeValues<T>(stats, target_data, source, adata.validity, offset, count);
			}
			return;
		} else if (adata.validity.CanHaveNull()) {
			AppendSelectedNullableFixedSizeValues<T>(stats, tdata + target_offset, sdata, *adata.sel, adata.validity,
			                                         offset, count);
		} else {
			AppendSelectedValidFixedSizeValues<T>(stats, tdata + target_offset, sdata, *adata.sel, offset, count);
		}
	}
};

struct ListFixedSizeAppend {
	template <class T>
	static void Append(BaseStatistics &stats, data_ptr_t target, idx_t target_offset, UnifiedVectorFormat &adata,
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
idx_t FixedSizeAppend(CompressionAppendState &append_state, ColumnSegment &segment, BaseStatistics &stats,
                      UnifiedVectorFormat &data, idx_t offset, idx_t count) {
	D_ASSERT(segment.GetBlockOffset() == 0);

	auto target_ptr = append_state.handle.GetDataMutable();
	idx_t max_tuple_count = segment.SegmentSize() / sizeof(T);
	idx_t copy_count = MinValue<idx_t>(count, max_tuple_count - segment.count);

	// Compress() flushes when Append() returns _fewer_ rows than requested. If the previous call filled this segment
	// exactly, this call starts with no remaining capacity, so copy_count is zero.
	// StandardFixedSizeAppend assumes at least one row, so if copy_count is zero, exit early here.
	if (copy_count == 0) {
		return 0;
	}

	OP::template Append<T>(stats, target_ptr, segment.count, data, offset, copy_count);
	segment.count += copy_count;
	return copy_count;
}

template <class T>
idx_t FixedSizeFinalizeAppend(ColumnSegment &segment, BaseStatistics &stats) {
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
