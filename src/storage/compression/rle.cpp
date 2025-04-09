#include "duckdb/common/types/null_value.hpp"
#include "duckdb/function/compression/compression.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/table/column_data_checkpointer.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/storage/table/scan_state.hpp"

#include <functional>

namespace duckdb {

using rle_count_t = uint16_t;

//===--------------------------------------------------------------------===//
// Analyze
//===--------------------------------------------------------------------===//
struct EmptyRLEWriter {
	template <class VALUE_TYPE>
	static void Operation(VALUE_TYPE value, rle_count_t count, void *dataptr, bool is_null) {
	}
};

template <class T>
struct RLEState {
	RLEState() : seen_count(0), last_value(NullValue<T>()), last_seen_count(0), dataptr(nullptr) {
	}

	idx_t seen_count;
	T last_value;
	rle_count_t last_seen_count;
	void *dataptr;
	bool all_null = true;

public:
	template <class OP>
	void Flush() {
		OP::template Operation<T>(last_value, last_seen_count, dataptr, all_null);
	}

	template <class OP = EmptyRLEWriter>
	void Update(const T *data, ValidityMask &validity, idx_t idx) {
		if (validity.RowIsValid(idx)) {
			if (all_null) {
				// no value seen yet
				// assign the current value, and increment the seen_count
				// note that we increment last_seen_count rather than setting it to 1
				// this is intentional: this is the first VALID value we see
				// but it might not be the first value in case of nulls!
				last_value = data[idx];
				seen_count++;
				last_seen_count++;
				all_null = false;
			} else if (last_value == data[idx]) {
				// the last value is identical to this value: increment the last_seen_count
				last_seen_count++;
			} else {
				// the values are different
				// issue the callback on the last value
				// edge case: if a value has exactly 2^16 repeated values, we can end up here with last_seen_count = 0
				if (last_seen_count > 0) {
					Flush<OP>();
					seen_count++;
				}

				// increment the seen_count and put the new value into the RLE slot
				last_value = data[idx];
				last_seen_count = 1;
			}
		} else {
			// NULL value: we merely increment the last_seen_count
			last_seen_count++;
		}
		if (last_seen_count == NumericLimits<rle_count_t>::Maximum()) {
			// we have seen the same value so many times in a row we are at the limit of what fits in our count
			// write away the value and move to the next value
			Flush<OP>();
			last_seen_count = 0;
			seen_count++;
		}
	}
};

template <class T>
struct RLEAnalyzeState : public AnalyzeState {
	explicit RLEAnalyzeState(const CompressionInfo &info) : AnalyzeState(info) {
	}

	RLEState<T> state;
};

template <class T>
unique_ptr<AnalyzeState> RLEInitAnalyze(ColumnData &col_data, PhysicalType type) {
	CompressionInfo info(col_data.GetBlockManager().GetBlockSize());
	return make_uniq<RLEAnalyzeState<T>>(info);
}

template <class T>
bool RLEAnalyze(AnalyzeState &state, Vector &input, idx_t count) {
	auto &rle_state = state.template Cast<RLEAnalyzeState<T>>();
	UnifiedVectorFormat vdata;
	input.ToUnifiedFormat(count, vdata);

	auto data = UnifiedVectorFormat::GetData<T>(vdata);
	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);
		rle_state.state.Update(data, vdata.validity, idx);
	}
	return true;
}

template <class T>
idx_t RLEFinalAnalyze(AnalyzeState &state) {
	auto &rle_state = state.template Cast<RLEAnalyzeState<T>>();
	return (sizeof(rle_count_t) + sizeof(T)) * rle_state.state.seen_count;
}

//===--------------------------------------------------------------------===//
// Compress
//===--------------------------------------------------------------------===//
struct RLEConstants {
	static constexpr const idx_t RLE_HEADER_SIZE = sizeof(uint64_t);
};

template <class T, bool WRITE_STATISTICS>
struct RLECompressState : public CompressionState {
	struct RLEWriter {
		template <class VALUE_TYPE>
		static void Operation(VALUE_TYPE value, rle_count_t count, void *dataptr, bool is_null) {
			auto state = reinterpret_cast<RLECompressState<T, WRITE_STATISTICS> *>(dataptr);
			state->WriteValue(value, count, is_null);
		}
	};

	idx_t MaxRLECount() {
		auto entry_size = sizeof(T) + sizeof(rle_count_t);
		return (info.GetBlockSize() - RLEConstants::RLE_HEADER_SIZE) / entry_size;
	}

	RLECompressState(ColumnDataCheckpointData &checkpoint_data_p, const CompressionInfo &info)
	    : CompressionState(info), checkpoint_data(checkpoint_data_p),
	      function(checkpoint_data.GetCompressionFunction(CompressionType::COMPRESSION_RLE)) {
		CreateEmptySegment(checkpoint_data.GetRowGroup().start);

		state.dataptr = (void *)this;
		max_rle_count = MaxRLECount();
	}

	void CreateEmptySegment(idx_t row_start) {
		auto &db = checkpoint_data.GetDatabase();
		auto &type = checkpoint_data.GetType();

		auto column_segment = ColumnSegment::CreateTransientSegment(db, function, type, row_start, info.GetBlockSize(),
		                                                            info.GetBlockSize());
		current_segment = std::move(column_segment);

		auto &buffer_manager = BufferManager::GetBufferManager(db);
		handle = buffer_manager.Pin(current_segment->block);
	}

	void Append(UnifiedVectorFormat &vdata, idx_t count) {
		auto data = UnifiedVectorFormat::GetData<T>(vdata);
		for (idx_t i = 0; i < count; i++) {
			auto idx = vdata.sel->get_index(i);
			state.template Update<RLECompressState<T, WRITE_STATISTICS>::RLEWriter>(data, vdata.validity, idx);
		}
	}

	void WriteValue(T value, rle_count_t count, bool is_null) {
		// write the RLE entry
		auto handle_ptr = handle.Ptr() + RLEConstants::RLE_HEADER_SIZE;
		auto data_pointer = reinterpret_cast<T *>(handle_ptr);
		auto index_pointer = reinterpret_cast<rle_count_t *>(handle_ptr + max_rle_count * sizeof(T));
		data_pointer[entry_count] = value;
		index_pointer[entry_count] = count;
		entry_count++;

		// update meta data
		if (WRITE_STATISTICS && !is_null) {
			current_segment->stats.statistics.UpdateNumericStats<T>(value);
		}
		current_segment->count += count;

		if (entry_count == max_rle_count) {
			// we have finished writing this segment: flush it and create a new segment
			auto row_start = current_segment->start + current_segment->count;
			FlushSegment();
			CreateEmptySegment(row_start);
			entry_count = 0;
		}
	}

	void FlushSegment() {
		// flush the segment
		// we compact the segment by moving the counts so they are directly next to the values
		idx_t counts_size = sizeof(rle_count_t) * entry_count;
		idx_t original_rle_offset = RLEConstants::RLE_HEADER_SIZE + max_rle_count * sizeof(T);
		idx_t minimal_rle_offset = RLEConstants::RLE_HEADER_SIZE + sizeof(T) * entry_count;
		idx_t aligned_rle_offset = AlignValue(minimal_rle_offset);
		idx_t total_segment_size = aligned_rle_offset + counts_size;
		auto data_ptr = handle.Ptr();
		if (aligned_rle_offset > minimal_rle_offset) {
			memset(data_ptr + minimal_rle_offset, 0, aligned_rle_offset - minimal_rle_offset);
		}
		memmove(data_ptr + aligned_rle_offset, data_ptr + original_rle_offset, counts_size);
		// store the final RLE offset within the segment
		Store<uint64_t>(aligned_rle_offset, data_ptr);
		handle.Destroy();

		auto &state = checkpoint_data.GetCheckpointState();
		state.FlushSegment(std::move(current_segment), std::move(handle), total_segment_size);
	}

	void Finalize() {
		state.template Flush<RLECompressState<T, WRITE_STATISTICS>::RLEWriter>();

		FlushSegment();
		current_segment.reset();
	}

	ColumnDataCheckpointData &checkpoint_data;
	CompressionFunction &function;
	unique_ptr<ColumnSegment> current_segment;
	BufferHandle handle;

	RLEState<T> state;
	idx_t entry_count = 0;
	idx_t max_rle_count;
};

template <class T, bool WRITE_STATISTICS>
unique_ptr<CompressionState> RLEInitCompression(ColumnDataCheckpointData &checkpoint_data,
                                                unique_ptr<AnalyzeState> state) {
	return make_uniq<RLECompressState<T, WRITE_STATISTICS>>(checkpoint_data, state->info);
}

template <class T, bool WRITE_STATISTICS>
void RLECompress(CompressionState &state_p, Vector &scan_vector, idx_t count) {
	auto &state = state_p.Cast<RLECompressState<T, WRITE_STATISTICS>>();
	UnifiedVectorFormat vdata;
	scan_vector.ToUnifiedFormat(count, vdata);

	state.Append(vdata, count);
}

template <class T, bool WRITE_STATISTICS>
void RLEFinalizeCompress(CompressionState &state_p) {
	auto &state = state_p.Cast<RLECompressState<T, WRITE_STATISTICS>>();
	state.Finalize();
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
template <class T>
struct RLEScanState : public SegmentScanState {
	explicit RLEScanState(ColumnSegment &segment) {
		auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
		handle = buffer_manager.Pin(segment.block);
		entry_pos = 0;
		position_in_entry = 0;
		rle_count_offset = UnsafeNumericCast<uint32_t>(Load<uint64_t>(handle.Ptr() + segment.GetBlockOffset()));
		D_ASSERT(rle_count_offset <= segment.GetBlockManager().GetBlockSize());
	}

	inline void SkipInternal(rle_count_t *index_pointer, idx_t skip_count) {
		while (skip_count > 0) {
			rle_count_t run_end = index_pointer[entry_pos];
			idx_t skip_amount = MinValue<idx_t>(skip_count, run_end - position_in_entry);

			skip_count -= skip_amount;
			position_in_entry += skip_amount;
			if (ExhaustedRun(index_pointer)) {
				ForwardToNextRun();
			}
		}
	}

	void Skip(ColumnSegment &segment, idx_t skip_count) {
		auto data = handle.Ptr() + segment.GetBlockOffset();
		auto index_pointer = reinterpret_cast<rle_count_t *>(data + rle_count_offset);
		SkipInternal(index_pointer, skip_count);
	}

	inline void ForwardToNextRun() {
		// handled all entries in this RLE value
		// move to the next entry
		entry_pos++;
		position_in_entry = 0;
	}

	inline bool ExhaustedRun(rle_count_t *index_pointer) {
		return position_in_entry >= index_pointer[entry_pos];
	}

	BufferHandle handle;
	idx_t entry_pos;
	idx_t position_in_entry;
	uint32_t rle_count_offset;
	//! If we are running a filter over the column - the runs that match the filter
	unsafe_unique_array<bool> matching_runs;
	idx_t matching_run_count = 0;
};

template <class T>
unique_ptr<SegmentScanState> RLEInitScan(ColumnSegment &segment) {
	auto result = make_uniq<RLEScanState<T>>(segment);
	return std::move(result);
}

//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//
template <class T>
void RLESkip(ColumnSegment &segment, ColumnScanState &state, idx_t skip_count) {
	auto &scan_state = state.scan_state->Cast<RLEScanState<T>>();
	scan_state.Skip(segment, skip_count);
}

template <bool ENTIRE_VECTOR>
static bool CanEmitConstantVector(idx_t position, idx_t run_length, idx_t scan_count) {
	if (!ENTIRE_VECTOR) {
		return false;
	}
	if (scan_count != STANDARD_VECTOR_SIZE) {
		// Only when we can fill an entire Vector can we emit a ConstantVector, because subsequent scans require the
		// input Vector to be flat
		return false;
	}
	D_ASSERT(position < run_length);
	auto remaining_in_run = run_length - position;
	// The amount of values left in this run are equal or greater than the amount of values we need to scan
	return remaining_in_run >= scan_count;
}

template <class T>
static void RLEScanConstant(RLEScanState<T> &scan_state, rle_count_t *index_pointer, T *data_pointer, idx_t scan_count,
                            Vector &result) {
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	auto result_data = ConstantVector::GetData<T>(result);
	result_data[0] = data_pointer[scan_state.entry_pos];
	scan_state.position_in_entry += scan_count;
	if (scan_state.ExhaustedRun(index_pointer)) {
		scan_state.ForwardToNextRun();
	}
	return;
}

template <class T, bool ENTIRE_VECTOR>
void RLEScanPartialInternal(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
                            idx_t result_offset) {
	auto &scan_state = state.scan_state->Cast<RLEScanState<T>>();

	auto data = scan_state.handle.Ptr() + segment.GetBlockOffset();
	auto data_pointer = reinterpret_cast<T *>(data + RLEConstants::RLE_HEADER_SIZE);
	auto index_pointer = reinterpret_cast<rle_count_t *>(data + scan_state.rle_count_offset);

	// If we are scanning an entire Vector and it contains only a single run
	if (CanEmitConstantVector<ENTIRE_VECTOR>(scan_state.position_in_entry, index_pointer[scan_state.entry_pos],
	                                         scan_count)) {
		RLEScanConstant<T>(scan_state, index_pointer, data_pointer, scan_count, result);
		return;
	}

	auto result_data = FlatVector::GetData<T>(result);
	result.SetVectorType(VectorType::FLAT_VECTOR);

	idx_t result_end = result_offset + scan_count;
	while (result_offset < result_end) {
		rle_count_t run_end = index_pointer[scan_state.entry_pos];
		idx_t run_count = run_end - scan_state.position_in_entry;
		idx_t remaining_scan_count = result_end - result_offset;
		T element = data_pointer[scan_state.entry_pos];
		if (DUCKDB_UNLIKELY(run_count > remaining_scan_count)) {
			for (idx_t i = 0; i < remaining_scan_count; i++) {
				result_data[result_offset + i] = element;
			}
			scan_state.position_in_entry += remaining_scan_count;
			break;
		}

		for (idx_t i = 0; i < run_count; i++) {
			result_data[result_offset + i] = element;
		}

		result_offset += run_count;
		scan_state.ForwardToNextRun();
	}
}

template <class T>
void RLEScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result,
                    idx_t result_offset) {
	return RLEScanPartialInternal<T, false>(segment, state, scan_count, result, result_offset);
}

template <class T>
void RLEScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result) {
	RLEScanPartialInternal<T, true>(segment, state, scan_count, result, 0);
}

//===--------------------------------------------------------------------===//
// Select
//===--------------------------------------------------------------------===//
template <class T>
void RLESelect(ColumnSegment &segment, ColumnScanState &state, idx_t vector_count, Vector &result,
               const SelectionVector &sel, idx_t sel_count) {
	auto &scan_state = state.scan_state->Cast<RLEScanState<T>>();

	auto data = scan_state.handle.Ptr() + segment.GetBlockOffset();
	auto data_pointer = reinterpret_cast<T *>(data + RLEConstants::RLE_HEADER_SIZE);
	auto index_pointer = reinterpret_cast<rle_count_t *>(data + scan_state.rle_count_offset);

	// If we are scanning an entire Vector and it contains only a single run we don't need to select at all
	if (CanEmitConstantVector<true>(scan_state.position_in_entry, index_pointer[scan_state.entry_pos], vector_count)) {
		RLEScanConstant<T>(scan_state, index_pointer, data_pointer, vector_count, result);
		return;
	}

	auto result_data = FlatVector::GetData<T>(result);
	result.SetVectorType(VectorType::FLAT_VECTOR);

	idx_t prev_idx = 0;
	for (idx_t i = 0; i < sel_count; i++) {
		auto next_idx = sel.get_index(i);
		if (next_idx < prev_idx) {
			throw InternalException("Error in RLESelect - selection vector indices are not ordered");
		}
		// skip forward to the next index
		scan_state.SkipInternal(index_pointer, next_idx - prev_idx);
		// read the element
		result_data[i] = data_pointer[scan_state.entry_pos];
		// move the next to the prev
		prev_idx = next_idx;
	}
	// skip the tail
	scan_state.SkipInternal(index_pointer, vector_count - prev_idx);
}

//===--------------------------------------------------------------------===//
// Filter
//===--------------------------------------------------------------------===//
template <class T>
void RLEFilter(ColumnSegment &segment, ColumnScanState &state, idx_t vector_count, Vector &result, SelectionVector &sel,
               idx_t &sel_count, const TableFilter &filter) {
	auto &scan_state = state.scan_state->Cast<RLEScanState<T>>();

	auto data = scan_state.handle.Ptr() + segment.GetBlockOffset();
	auto data_pointer = reinterpret_cast<T *>(data + RLEConstants::RLE_HEADER_SIZE);
	auto index_pointer = reinterpret_cast<rle_count_t *>(data + scan_state.rle_count_offset);

	auto total_run_count = (scan_state.rle_count_offset - RLEConstants::RLE_HEADER_SIZE) / sizeof(T);
	if (!scan_state.matching_runs) {
		// we haven't applied the filter yet
		// apply the filter to all RLE values at once

		// initialize the filter set to all false (all runs are filtered out)
		scan_state.matching_runs = make_unsafe_uniq_array<bool>(total_run_count);
		memset(scan_state.matching_runs.get(), 0, sizeof(bool) * total_run_count);

		// execute the filter over all runs at once
		Vector run_vector(result.GetType(), data_ptr_cast(data_pointer));

		UnifiedVectorFormat run_format;
		run_vector.ToUnifiedFormat(total_run_count, run_format);

		SelectionVector run_matches;
		scan_state.matching_run_count = total_run_count;
		ColumnSegment::FilterSelection(run_matches, run_vector, run_format, filter, total_run_count,
		                               scan_state.matching_run_count);

		// for any runs that pass the filter - set the matches to true
		for (idx_t i = 0; i < scan_state.matching_run_count; i++) {
			auto idx = run_matches.get_index(i);
			scan_state.matching_runs[idx] = true;
		}
	}
	if (scan_state.matching_run_count == 0) {
		// early-out, no runs match the filter so the filter can never pass
		sel_count = 0;
		return;
	}
	// scan (the subset of) the matching runs AND set the output selection vector with the rows that match
	auto result_data = FlatVector::GetData<T>(result);
	result.SetVectorType(VectorType::FLAT_VECTOR);

	idx_t matching_count = 0;
	SelectionVector matching_sel(sel_count);
	if (!sel.IsSet()) {
		// no selection vector yet - fast path
		// this is essentially the normal scan, but we apply the filter and fill the selection vector
		idx_t result_offset = 0;
		idx_t result_end = sel_count;
		while (result_offset < result_end) {
			rle_count_t run_end = index_pointer[scan_state.entry_pos];
			idx_t run_count = run_end - scan_state.position_in_entry;
			idx_t remaining_scan_count = result_end - result_offset;
			// the run is scanned - scan it
			T element = data_pointer[scan_state.entry_pos];
			if (DUCKDB_UNLIKELY(run_count > remaining_scan_count)) {
				if (scan_state.matching_runs[scan_state.entry_pos]) {
					for (idx_t i = 0; i < remaining_scan_count; i++) {
						result_data[result_offset + i] = element;
						matching_sel.set_index(matching_count++, result_offset + i);
					}
				}
				scan_state.position_in_entry += remaining_scan_count;
				break;
			}

			if (scan_state.matching_runs[scan_state.entry_pos]) {
				for (idx_t i = 0; i < run_count; i++) {
					result_data[result_offset + i] = element;
					matching_sel.set_index(matching_count++, result_offset + i);
				}
			}

			result_offset += run_count;
			scan_state.ForwardToNextRun();
		}
	} else {
		// we already have a selection applied - this is more complex since we need to merge it with our filter
		// use a simpler (but slower) approach
		idx_t prev_idx = 0;
		for (idx_t i = 0; i < sel_count; i++) {
			auto read_idx = sel.get_index(i);
			if (read_idx < prev_idx) {
				throw InternalException("Error in RLEFilter - selection vector indices are not ordered");
			}
			// skip forward to the next index
			scan_state.SkipInternal(index_pointer, read_idx - prev_idx);
			prev_idx = read_idx;
			if (!scan_state.matching_runs[scan_state.entry_pos]) {
				// this run is filtered out - we don't need to scan it
				continue;
			}
			// the run is not filtered out - read the element
			result_data[read_idx] = data_pointer[scan_state.entry_pos];
			matching_sel.set_index(matching_count++, read_idx);
		}
		// skip the tail
		scan_state.SkipInternal(index_pointer, vector_count - prev_idx);
	}

	// set up the filter result
	if (matching_count != sel_count) {
		sel.Initialize(matching_sel);
		sel_count = matching_count;
	}
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
template <class T>
void RLEFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx) {
	RLEScanState<T> scan_state(segment);
	scan_state.Skip(segment, NumericCast<idx_t>(row_id));

	auto data = scan_state.handle.Ptr() + segment.GetBlockOffset();
	auto data_pointer = reinterpret_cast<T *>(data + RLEConstants::RLE_HEADER_SIZE);
	auto result_data = FlatVector::GetData<T>(result);
	result_data[result_idx] = data_pointer[scan_state.entry_pos];
}

//===--------------------------------------------------------------------===//
// Get Function
//===--------------------------------------------------------------------===//
template <class T, bool WRITE_STATISTICS = true>
CompressionFunction GetRLEFunction(PhysicalType data_type) {
	return CompressionFunction(CompressionType::COMPRESSION_RLE, data_type, RLEInitAnalyze<T>, RLEAnalyze<T>,
	                           RLEFinalAnalyze<T>, RLEInitCompression<T, WRITE_STATISTICS>,
	                           RLECompress<T, WRITE_STATISTICS>, RLEFinalizeCompress<T, WRITE_STATISTICS>,
	                           RLEInitScan<T>, RLEScan<T>, RLEScanPartial<T>, RLEFetchRow<T>, RLESkip<T>, nullptr,
	                           nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, nullptr, RLESelect<T>,
	                           RLEFilter<T>);
}

CompressionFunction RLEFun::GetFunction(PhysicalType type) {
	switch (type) {
	case PhysicalType::BOOL: {
		auto function = GetRLEFunction<int8_t>(type);
		function.filter = nullptr;
		return function;
	}
	case PhysicalType::INT8:
		return GetRLEFunction<int8_t>(type);
	case PhysicalType::INT16:
		return GetRLEFunction<int16_t>(type);
	case PhysicalType::INT32:
		return GetRLEFunction<int32_t>(type);
	case PhysicalType::INT64:
		return GetRLEFunction<int64_t>(type);
	case PhysicalType::INT128:
		return GetRLEFunction<hugeint_t>(type);
	case PhysicalType::UINT128:
		return GetRLEFunction<uhugeint_t>(type);
	case PhysicalType::UINT8:
		return GetRLEFunction<uint8_t>(type);
	case PhysicalType::UINT16:
		return GetRLEFunction<uint16_t>(type);
	case PhysicalType::UINT32:
		return GetRLEFunction<uint32_t>(type);
	case PhysicalType::UINT64:
		return GetRLEFunction<uint64_t>(type);
	case PhysicalType::FLOAT:
		return GetRLEFunction<float>(type);
	case PhysicalType::DOUBLE:
		return GetRLEFunction<double>(type);
	case PhysicalType::LIST:
		return GetRLEFunction<uint64_t, false>(type);
	default:
		throw InternalException("Unsupported type for RLE");
	}
}

bool RLEFun::TypeIsSupported(const PhysicalType physical_type) {
	switch (physical_type) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
	case PhysicalType::INT16:
	case PhysicalType::INT32:
	case PhysicalType::INT64:
	case PhysicalType::INT128:
	case PhysicalType::UINT8:
	case PhysicalType::UINT16:
	case PhysicalType::UINT32:
	case PhysicalType::UINT64:
	case PhysicalType::UINT128:
	case PhysicalType::FLOAT:
	case PhysicalType::DOUBLE:
	case PhysicalType::LIST:
		return true;
	default:
		return false;
	}
}

} // namespace duckdb
