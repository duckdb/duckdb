#include "duckdb/function/compression/compression.hpp"
#include "duckdb/storage/statistics/numeric_statistics.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/storage/table/column_data_checkpointer.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/common/types/null_value.hpp"
#include <functional>

namespace duckdb {

using rle_count_t = uint16_t;

//===--------------------------------------------------------------------===//
// Analyze
//===--------------------------------------------------------------------===//
struct EmptyRLEWriter {
	template<class VALUE_TYPE>
	static void Operation(VALUE_TYPE value, rle_count_t count, void *dataptr) {
	}
};

template<class T>
struct RLEState {
	RLEState() : seen_count(0), last_value(NullValue<T>()), last_seen_count(0), dataptr(nullptr) {}

	idx_t seen_count;
	T last_value;
	rle_count_t last_seen_count;
	void *dataptr;

public:
	template<class OP>
	void Flush() {
		OP::template Operation<T>(last_value, last_seen_count, dataptr);
	}

	template<class OP=EmptyRLEWriter>
	void Update(T *data, ValidityMask &validity, idx_t idx) {
		if (validity.RowIsValid(idx)) {
			if (seen_count == 0) {
				// no value seen yet
				// assign the current value, and set the seen_count to 1
				// note that we increment last_seen_count rather than setting it to 1
				// this is intentional: this is the first VALID value we see
				// but it might not be the first value in case of nulls!
				last_value = data[idx];
				seen_count = 1;
				last_seen_count++;
			} else if (last_value == data[idx]) {
				// the last value is identical to this value: increment the last_seen_count
				last_seen_count++;
			} else {
				// the values are different
				// issue the callback on the last value
				Flush<OP>();

				// increment the seen_count and put the new value into the RLE slot
				last_value = data[idx];
				seen_count++;
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

template<class T>
struct RLEAnalyzeState : public AnalyzeState {
	RLEAnalyzeState() {}

	RLEState<T> state;
};

template<class T>
unique_ptr<AnalyzeState> RLEInitAnalyze(ColumnData &col_data, PhysicalType type) {
	return make_unique<RLEAnalyzeState<T>>();
}

template<class T>
bool RLEAnalyze(AnalyzeState &state, Vector &input, idx_t count) {
	auto &rle_state = (RLEAnalyzeState<T> &) state;
	VectorData vdata;
	input.Orrify(count, vdata);

	auto data = (T *) vdata.data;
	for(idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);
		rle_state.state.Update(data, vdata.validity, idx);
	}
	return true;
}

template<class T>
idx_t RLEFinalAnalyze(AnalyzeState &state) {
	auto &rle_state = (RLEAnalyzeState<T> &) state;
	return (sizeof(rle_count_t) + sizeof(T)) * rle_state.state.seen_count;
}

//===--------------------------------------------------------------------===//
// Compress
//===--------------------------------------------------------------------===//
template<class T>
struct RLECompressState : public CompressionState {
	struct RLEWriter {
		template<class VALUE_TYPE>
		static void Operation(VALUE_TYPE value, rle_count_t count, void *dataptr) {
			auto state = (RLECompressState<T> *) dataptr;
			state->WriteValue(value, count);
		}
	};

	static idx_t MaxRLECount() {
		auto entry_size = sizeof(T) + sizeof(rle_count_t);
		auto entry_count = Storage::BLOCK_SIZE / entry_size;
		auto max_vector_count = entry_count / STANDARD_VECTOR_SIZE;
		return max_vector_count * STANDARD_VECTOR_SIZE;
	}

	RLECompressState(ColumnDataCheckpointer &checkpointer_p) :
	    checkpointer(checkpointer_p) {
		auto &db = checkpointer.GetDatabase();
		auto &type = checkpointer.GetType();
		auto &config = DBConfig::GetConfig(db);
		function = config.GetCompressionFunction(CompressionType::COMPRESSION_RLE, type.InternalType());
		CreateEmptySegment(checkpointer.GetRowGroup().start);

		state.dataptr = (void *) this;
		max_rle_count = MaxRLECount();
	}

	void CreateEmptySegment(idx_t row_start) {
		auto &db = checkpointer.GetDatabase();
		auto &type = checkpointer.GetType();
		auto column_segment = ColumnSegment::CreateTransientSegment(db, type, row_start);
		column_segment->function = function;
		current_segment = move(column_segment);
		auto &buffer_manager = BufferManager::GetBufferManager(db);
		handle = buffer_manager.Pin(current_segment->block);
	}

	void Append(VectorData &vdata, idx_t count) {
		auto data = (T *) vdata.data;
		for(idx_t i = 0; i < count; i++) {
			auto idx = vdata.sel->get_index(i);
			state.template Update<RLECompressState<T>::RLEWriter>(data, vdata.validity, idx);
		}
	}

	void WriteValue(T value, rle_count_t count) {
		// write the RLE entry
		auto handle_ptr = handle->Ptr();
		auto data_pointer = (T *) handle_ptr;
		auto index_pointer = (rle_count_t *) (handle_ptr + max_rle_count * sizeof(T));
		data_pointer[entry_count] = value;
		index_pointer[entry_count] = count;
		entry_count++;

		// update meta data
		NumericStatistics::Update<T>(current_segment->stats, value);
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
		handle.reset();

		auto &state = checkpointer.GetCheckpointState();
		state.FlushSegment(move(current_segment));
	}

	void Finalize() {
		state.template Flush<RLECompressState<T>::RLEWriter>();

		FlushSegment();
		current_segment.reset();
	}

	ColumnDataCheckpointer &checkpointer;
	CompressionFunction *function;
	unique_ptr<ColumnSegment> current_segment;
	unique_ptr<BufferHandle> handle;

	RLEState<T> state;
	idx_t entry_count = 0;
	idx_t max_rle_count;
};

template<class T>
unique_ptr<CompressionState> RLEInitCompression(ColumnDataCheckpointer &checkpointer, unique_ptr<AnalyzeState> state) {
	return make_unique<RLECompressState<T>>(checkpointer);
}

template<class T>
void RLECompress(CompressionState& state_p, Vector &scan_vector, idx_t count) {
	auto &state = (RLECompressState<T> &) state_p;
	VectorData vdata;
	scan_vector.Orrify(count, vdata);

	state.Append(vdata, count);
}

template<class T>
void RLEFinalizeCompress(CompressionState& state_p) {
	auto &state = (RLECompressState<T> &) state_p;
	state.Finalize();
}

//===--------------------------------------------------------------------===//
// Scan
//===--------------------------------------------------------------------===//
template<class T>
struct RLEScanState : public SegmentScanState {
	RLEScanState(ColumnSegment &segment) {
		auto &buffer_manager = BufferManager::GetBufferManager(segment.db);
		handle = buffer_manager.Pin(segment.block);
		entry_pos = 0;
		position_in_entry = 0;
		max_rle_count = RLECompressState<T>::MaxRLECount();
	}

	void Skip(idx_t skip_count) {
		auto data = handle->node->buffer;
		auto index_pointer = (rle_count_t *) (data + (max_rle_count * sizeof(T)));

		for(idx_t i = 0; i < skip_count; i++) {
			// assign the current value
			position_in_entry++;
			if (position_in_entry >= index_pointer[entry_pos]) {
				// handled all entries in this RLE value
				// move to the next entry
				entry_pos++;
				position_in_entry = 0;
			}
		}
	}

	unique_ptr<BufferHandle> handle;
	idx_t entry_pos;
	idx_t position_in_entry;
	idx_t max_rle_count;
};

template<class T>
unique_ptr<SegmentScanState> RLEInitScan(ColumnSegment &segment) {
	auto result = make_unique<RLEScanState<T>>(segment);
	return move(result);
}

//===--------------------------------------------------------------------===//
// Scan base data
//===--------------------------------------------------------------------===//
template<class T>
void RLESkip(ColumnSegment &segment, ColumnScanState &state, idx_t skip_count) {
	auto &scan_state = (RLEScanState<T> &) *state.scan_state;
	scan_state.Skip(skip_count);
}

template<class T>
void RLEScanPartial(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result, idx_t result_offset) {
	auto &scan_state = (RLEScanState<T> &) *state.scan_state;

	auto data = scan_state.handle->node->buffer;
	auto data_pointer = (T *) data;
	auto index_pointer = (rle_count_t *) (data + (scan_state.max_rle_count * sizeof(T)));

	auto result_data = FlatVector::GetData<T>(result);
	result.SetVectorType(VectorType::FLAT_VECTOR);
	for(idx_t i = 0; i < scan_count; i++) {
		// assign the current value
		result_data[result_offset + i] = data_pointer[scan_state.entry_pos];
		scan_state.position_in_entry++;
		if (scan_state.position_in_entry >= index_pointer[scan_state.entry_pos]) {
			// handled all entries in this RLE value
			// move to the next entry
			scan_state.entry_pos++;
			scan_state.position_in_entry = 0;
		}
	}
}

template<class T>
void RLEScan(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count, Vector &result) {
	// FIXME: emit constant vector if repetition of single value is >= scan_count
	RLEScanPartial<T>(segment, state, scan_count, result, 0);
}

//===--------------------------------------------------------------------===//
// Fetch
//===--------------------------------------------------------------------===//
template<class T>
void RLEFetchRow(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx) {
	RLEScanState<T> scan_state(segment);
	scan_state.Skip(row_id);

	auto data = scan_state.handle->node->buffer;
	auto data_pointer = (T *) data;
	auto result_data = FlatVector::GetData<T>(result);
	result_data[result_idx] = data_pointer[scan_state.entry_pos];
}

//===--------------------------------------------------------------------===//
// Get Function
//===--------------------------------------------------------------------===//
template<class T>
CompressionFunction GetRLEFunction(PhysicalType data_type) {
	return CompressionFunction(
		CompressionType::COMPRESSION_RLE,
		data_type,
		RLEInitAnalyze<T>,
		RLEAnalyze<T>,
		RLEFinalAnalyze<T>,
		RLEInitCompression<T>,
		RLECompress<T>,
		RLEFinalizeCompress<T>,
		RLEInitScan<T>,
		RLEScan<T>,
		RLEScanPartial<T>,
		RLEFetchRow<T>,
		RLESkip<T>,
		nullptr,
		nullptr,
		nullptr
	);
}

CompressionFunction RLEFun::GetFunction(PhysicalType type) {
	switch(type) {
	case PhysicalType::BOOL:
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
	default:
		throw InternalException("Unsupported type for RLE");
	}
}

bool RLEFun::TypeIsSupported(PhysicalType type) {
	switch(type) {
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
	case PhysicalType::FLOAT:
	case PhysicalType::DOUBLE:
		return true;
	default:
		return false;
	}
}

}
