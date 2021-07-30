#include "duckdb/function/compression/compression.hpp"

namespace duckdb {

template<class T>
struct RLEAnalyzeState : public AnalyzeState {
	RLEAnalyzeState() : seen_count(0), last_seen_count(0) {}

	idx_t seen_count;
	T last_value;
	idx_t last_seen_count;
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
		if (vdata.validity.RowIsValid(idx)) {
			if (rle_state.seen_count == 0) {
				// no value seen yet
				// assign the current value, and set the seen_count to 1
				// note that we increment last_seen_count rather than setting it to 1
				// this is intentional: this is the first VALID value we see
				// but it might not be the first value!
				rle_state.last_value = data[idx];
				rle_state.seen_count = 1;
				rle_state.last_seen_count++;
			} else if (rle_state.last_value == data[idx]) {
				// the last value is identical to this value: increment the last_seen_count
				rle_state.last_seen_count++;
			} else {
				// the values are different: increment the seen_count and put this value into the RLE slot
				rle_state.last_value = data[idx];
				rle_state.seen_count++;
				rle_state.last_seen_count = 1;
			}
		} else {
			// NULL value: we merely increment the last_seen_count
			rle_state.last_seen_count++;
		}
	}
	return true;
}

template<class T>
idx_t RLEFinalAnalyze(AnalyzeState &state) {
	// auto &rle_state = (RLEAnalyzeState<T> &) state;
	return NumericLimits<idx_t>::Maximum();
	// return (sizeof(uint32_t) + sizeof(T)) * rle_state.seen_count;
}

template<class T>
unique_ptr<CompressionState> RLEInitCompression(ColumnDataCheckpointer &checkpointer, unique_ptr<AnalyzeState> state) {
	throw InternalException("FIXME: RLE");
}

template<class T>
void RLECompress(CompressionState& state, Vector &scan_vector, idx_t count) {
	throw InternalException("FIXME: RLE");
}

template<class T>
void RLEFinalizeCompress(CompressionState& state) {
	throw InternalException("FIXME: RLE");
}

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
		nullptr,
		nullptr,
		nullptr,
		nullptr,
		nullptr,
		nullptr,
		nullptr
	);
}

CompressionFunction RLEFun::GetFunction(PhysicalType type) {
	switch(type) {
	case PhysicalType::BOOL:
		return GetRLEFunction<bool>(type);
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
