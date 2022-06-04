#include "rapi.hpp"
#include "typesr.hpp"
#include "altrepstring.hpp"

#include "duckdb/main/client_context.hpp"

using namespace duckdb;

template <class SRC, class DST, class RTYPE>
static void AppendColumnSegment(SRC *source_data, Vector &result, idx_t count) {
	auto result_data = FlatVector::GetData<DST>(result);
	auto &result_mask = FlatVector::Validity(result);
	for (idx_t i = 0; i < count; i++) {
		auto val = source_data[i];
		if (RTYPE::IsNull(val)) {
			result_mask.SetInvalid(i);
		} else {
			result_data[i] = RTYPE::Convert(val);
		}
	}
}

static void AppendStringSegment(SEXP coldata, Vector &result, idx_t row_idx, idx_t count) {
	auto result_data = FlatVector::GetData<string_t>(result);
	auto &result_mask = FlatVector::Validity(result);
	for (idx_t i = 0; i < count; i++) {
		SEXP val = STRING_ELT(coldata, row_idx + i);
		if (val == NA_STRING) {
			result_mask.SetInvalid(i);
		} else {
			result_data[i] = string_t((char *)CHAR(val));
		}
	}
}

struct DataFrameScanFunctionData : public TableFunctionData {
	DataFrameScanFunctionData(SEXP df, idx_t row_count, vector<RType> rtypes)
	    : df(df), row_count(row_count), rtypes(rtypes) {
	}
	SEXP df;
	idx_t row_count;
	vector<RType> rtypes;
};

struct DataFrameGlobalScanState : public GlobalTableFunctionState {
	static constexpr const idx_t DF_PARTITION_SIZE = STANDARD_VECTOR_SIZE * 50;

	DataFrameGlobalScanState(idx_t max_threads) : position(0), max_threads(max_threads) {
	}

	mutex lock;
	idx_t position;
	idx_t max_threads;

	idx_t MaxThreads() const override {
		return max_threads;
	}
};

struct DataFrameLocalScanState : public LocalTableFunctionState {
	DataFrameLocalScanState() : position(0), end(0) {
	}

	idx_t position;
	idx_t end;
};

static unique_ptr<FunctionData> DataFrameScanBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	RProtector r;
	SEXP df((SEXP)input.inputs[0].GetPointer());

	auto df_names = r.Protect(GET_NAMES(df));
	vector<RType> rtypes;

	for (idx_t col_idx = 0; col_idx < (idx_t)Rf_length(df); col_idx++) {
		auto column_name = string(CHAR(STRING_ELT(df_names, col_idx)));
		names.push_back(column_name);
		SEXP coldata = VECTOR_ELT(df, col_idx);
		rtypes.push_back(RApiTypes::DetectRType(coldata));
		LogicalType duckdb_col_type;
		switch (rtypes[col_idx]) {
		case RType::LOGICAL:
			duckdb_col_type = LogicalType::BOOLEAN;
			break;
		case RType::INTEGER:
			duckdb_col_type = LogicalType::INTEGER;
			break;
		case RType::NUMERIC:
			duckdb_col_type = LogicalType::DOUBLE;
			break;
		case RType::FACTOR: {
			// TODO What about factors that use numeric?

			auto levels = r.Protect(GET_LEVELS(coldata));
			idx_t size = LENGTH(levels);
			Vector duckdb_levels(LogicalType::VARCHAR, size);
			auto str_ptr = FlatVector::GetData<string_t>(duckdb_levels);
			for (idx_t level_idx = 0; level_idx < size; level_idx++) {
				auto csexp = STRING_ELT(levels, level_idx);
				str_ptr[level_idx] = StringVector::AddStringOrBlob(duckdb_levels, CHAR(csexp), LENGTH(csexp));
			}
			duckdb_col_type = LogicalType::ENUM(column_name, duckdb_levels, size);
			break;
		}
		case RType::STRING:
			duckdb_col_type = LogicalType::VARCHAR;
			break;
		case RType::TIMESTAMP:
			duckdb_col_type = LogicalType::TIMESTAMP;
			break;
		case RType::TIME_SECONDS:
		case RType::TIME_MINUTES:
		case RType::TIME_HOURS:
		case RType::TIME_DAYS:
		case RType::TIME_WEEKS:
		case RType::TIME_SECONDS_INTEGER:
		case RType::TIME_MINUTES_INTEGER:
		case RType::TIME_HOURS_INTEGER:
		case RType::TIME_DAYS_INTEGER:
		case RType::TIME_WEEKS_INTEGER:
			duckdb_col_type = LogicalType::TIME;
			break;
		case RType::DATE:
		case RType::DATE_INTEGER:
			duckdb_col_type = LogicalType::DATE;
			break;
		default:
			cpp11::stop("rapi_execute: Unsupported column type for scan");
		}
		return_types.push_back(duckdb_col_type);
	}

	auto row_count = Rf_length(VECTOR_ELT(df, 0));
	return make_unique<DataFrameScanFunctionData>(df, row_count, rtypes);
}

static bool NextDataFrameScan(ClientContext &context, const DataFrameScanFunctionData &bind_data,
                              DataFrameLocalScanState &lstate, DataFrameGlobalScanState &gstate) {
	lock_guard<mutex> glock(gstate.lock);
	if (gstate.position >= bind_data.row_count) {
		return false;
	}
	lstate.position = gstate.position;
	gstate.position += DataFrameGlobalScanState::DF_PARTITION_SIZE;
	if (gstate.position > bind_data.row_count) {
		gstate.position = bind_data.row_count;
	}
	lstate.end = gstate.position;
	return true;
}

static unique_ptr<GlobalTableFunctionState> DataFrameScanInitGlobal(ClientContext &context,
                                                                    TableFunctionInitInput &input) {
	auto &data = (const DataFrameScanFunctionData &)*input.bind_data;
	auto max_threads = MaxValue<idx_t>(1, data.row_count / DataFrameGlobalScanState::DF_PARTITION_SIZE);
	return make_unique<DataFrameGlobalScanState>(max_threads);
}

static unique_ptr<LocalTableFunctionState> DataFrameScanInitLocal(ClientContext &context, TableFunctionInitInput &input,
                                                                  GlobalTableFunctionState *gstate_p) {
	auto &bind_data = (const DataFrameScanFunctionData &)*input.bind_data;
	auto &gstate = (DataFrameGlobalScanState &)*gstate_p;

	auto result = make_unique<DataFrameLocalScanState>();
	NextDataFrameScan(context, bind_data, *result, gstate);
	return move(result);
}

static void DataFrameScanFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = (DataFrameScanFunctionData &)*data_p.bind_data;
	auto &gstate = (DataFrameGlobalScanState &)*data_p.global_state;
	auto &lstate = (DataFrameLocalScanState &)*data_p.local_state;
	if (lstate.position >= lstate.end) {
		if (!NextDataFrameScan(context, data, lstate, gstate)) {
			return;
		}
	}
	idx_t this_count = std::min((idx_t)STANDARD_VECTOR_SIZE, lstate.end - lstate.position);

	output.SetCardinality(this_count);
	auto pos = lstate.position;
	for (idx_t col_idx = 0; col_idx < output.ColumnCount(); col_idx++) {
		auto &v = output.data[col_idx];
		SEXP coldata = VECTOR_ELT(data.df, col_idx);

		switch (data.rtypes[col_idx]) {
		case RType::LOGICAL: {
			auto data_ptr = INTEGER_POINTER(coldata) + pos;
			AppendColumnSegment<int, bool, RBooleanType>(data_ptr, v, this_count);
			break;
		}
		case RType::INTEGER: {
			auto data_ptr = INTEGER_POINTER(coldata) + pos;
			AppendColumnSegment<int, int, RIntegerType>(data_ptr, v, this_count);
			break;
		}
		case RType::NUMERIC: {
			auto data_ptr = NUMERIC_POINTER(coldata) + pos;
			AppendColumnSegment<double, double, RDoubleType>(data_ptr, v, this_count);
			break;
		}
		case RType::STRING: {
			AppendStringSegment(coldata, v, pos, this_count);
			break;
		}
		case RType::FACTOR: {
			auto data_ptr = INTEGER_POINTER(coldata) + pos;
			switch (v.GetType().InternalType()) {
			case PhysicalType::UINT8:
				AppendColumnSegment<int, uint8_t, RFactorType>(data_ptr, v, this_count);
				break;

			case PhysicalType::UINT16:
				AppendColumnSegment<int, uint16_t, RFactorType>(data_ptr, v, this_count);
				break;

			case PhysicalType::UINT32:
				AppendColumnSegment<int, uint32_t, RFactorType>(data_ptr, v, this_count);
				break;

			default:
				cpp11::stop("rapi_execute: Unknown enum type for scan: %s",
				            TypeIdToString(v.GetType().InternalType()).c_str());
			}
			break;
		}
		case RType::TIMESTAMP: {
			auto data_ptr = NUMERIC_POINTER(coldata) + pos;
			AppendColumnSegment<double, timestamp_t, RTimestampType>(data_ptr, v, this_count);
			break;
		}
		case RType::TIME_SECONDS: {
			auto data_ptr = NUMERIC_POINTER(coldata) + pos;
			AppendColumnSegment<double, dtime_t, RTimeSecondsType>(data_ptr, v, this_count);
			break;
		}
		case RType::TIME_MINUTES: {
			auto data_ptr = NUMERIC_POINTER(coldata) + pos;
			AppendColumnSegment<double, dtime_t, RTimeMinutesType>(data_ptr, v, this_count);
			break;
		}
		case RType::TIME_HOURS: {
			auto data_ptr = NUMERIC_POINTER(coldata) + pos;
			AppendColumnSegment<double, dtime_t, RTimeHoursType>(data_ptr, v, this_count);
			break;
		}
		case RType::TIME_DAYS: {
			auto data_ptr = NUMERIC_POINTER(coldata) + pos;
			AppendColumnSegment<double, dtime_t, RTimeDaysType>(data_ptr, v, this_count);
			break;
		}
		case RType::TIME_WEEKS: {
			auto data_ptr = NUMERIC_POINTER(coldata) + pos;
			AppendColumnSegment<double, dtime_t, RTimeWeeksType>(data_ptr, v, this_count);
			break;
		}
		case RType::TIME_SECONDS_INTEGER: {
			auto data_ptr = INTEGER_POINTER(coldata) + pos;
			AppendColumnSegment<int, dtime_t, RTimeSecondsType>(data_ptr, v, this_count);
			break;
		}
		case RType::TIME_MINUTES_INTEGER: {
			auto data_ptr = INTEGER_POINTER(coldata) + pos;
			AppendColumnSegment<int, dtime_t, RTimeMinutesType>(data_ptr, v, this_count);
			break;
		}
		case RType::TIME_HOURS_INTEGER: {
			auto data_ptr = INTEGER_POINTER(coldata) + pos;
			AppendColumnSegment<int, dtime_t, RTimeHoursType>(data_ptr, v, this_count);
			break;
		}
		case RType::TIME_DAYS_INTEGER: {
			auto data_ptr = INTEGER_POINTER(coldata) + pos;
			AppendColumnSegment<int, dtime_t, RTimeDaysType>(data_ptr, v, this_count);
			break;
		}
		case RType::TIME_WEEKS_INTEGER: {
			auto data_ptr = INTEGER_POINTER(coldata) + pos;
			AppendColumnSegment<int, dtime_t, RTimeWeeksType>(data_ptr, v, this_count);
			break;
		}
		case RType::DATE: {
			auto data_ptr = NUMERIC_POINTER(coldata) + pos;
			AppendColumnSegment<double, date_t, RDateType>(data_ptr, v, this_count);
			break;
		}
		case RType::DATE_INTEGER: {
			auto data_ptr = INTEGER_POINTER(coldata) + pos;
			AppendColumnSegment<int, date_t, RDateType>(data_ptr, v, this_count);
			break;
		}
		default:
			throw InternalException("Unrecognized type in R DataFrame scan");
		}
	}

	lstate.position += this_count;
}

static unique_ptr<NodeStatistics> DataFrameScanCardinality(ClientContext &context, const FunctionData *bind_data) {
	auto &data = (DataFrameScanFunctionData &)*bind_data;
	return make_unique<NodeStatistics>(data.row_count, data.row_count);
}

DataFrameScanFunction::DataFrameScanFunction()
    : TableFunction("r_dataframe_scan", {LogicalType::POINTER}, DataFrameScanFunc, DataFrameScanBind,
                    DataFrameScanInitGlobal, DataFrameScanInitLocal) {
	cardinality = DataFrameScanCardinality;
};
