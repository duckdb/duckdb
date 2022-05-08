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

struct DataFrameScanState : public FunctionOperatorData {
	DataFrameScanState() : position(0) {
	}

	idx_t position;
};

static unique_ptr<FunctionData> dataframe_scan_bind(ClientContext &context, TableFunctionBindInput &input,
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
			for (idx_t level_idx = 0; level_idx < size; level_idx++) {
				duckdb_levels.SetValue(level_idx, string(CHAR(STRING_ELT(levels, level_idx))));
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

static unique_ptr<FunctionOperatorData> dataframe_scan_init(ClientContext &context, const FunctionData *bind_data,
                                                            const vector<column_t> &column_ids,
                                                            TableFilterCollection *filters) {
	return make_unique<DataFrameScanState>();
}

static void dataframe_scan_function(ClientContext &context, const FunctionData *bind_data,
                                    FunctionOperatorData *operator_state, DataChunk &output) {
	auto &data = (DataFrameScanFunctionData &)*bind_data;
	auto &state = (DataFrameScanState &)*operator_state;
	if (state.position >= data.row_count) {
		return;
	}
	idx_t this_count = std::min((idx_t)STANDARD_VECTOR_SIZE, data.row_count - state.position);

	output.SetCardinality(this_count);

	for (idx_t col_idx = 0; col_idx < output.ColumnCount(); col_idx++) {
		auto &v = output.data[col_idx];
		SEXP coldata = VECTOR_ELT(data.df, col_idx);

		switch (data.rtypes[col_idx]) {
		case RType::LOGICAL: {
			auto data_ptr = INTEGER_POINTER(coldata) + state.position;
			AppendColumnSegment<int, bool, RBooleanType>(data_ptr, v, this_count);
			break;
		}
		case RType::INTEGER: {
			auto data_ptr = INTEGER_POINTER(coldata) + state.position;
			AppendColumnSegment<int, int, RIntegerType>(data_ptr, v, this_count);
			break;
		}
		case RType::NUMERIC: {
			auto data_ptr = NUMERIC_POINTER(coldata) + state.position;
			AppendColumnSegment<double, double, RDoubleType>(data_ptr, v, this_count);
			break;
		}
		case RType::STRING: {
			AppendStringSegment(coldata, v, state.position, this_count);
			break;
		}
		case RType::FACTOR: {
			auto data_ptr = INTEGER_POINTER(coldata) + state.position;
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
			auto data_ptr = NUMERIC_POINTER(coldata) + state.position;
			AppendColumnSegment<double, timestamp_t, RTimestampType>(data_ptr, v, this_count);
			break;
		}
		case RType::TIME_SECONDS: {
			auto data_ptr = NUMERIC_POINTER(coldata) + state.position;
			AppendColumnSegment<double, dtime_t, RTimeSecondsType>(data_ptr, v, this_count);
			break;
		}
		case RType::TIME_MINUTES: {
			auto data_ptr = NUMERIC_POINTER(coldata) + state.position;
			AppendColumnSegment<double, dtime_t, RTimeMinutesType>(data_ptr, v, this_count);
			break;
		}
		case RType::TIME_HOURS: {
			auto data_ptr = NUMERIC_POINTER(coldata) + state.position;
			AppendColumnSegment<double, dtime_t, RTimeHoursType>(data_ptr, v, this_count);
			break;
		}
		case RType::TIME_DAYS: {
			auto data_ptr = NUMERIC_POINTER(coldata) + state.position;
			AppendColumnSegment<double, dtime_t, RTimeDaysType>(data_ptr, v, this_count);
			break;
		}
		case RType::TIME_WEEKS: {
			auto data_ptr = NUMERIC_POINTER(coldata) + state.position;
			AppendColumnSegment<double, dtime_t, RTimeWeeksType>(data_ptr, v, this_count);
			break;
		}
		case RType::TIME_SECONDS_INTEGER: {
			auto data_ptr = INTEGER_POINTER(coldata) + state.position;
			AppendColumnSegment<int, dtime_t, RTimeSecondsType>(data_ptr, v, this_count);
			break;
		}
		case RType::TIME_MINUTES_INTEGER: {
			auto data_ptr = INTEGER_POINTER(coldata) + state.position;
			AppendColumnSegment<int, dtime_t, RTimeMinutesType>(data_ptr, v, this_count);
			break;
		}
		case RType::TIME_HOURS_INTEGER: {
			auto data_ptr = INTEGER_POINTER(coldata) + state.position;
			AppendColumnSegment<int, dtime_t, RTimeHoursType>(data_ptr, v, this_count);
			break;
		}
		case RType::TIME_DAYS_INTEGER: {
			auto data_ptr = INTEGER_POINTER(coldata) + state.position;
			AppendColumnSegment<int, dtime_t, RTimeDaysType>(data_ptr, v, this_count);
			break;
		}
		case RType::TIME_WEEKS_INTEGER: {
			auto data_ptr = INTEGER_POINTER(coldata) + state.position;
			AppendColumnSegment<int, dtime_t, RTimeWeeksType>(data_ptr, v, this_count);
			break;
		}
		case RType::DATE: {
			auto data_ptr = NUMERIC_POINTER(coldata) + state.position;
			AppendColumnSegment<double, date_t, RDateType>(data_ptr, v, this_count);
			break;
		}
		case RType::DATE_INTEGER: {
			auto data_ptr = INTEGER_POINTER(coldata) + state.position;
			AppendColumnSegment<int, date_t, RDateType>(data_ptr, v, this_count);
			break;
		}
		default:
			throw;
		}
	}

	state.position += this_count;
}

static unique_ptr<NodeStatistics> dataframe_scan_cardinality(ClientContext &context, const FunctionData *bind_data) {
	auto &data = (DataFrameScanFunctionData &)*bind_data;
	return make_unique<NodeStatistics>(data.row_count, data.row_count);
}

DataFrameScanFunction::DataFrameScanFunction()
    : TableFunction("r_dataframe_scan", {LogicalType::POINTER}, dataframe_scan_function, dataframe_scan_bind,
                    dataframe_scan_init, nullptr, nullptr, nullptr, dataframe_scan_cardinality) {};
