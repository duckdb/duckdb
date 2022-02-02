#include "rapi.hpp"
#include "typesr.hpp"

#include "duckdb/main/client_context.hpp"

using namespace duckdb;
using namespace cpp11;

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

static void AppendStringSegment(SEXP *coldata, Vector &result, idx_t count) {
	auto result_data = FlatVector::GetData<string_t>(result);
	auto &result_mask = FlatVector::Validity(result);
	for (idx_t i = 0; i < count; i++) {
		SEXP val = coldata[i];
		if (val == NA_STRING) {
			result_mask.SetInvalid(i);
		} else {
			result_data[i] = string_t((char *)CHAR(val), LENGTH(val));
		}
	}
}

struct DataFrameScanBindData : public TableFunctionData {
	DataFrameScanBindData(SEXP df_p, idx_t row_count_p, vector<RType> &rtypes_p, vector<data_ptr_t> &dataptrs_p)
	    : df(df_p), row_count(row_count_p), rtypes(rtypes_p), data_ptrs(dataptrs_p) {
	}
	data_frame df;
	idx_t row_count;
	vector<RType> rtypes;
	vector<data_ptr_t> data_ptrs;
	idx_t rows_per_task = 1000000;
};

struct DataFrameParallelState : public ParallelState {
	mutex lock;
	idx_t row_group_index = 0;
};

struct DataFrameOperatorData : public FunctionOperatorData {
	bool done = false;
	vector<column_t> column_ids;
	idx_t position;
	idx_t offset;
	idx_t count;
};

static unique_ptr<FunctionData> dataframe_scan_bind(ClientContext &context, vector<Value> &inputs,
                                                    named_parameter_map_t &named_parameters,
                                                    vector<LogicalType> &input_table_types,
                                                    vector<string> &input_table_names,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
	data_frame df((SEXP)inputs[0].GetPointer());

	auto df_names = df.names();
	vector<RType> rtypes;
	vector<data_ptr_t> data_ptrs;

	for (R_xlen_t col_idx = 0; col_idx < df.size(); col_idx++) {
		auto coldata = df[col_idx];
		LogicalType duckdb_col_type;
		data_ptr_t coldata_ptr = nullptr;

		names.push_back(df_names[col_idx]);
		rtypes.push_back(RApiTypes::DetectRType(coldata));

		switch (rtypes[col_idx]) {
		case RType::LOGICAL:
			duckdb_col_type = LogicalType::BOOLEAN;
			coldata_ptr = (data_ptr_t)LOGICAL_POINTER(coldata);
			break;
		case RType::INTEGER:
			duckdb_col_type = LogicalType::INTEGER;
			coldata_ptr = (data_ptr_t)INTEGER_POINTER(coldata);
			break;
		case RType::NUMERIC:
			duckdb_col_type = LogicalType::DOUBLE;
			coldata_ptr = (data_ptr_t)NUMERIC_POINTER(coldata);
			break;
		case RType::FACTOR: {
			// TODO What about factors that use numeric?
			coldata_ptr = (data_ptr_t)INTEGER_POINTER(coldata);
			strings levels = GET_LEVELS(coldata);
			Vector duckdb_levels(LogicalType::VARCHAR, levels.size());
			for (R_xlen_t level_idx = 0; level_idx < levels.size(); level_idx++) {
				duckdb_levels.SetValue(level_idx, (string)levels[level_idx]);
			}
			duckdb_col_type = LogicalType::ENUM(df_names[col_idx], duckdb_levels, levels.size());
			break;
		}
		case RType::STRING:
			duckdb_col_type = LogicalType::VARCHAR;
			coldata_ptr = (data_ptr_t)DATAPTR_RO(coldata);
			break;
		case RType::TIMESTAMP:
			duckdb_col_type = LogicalType::TIMESTAMP;
			coldata_ptr = (data_ptr_t)NUMERIC_POINTER(coldata);
			break;
		case RType::TIME_SECONDS:
		case RType::TIME_MINUTES:
		case RType::TIME_HOURS:
		case RType::TIME_DAYS:
		case RType::TIME_WEEKS:
			duckdb_col_type = LogicalType::TIME;
			coldata_ptr = (data_ptr_t)NUMERIC_POINTER(coldata);
			break;
		case RType::TIME_SECONDS_INTEGER:
		case RType::TIME_MINUTES_INTEGER:
		case RType::TIME_HOURS_INTEGER:
		case RType::TIME_DAYS_INTEGER:
		case RType::TIME_WEEKS_INTEGER:
			duckdb_col_type = LogicalType::TIME;
			coldata_ptr = (data_ptr_t)INTEGER_POINTER(coldata);
			break;
		case RType::DATE:
		case RType::DATE_INTEGER:
			coldata_ptr = (data_ptr_t)NUMERIC_POINTER(coldata);
			duckdb_col_type = LogicalType::DATE;
			break;
		default:
			stop("Unsupported column type for scan");
		}

		return_types.push_back(duckdb_col_type);
		data_ptrs.push_back(coldata_ptr);
	}
	auto row_count = Rf_length(VECTOR_ELT(df, 0));
	return make_unique<DataFrameScanBindData>(df, row_count, rtypes, data_ptrs);
}

static void dataframe_scan_init_internal(ClientContext &context, const DataFrameScanBindData *bind_data,
                                         DataFrameOperatorData *operator_state, idx_t offset, idx_t count) {
	D_ASSERT(bind_data);
	D_ASSERT(operator_state);

	operator_state->position = 0;
	operator_state->offset = offset;
	operator_state->count = count;
	operator_state->done = false;
}

static unique_ptr<FunctionOperatorData> dataframe_scan_init(ClientContext &context, const FunctionData *bind_data_p,
                                                            const vector<column_t> &column_ids,
                                                            TableFilterCollection *filters) {
	D_ASSERT(bind_data_p);
	auto bind_data = (const DataFrameScanBindData *)bind_data_p;
	auto operator_data = make_unique<DataFrameOperatorData>();

	operator_data->column_ids = column_ids;
	dataframe_scan_init_internal(context, bind_data, operator_data.get(), 0, bind_data->row_count);
	return move(operator_data);
}

static void dataframe_scan_function(ClientContext &context, const FunctionData *bind_data_p,
                                    FunctionOperatorData *operator_data_p, DataChunk *input, DataChunk &output) {
	auto &bind_data = (DataFrameScanBindData &)*bind_data_p;
	auto &operator_data = (DataFrameOperatorData &)*operator_data_p;
	if (operator_data.position >= operator_data.count) {
		return;
	}
	idx_t this_count = std::min((idx_t)STANDARD_VECTOR_SIZE, operator_data.count - operator_data.position);
	output.SetCardinality(this_count);

	auto sexp_offset = operator_data.offset + operator_data.position;
	D_ASSERT(sexp_offset + this_count <= bind_data.row_count);

	for (R_xlen_t col_idx = 0; col_idx < output.ColumnCount(); col_idx++) {
		auto &v = output.data[col_idx];
		auto coldata_ptr = bind_data.data_ptrs[col_idx];
		switch (bind_data.rtypes[col_idx]) {
		case RType::LOGICAL: {
			auto data_ptr = (int *)coldata_ptr + sexp_offset;
			AppendColumnSegment<int, bool, RBooleanType>(data_ptr, v, this_count);
			break;
		}
		case RType::INTEGER: {
			auto data_ptr = (int *)coldata_ptr + sexp_offset;
			AppendColumnSegment<int, int, RIntegerType>(data_ptr, v, this_count);
			break;
		}
		case RType::NUMERIC: {
			auto data_ptr = (double *)coldata_ptr + sexp_offset;
			AppendColumnSegment<double, double, RDoubleType>(data_ptr, v, this_count);
			break;
		}
		case RType::STRING: {
			auto data_ptr = (SEXP *)coldata_ptr + sexp_offset;
			AppendStringSegment(data_ptr, v, this_count);
			break;
		}
		case RType::FACTOR: {
			auto data_ptr = (int *)coldata_ptr + sexp_offset;
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
				stop("duckdb_execute_R: Unknown enum type for scan: %s",
				     TypeIdToString(v.GetType().InternalType()).c_str());
			}
			break;
		}
		case RType::TIMESTAMP: {
			auto data_ptr = (double *)coldata_ptr + sexp_offset;
			AppendColumnSegment<double, timestamp_t, RTimestampType>(data_ptr, v, this_count);
			break;
		}
		case RType::TIME_SECONDS: {
			auto data_ptr = (double *)coldata_ptr + sexp_offset;
			AppendColumnSegment<double, dtime_t, RTimeSecondsType>(data_ptr, v, this_count);
			break;
		}
		case RType::TIME_MINUTES: {
			auto data_ptr = (double *)coldata_ptr + sexp_offset;
			AppendColumnSegment<double, dtime_t, RTimeMinutesType>(data_ptr, v, this_count);
			break;
		}
		case RType::TIME_HOURS: {
			auto data_ptr = (double *)coldata_ptr + sexp_offset;
			AppendColumnSegment<double, dtime_t, RTimeHoursType>(data_ptr, v, this_count);
			break;
		}
		case RType::TIME_DAYS: {
			auto data_ptr = (double *)coldata_ptr + sexp_offset;
			AppendColumnSegment<double, dtime_t, RTimeDaysType>(data_ptr, v, this_count);
			break;
		}
		case RType::TIME_WEEKS: {
			auto data_ptr = (double *)coldata_ptr + sexp_offset;
			AppendColumnSegment<double, dtime_t, RTimeWeeksType>(data_ptr, v, this_count);
			break;
		}
		case RType::TIME_SECONDS_INTEGER: {
			auto data_ptr = (int *)coldata_ptr + sexp_offset;
			AppendColumnSegment<int, dtime_t, RTimeSecondsType>(data_ptr, v, this_count);
			break;
		}
		case RType::TIME_MINUTES_INTEGER: {
			auto data_ptr = (int *)coldata_ptr + sexp_offset;
			AppendColumnSegment<int, dtime_t, RTimeMinutesType>(data_ptr, v, this_count);
			break;
		}
		case RType::TIME_HOURS_INTEGER: {
			auto data_ptr = (int *)coldata_ptr + sexp_offset;
			AppendColumnSegment<int, dtime_t, RTimeHoursType>(data_ptr, v, this_count);
			break;
		}
		case RType::TIME_DAYS_INTEGER: {
			auto data_ptr = (int *)coldata_ptr + sexp_offset;
			AppendColumnSegment<int, dtime_t, RTimeDaysType>(data_ptr, v, this_count);
			break;
		}
		case RType::TIME_WEEKS_INTEGER: {
			auto data_ptr = (int *)coldata_ptr + sexp_offset;
			AppendColumnSegment<int, dtime_t, RTimeWeeksType>(data_ptr, v, this_count);
			break;
		}
		case RType::DATE: {
			auto data_ptr = (double *)coldata_ptr + sexp_offset;
			AppendColumnSegment<double, date_t, RDateType>(data_ptr, v, this_count);
			break;
		}
		case RType::DATE_INTEGER: {
			auto data_ptr = (int *)coldata_ptr + sexp_offset;
			AppendColumnSegment<int, date_t, RDateType>(data_ptr, v, this_count);
			break;
		}
		default:
			throw;
		}
	}

	operator_data.position += this_count;
}

static unique_ptr<NodeStatistics> dataframe_scan_cardinality(ClientContext &context, const FunctionData *bind_data_p) {
	auto &bind_data = (DataFrameScanBindData &)*bind_data_p;
	return make_unique<NodeStatistics>(bind_data.row_count, bind_data.row_count);
}

static string dataframe_scan_tostring(const FunctionData *bind_data_p) {
	return "data.frame";
}

static idx_t dataframe_scan_max_threads(ClientContext &context, const FunctionData *bind_data_p) {
	D_ASSERT(bind_data_p);
	auto bind_data = (const DataFrameScanBindData *)bind_data_p;
	return ceil((double)bind_data->row_count / bind_data->rows_per_task);
}

static unique_ptr<ParallelState> dataframe_scan_init_parallel_state(ClientContext &context, const FunctionData *,
                                                                    const vector<column_t> &column_ids,
                                                                    TableFilterCollection *) {
	auto result = make_unique<DataFrameParallelState>();
	result->row_group_index = 0;
	return move(result);
}

static bool dataframe_scan_parallel_next(ClientContext &context, const FunctionData *bind_data_p,
                                         FunctionOperatorData *operator_data_p, ParallelState *parallel_state_p) {
	D_ASSERT(bind_data_p);
	D_ASSERT(operator_data_p);
	D_ASSERT(parallel_state_p);

	auto bind_data = (const DataFrameScanBindData *)bind_data_p;
	auto operator_data = (DataFrameOperatorData *)operator_data_p;
	auto &parallel_state = (DataFrameParallelState &)*parallel_state_p;

	lock_guard<mutex> parallel_lock(parallel_state.lock);

	if (parallel_state.row_group_index < ceil((double)bind_data->row_count / bind_data->rows_per_task)) {
		auto offset = parallel_state.row_group_index * bind_data->rows_per_task;
		auto count = std::min(bind_data->rows_per_task, bind_data->row_count - offset);
		dataframe_scan_init_internal(context, bind_data, operator_data, offset, count);
		parallel_state.row_group_index++;
		return true;
	}
	return false;
}

static unique_ptr<FunctionOperatorData>
dataframe_scan_parallel_init(ClientContext &context, const FunctionData *bind_data_p, ParallelState *parallel_state_p,
                             const vector<column_t> &column_ids, TableFilterCollection *) {
	auto operator_data = make_unique<DataFrameOperatorData>();
	operator_data->column_ids = column_ids;
	if (!dataframe_scan_parallel_next(context, bind_data_p, operator_data.get(), parallel_state_p)) {
		operator_data->done = true;
	}
	return move(operator_data);
}

DataFrameScanFunction::DataFrameScanFunction()
    : TableFunction("r_dataframe_scan", {LogicalType::POINTER}, dataframe_scan_function, dataframe_scan_bind,
                    dataframe_scan_init, nullptr, nullptr, nullptr, dataframe_scan_cardinality, nullptr,
                    dataframe_scan_tostring, dataframe_scan_max_threads, dataframe_scan_init_parallel_state, nullptr,
                    dataframe_scan_parallel_init, dataframe_scan_parallel_next, true, false, nullptr) {};
