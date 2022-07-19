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

static bool get_bool_param(named_parameter_map_t &named_parameters, string name, bool dflt = false) {
	bool res = dflt;
	auto entry = named_parameters.find(name);
	if (entry != named_parameters.end()) {
		res = BooleanValue::Get(entry->second);
	}
	return res;
}

struct DataFrameScanBindData : public TableFunctionData {
	DataFrameScanBindData(SEXP df_p, idx_t row_count_p, vector<RType> &rtypes_p, vector<data_ptr_t> &dataptrs_p,
	                      named_parameter_map_t &named_parameters)
	    : df(df_p), row_count(row_count_p), rtypes(rtypes_p), data_ptrs(dataptrs_p) {
		experimental = get_bool_param(named_parameters, "experimental", false);
	}
	data_frame df;
	idx_t row_count;
	vector<RType> rtypes;
	vector<data_ptr_t> data_ptrs;
	idx_t rows_per_task = 1000000;
	bool experimental;
};

struct DataFrameGlobalState : public GlobalTableFunctionState {
	DataFrameGlobalState(idx_t max_threads) : max_threads(max_threads) {
	}

	mutex lock;
	idx_t position = 0;
	idx_t max_threads;

	idx_t MaxThreads() const override {
		return max_threads;
	}
};

struct DataFrameLocalState : public LocalTableFunctionState {
	vector<column_t> column_ids;
	idx_t position;
	idx_t offset;
	idx_t count;
};

static unique_ptr<FunctionData> DataFrameScanBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
	data_frame df((SEXP)input.inputs[0].GetPointer());

	auto integer64 = get_bool_param(input.named_parameters, "integer64", false);
	auto experimental = get_bool_param(input.named_parameters, "experimental", false);

	auto df_names = df.names();
	vector<RType> rtypes;
	vector<data_ptr_t> data_ptrs;

	for (R_xlen_t col_idx = 0; col_idx < df.size(); col_idx++) {
		auto coldata = df[col_idx];
		LogicalType duckdb_col_type;
		data_ptr_t coldata_ptr = nullptr;

		names.push_back(df_names[col_idx]);
		rtypes.push_back(RApiTypes::DetectRType(coldata, integer64));

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
		case RType::INTEGER64:
			duckdb_col_type = LogicalType::BIGINT;
			coldata_ptr = (data_ptr_t)NUMERIC_POINTER(coldata);
			break;
		case RType::FACTOR: {
			// TODO What about factors that use numeric?
			coldata_ptr = (data_ptr_t)INTEGER_POINTER(coldata);
			strings levels = GET_LEVELS(coldata);
			Vector duckdb_levels(LogicalType::VARCHAR, levels.size());
			auto levels_ptr = FlatVector::GetData<string_t>(duckdb_levels);
			for (R_xlen_t level_idx = 0; level_idx < levels.size(); level_idx++) {
				levels_ptr[level_idx] = StringVector::AddString(duckdb_levels, (string)levels[level_idx]);
			}
			duckdb_col_type = LogicalType::ENUM(df_names[col_idx], duckdb_levels, levels.size());
			break;
		}
		case RType::STRING:
			if (experimental) {
				duckdb_col_type = LogicalType::DEDUP_POINTER_ENUM();
				coldata_ptr = (data_ptr_t)DATAPTR_RO(coldata);
			} else {
				duckdb_col_type = LogicalType::VARCHAR;
			}
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
			cpp11::stop("rapi_execute: Unsupported column type for bind");
		}

		return_types.push_back(duckdb_col_type);
		data_ptrs.push_back(coldata_ptr);
	}
	auto row_count = Rf_length(VECTOR_ELT(df, 0));
	return make_unique<DataFrameScanBindData>(df, row_count, rtypes, data_ptrs, input.named_parameters);
}

static idx_t DataFrameScanMaxThreads(ClientContext &context, const FunctionData *bind_data_p) {
	D_ASSERT(bind_data_p);
	auto bind_data = (const DataFrameScanBindData *)bind_data_p;
	if (!bind_data->experimental) {
		return 1;
	}
	return ceil((double)bind_data->row_count / bind_data->rows_per_task);
}

static unique_ptr<GlobalTableFunctionState> DataFrameScanInitGlobal(ClientContext &context,
                                                                    TableFunctionInitInput &input) {
	auto result = make_unique<DataFrameGlobalState>(DataFrameScanMaxThreads(context, input.bind_data));
	result->position = 0;
	return move(result);
}

static bool DataFrameScanParallelStateNext(ClientContext &context, const FunctionData *bind_data_p,
                                           DataFrameLocalState &local_state, DataFrameGlobalState &global_state) {
	auto &bind_data = (const DataFrameScanBindData &)*bind_data_p;

	lock_guard<mutex> parallel_lock(global_state.lock);
	if (global_state.position >= bind_data.row_count) {
		local_state.position = 0;
		local_state.offset = 0;
		local_state.count = 0;
		return false;
	}
	auto offset = global_state.position;
	auto count = MinValue<idx_t>(bind_data.rows_per_task, bind_data.row_count - offset);
	local_state.position = 0;
	local_state.offset = offset;
	local_state.count = count;

	global_state.position += bind_data.rows_per_task;
	return true;
}

static unique_ptr<LocalTableFunctionState> DataFrameScanInitLocal(ExecutionContext &context,
                                                                  TableFunctionInitInput &input,
                                                                  GlobalTableFunctionState *global_state) {
	auto &gstate = (DataFrameGlobalState &)*global_state;
	auto result = make_unique<DataFrameLocalState>();

	result->column_ids = input.column_ids;
	DataFrameScanParallelStateNext(context.client, input.bind_data, *result, gstate);
	return move(result);
}

struct DedupPointerEnumType {
	static bool IsNull(SEXP val) {
		return val == NA_STRING;
	}
	static uint64_t Convert(SEXP val) {
		return (uint64_t)DATAPTR(val);
	}
};

static void DataFrameScanFunc(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = (DataFrameScanBindData &)*data.bind_data;
	auto &operator_data = (DataFrameLocalState &)*data.local_state;
	auto &gstate = (DataFrameGlobalState &)*data.global_state;
	if (operator_data.position >= operator_data.count) {
		if (!DataFrameScanParallelStateNext(context, data.bind_data, operator_data, gstate)) {
			return;
		}
	}
	idx_t this_count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, operator_data.count - operator_data.position);
	output.SetCardinality(this_count);

	auto sexp_offset = operator_data.offset + operator_data.position;
	D_ASSERT(sexp_offset + this_count <= bind_data.row_count);

	for (R_xlen_t out_col_idx = 0; out_col_idx < output.ColumnCount(); out_col_idx++) {
		auto &v = output.data[out_col_idx];
		auto src_df_col_idx = operator_data.column_ids[out_col_idx];

		if (src_df_col_idx == COLUMN_IDENTIFIER_ROW_ID) {
			Value constant_42 = Value::BIGINT(42);
			output.data[out_col_idx].Reference(constant_42);
			continue;
		}

		auto coldata_ptr = bind_data.data_ptrs[src_df_col_idx];
		switch (bind_data.rtypes[src_df_col_idx]) {
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
		case RType::INTEGER64: {
			auto data_ptr = (int64_t *)coldata_ptr + sexp_offset;
			AppendColumnSegment<int64_t, int64_t, RInteger64Type>(data_ptr, v, this_count);
			break;
		}
		case RType::STRING: {
			if (bind_data.experimental) {
				auto data_ptr = (SEXP *)coldata_ptr + sexp_offset;
				//  DEDUP_POINTER_ENUM
				AppendColumnSegment<SEXP, uint64_t, DedupPointerEnumType>(data_ptr, v, this_count);
			} else {
				AppendStringSegment(((data_frame)bind_data.df)[(R_xlen_t)src_df_col_idx], v, sexp_offset, this_count);
			}

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
				cpp11::stop("rapi_execute: Unknown enum type for scan: %s",
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
			cpp11::stop("rapi_execute: Unsupported column type for scan");
		}
	}

	operator_data.position += this_count;
}

static unique_ptr<NodeStatistics> DataFrameScanCardinality(ClientContext &context, const FunctionData *bind_data_p) {
	auto &bind_data = (DataFrameScanBindData &)*bind_data_p;
	return make_unique<NodeStatistics>(bind_data.row_count, bind_data.row_count);
}

static string DataFrameScanToString(const FunctionData *bind_data_p) {
	return "data.frame";
}

DataFrameScanFunction::DataFrameScanFunction()
    : TableFunction("r_dataframe_scan", {LogicalType::POINTER}, DataFrameScanFunc, DataFrameScanBind,
                    DataFrameScanInitGlobal, DataFrameScanInitLocal) {
	cardinality = DataFrameScanCardinality;
	to_string = DataFrameScanToString;
	named_parameters["experimental"] = LogicalType::BOOLEAN;
	named_parameters["integer64"] = LogicalType::BOOLEAN;
	projection_pushdown = true;
}
