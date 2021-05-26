#include "duckdbr.hpp"
#include "duckdb/main/client_context.hpp"

using namespace duckdb;

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

struct DataFrameScanFunction : public TableFunction {
	DataFrameScanFunction()
	    : TableFunction("r_dataframe_scan", {LogicalType::POINTER}, dataframe_scan_function, dataframe_scan_bind,
	                    dataframe_scan_init, nullptr, nullptr, nullptr, dataframe_scan_cardinality) {};

	static unique_ptr<FunctionData> dataframe_scan_bind(ClientContext &context, vector<Value> &inputs,
	                                                    unordered_map<string, Value> &named_parameters,
	                                                    vector<LogicalType> &input_table_types,
	                                                    vector<string> &input_table_names,
	                                                    vector<LogicalType> &return_types, vector<string> &names) {
		RProtector r;
		SEXP df((SEXP)inputs[0].GetPointer());

		auto df_names = r.Protect(GET_NAMES(df));
		vector<RType> rtypes;

		for (idx_t col_idx = 0; col_idx < (idx_t)Rf_length(df); col_idx++) {
			names.push_back(string(CHAR(STRING_ELT(df_names, col_idx))));
			SEXP coldata = VECTOR_ELT(df, col_idx);
			rtypes.push_back(RApi::DetectRType(coldata));
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
			case RType::FACTOR:
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
				duckdb_col_type = LogicalType::TIME;
				break;
			case RType::DATE:
				duckdb_col_type = LogicalType::DATE;
				break;
			default:
				Rf_error("Unsupported column type for scan");
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
	                                    FunctionOperatorData *operator_state, DataChunk *input, DataChunk &output) {
		auto &data = (DataFrameScanFunctionData &)*bind_data;
		auto &state = (DataFrameScanState &)*operator_state;
		if (state.position >= data.row_count) {
			return;
		}
		idx_t this_count = std::min((idx_t)STANDARD_VECTOR_SIZE, data.row_count - state.position);

		output.SetCardinality(this_count);

		// TODO this is quite similar to append, unify!
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
			case RType::STRING:
				AppendStringSegment(coldata, v, state.position, this_count);
				break;
			case RType::FACTOR:
				AppendFactor(coldata, v, state.position, this_count);
				break;
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
			case RType::DATE: {
				auto data_ptr = NUMERIC_POINTER(coldata) + state.position;
				AppendColumnSegment<double, date_t, RDateType>(data_ptr, v, this_count);
				break;
			}
			default:
				throw;
			}
		}

		state.position += this_count;
	}

	static unique_ptr<NodeStatistics> dataframe_scan_cardinality(ClientContext &context,
	                                                             const FunctionData *bind_data) {
		auto &data = (DataFrameScanFunctionData &)*bind_data;
		return make_unique<NodeStatistics>(data.row_count, data.row_count);
	}
};
