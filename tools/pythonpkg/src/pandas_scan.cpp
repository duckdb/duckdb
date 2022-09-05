#include "duckdb_python/pandas_scan.hpp"
#include "duckdb_python/array_wrapper.hpp"
#include "utf8proc_wrapper.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb_python/vector_conversion.hpp"
#include "duckdb/main/client_context.hpp"

#include "duckdb/common/atomic.hpp"

namespace duckdb {

struct PandasScanFunctionData : public PyTableFunctionData {
	PandasScanFunctionData(py::handle df, idx_t row_count, vector<PandasColumnBindData> pandas_bind_data,
	                       vector<LogicalType> sql_types)
	    : df(df), row_count(row_count), lines_read(0), pandas_bind_data(move(pandas_bind_data)),
	      sql_types(move(sql_types)) {
	}
	py::handle df;
	idx_t row_count;
	atomic<idx_t> lines_read;
	vector<PandasColumnBindData> pandas_bind_data;
	vector<LogicalType> sql_types;

	~PandasScanFunctionData() override {
		py::gil_scoped_acquire acquire;
		pandas_bind_data.clear();
	}
};

struct PandasScanLocalState : public LocalTableFunctionState {
	PandasScanLocalState(idx_t start, idx_t end) : start(start), end(end), batch_index(0) {
	}

	idx_t start;
	idx_t end;
	idx_t batch_index;
	vector<column_t> column_ids;
};

struct PandasScanGlobalState : public GlobalTableFunctionState {
	explicit PandasScanGlobalState(idx_t max_threads) : position(0), batch_index(0), max_threads(max_threads) {
	}

	std::mutex lock;
	idx_t position;
	idx_t batch_index;
	idx_t max_threads;

	idx_t MaxThreads() const override {
		return max_threads;
	}
};

PandasScanFunction::PandasScanFunction()
    : TableFunction("pandas_scan", {LogicalType::POINTER}, PandasScanFunc, PandasScanBind, PandasScanInitGlobal,
                    PandasScanInitLocal) {
	get_batch_index = PandasScanGetBatchIndex;
	cardinality = PandasScanCardinality;
	table_scan_progress = PandasProgress;
	projection_pushdown = true;
}

idx_t PandasScanFunction::PandasScanGetBatchIndex(ClientContext &context, const FunctionData *bind_data_p,
                                                  LocalTableFunctionState *local_state,
                                                  GlobalTableFunctionState *global_state) {
	auto &data = (PandasScanLocalState &)*local_state;
	return data.batch_index;
}

unique_ptr<FunctionData> PandasScanFunction::PandasScanBind(ClientContext &context, TableFunctionBindInput &input,
                                                            vector<LogicalType> &return_types, vector<string> &names) {
	py::gil_scoped_acquire acquire;
	py::handle df((PyObject *)(input.inputs[0].GetPointer()));

	vector<PandasColumnBindData> pandas_bind_data;
	VectorConversion::BindPandas(DBConfig::GetConfig(context), df, pandas_bind_data, return_types, names);

	auto df_columns = py::list(df.attr("columns"));
	auto get_fun = df.attr("__getitem__");

	idx_t row_count = py::len(get_fun(df_columns[0]));
	return make_unique<PandasScanFunctionData>(df, row_count, move(pandas_bind_data), return_types);
}

unique_ptr<GlobalTableFunctionState> PandasScanFunction::PandasScanInitGlobal(ClientContext &context,
                                                                              TableFunctionInitInput &input) {
	return make_unique<PandasScanGlobalState>(PandasScanMaxThreads(context, input.bind_data));
}

unique_ptr<LocalTableFunctionState> PandasScanFunction::PandasScanInitLocal(ExecutionContext &context,
                                                                            TableFunctionInitInput &input,
                                                                            GlobalTableFunctionState *gstate) {
	auto result = make_unique<PandasScanLocalState>(0, 0);
	result->column_ids = input.column_ids;
	PandasScanParallelStateNext(context.client, input.bind_data, result.get(), gstate);
	return move(result);
}

idx_t PandasScanFunction::PandasScanMaxThreads(ClientContext &context, const FunctionData *bind_data_p) {
	if (ClientConfig::GetConfig(context).verify_parallelism) {
		return context.db->NumberOfThreads();
	}
	auto &bind_data = (const PandasScanFunctionData &)*bind_data_p;
	return bind_data.row_count / PANDAS_PARTITION_COUNT + 1;
}

bool PandasScanFunction::PandasScanParallelStateNext(ClientContext &context, const FunctionData *bind_data_p,
                                                     LocalTableFunctionState *lstate,
                                                     GlobalTableFunctionState *gstate) {
	auto &bind_data = (const PandasScanFunctionData &)*bind_data_p;
	auto &parallel_state = (PandasScanGlobalState &)*gstate;
	auto &state = (PandasScanLocalState &)*lstate;

	lock_guard<mutex> parallel_lock(parallel_state.lock);
	if (parallel_state.position >= bind_data.row_count) {
		return false;
	}
	state.start = parallel_state.position;
	parallel_state.position += PANDAS_PARTITION_COUNT;
	if (parallel_state.position > bind_data.row_count) {
		parallel_state.position = bind_data.row_count;
	}
	state.end = parallel_state.position;
	state.batch_index = parallel_state.batch_index++;
	return true;
}

double PandasScanFunction::PandasProgress(ClientContext &context, const FunctionData *bind_data_p,
                                          const GlobalTableFunctionState *gstate) {
	auto &bind_data = (const PandasScanFunctionData &)*bind_data_p;
	if (bind_data.row_count == 0) {
		return 100;
	}
	auto percentage = (bind_data.lines_read * 100.0) / bind_data.row_count;
	return percentage;
}

//! The main pandas scan function: note that this can be called in parallel without the GIL
//! hence this needs to be GIL-safe, i.e. no methods that create Python objects are allowed
void PandasScanFunction::PandasScanFunc(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = (PandasScanFunctionData &)*data_p.bind_data;
	auto &state = (PandasScanLocalState &)*data_p.local_state;

	if (state.start >= state.end) {
		if (!PandasScanParallelStateNext(context, data_p.bind_data, data_p.local_state, data_p.global_state)) {
			return;
		}
	}
	idx_t this_count = std::min((idx_t)STANDARD_VECTOR_SIZE, state.end - state.start);
	output.SetCardinality(this_count);
	for (idx_t idx = 0; idx < state.column_ids.size(); idx++) {
		auto col_idx = state.column_ids[idx];
		if (col_idx == COLUMN_IDENTIFIER_ROW_ID) {
			output.data[idx].Sequence(state.start, this_count);
		} else {
			VectorConversion::NumpyToDuckDB(data.pandas_bind_data[col_idx], data.pandas_bind_data[col_idx].numpy_col,
			                                this_count, state.start, output.data[idx]);
		}
	}
	state.start += this_count;
	data.lines_read += this_count;
}

unique_ptr<NodeStatistics> PandasScanFunction::PandasScanCardinality(ClientContext &context,
                                                                     const FunctionData *bind_data) {
	auto &data = (PandasScanFunctionData &)*bind_data;
	return make_unique<NodeStatistics>(data.row_count, data.row_count);
}

py::object PandasScanFunction::PandasReplaceCopiedNames(const py::object &original_df) {
	auto copy_df = original_df.attr("copy")(false);
	unordered_map<string, idx_t> name_map;
	unordered_set<string> columns_seen;
	py::list column_name_list;
	auto df_columns = py::list(original_df.attr("columns"));

	for (auto &column_name_py : df_columns) {
		string column_name = py::str(column_name_py);
		// put it all lower_case
		auto column_name_low = StringUtil::Lower(column_name);
		name_map[column_name_low] = 1;
	}
	for (auto &column_name_py : df_columns) {
		const string column_name = py::str(column_name_py);
		auto column_name_low = StringUtil::Lower(column_name);
		if (columns_seen.find(column_name_low) == columns_seen.end()) {
			// `column_name` has not been seen before -> It isn't a duplicate
			column_name_list.append(column_name);
			columns_seen.insert(column_name_low);
		} else {
			// `column_name` already seen. Deduplicate by with suffix _{x} where x starts at the repetition number of
			// `column_name` If `column_name_{x}` already exists in `name_map`, increment x and try again.
			string new_column_name = column_name + "_" + std::to_string(name_map[column_name_low]);
			auto new_column_name_low = StringUtil::Lower(new_column_name);
			while (name_map.find(new_column_name_low) != name_map.end()) {
				// This name is already here due to a previous definition
				name_map[column_name_low]++;
				new_column_name = column_name + "_" + std::to_string(name_map[column_name_low]);
				new_column_name_low = StringUtil::Lower(new_column_name);
			}
			column_name_list.append(new_column_name);
			columns_seen.insert(new_column_name_low);
			name_map[column_name_low]++;
		}
	}

	copy_df.attr("columns") = column_name_list;
	return copy_df;
}

} // namespace duckdb
