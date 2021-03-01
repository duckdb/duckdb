#include "duckdb/function/table/sqlite_functions.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/planner/constraints/bound_not_null_constraint.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/main/client_context.hpp"

#include "duckdb/common/limits.hpp"
namespace duckdb {

struct PragmaLastProfilingOutputData : public FunctionOperatorData {
	explicit PragmaLastProfilingOutputData(idx_t rows) : rows(rows) {
	}
	idx_t rows;
};

static unique_ptr<FunctionData> PragmaLastProfilingOutputBind(ClientContext &context, vector<Value> &inputs,
                                                              unordered_map<string, Value> &named_parameters,
                                                              vector<LogicalType> &return_types,
                                                              vector<string> &names) {
	names.emplace_back("OPERATOR_ID");
	return_types.push_back(LogicalType::INTEGER);

	names.emplace_back("NAME");
	return_types.push_back(LogicalType::VARCHAR);

	names.emplace_back("TIME");
	return_types.push_back(LogicalType::DOUBLE);

    names.emplace_back("CARDINALITY");
    return_types.push_back(LogicalType::BIGINT);

    names.emplace_back("DESCRIPTION");
    return_types.push_back(LogicalType::VARCHAR);

	return make_unique<TableFunctionData>();
}

unique_ptr<FunctionOperatorData> PragmaLastProfilingOutputInit(ClientContext &context, const FunctionData *bind_data,
                                                               vector<column_t> &column_ids,
                                                               TableFilterCollection *filters) {
	return make_unique<PragmaLastProfilingOutputData>(1024);
}

static void SetValue(DataChunk &output, int index, int op_id, string name, double time, int64_t car, string description) {
	output.SetValue(0, index, op_id);
	output.SetValue(1, index, move(name));
    output.SetValue(2, index, time);
    output.SetValue(3, index, car);
	output.SetValue(4, index, move(description));

}

static void PragmaLastProfilingOutputFunction(ClientContext &context, const FunctionData *bind_data_p,
                                              FunctionOperatorData *operator_state, DataChunk &output) {
	auto &state = (PragmaLastProfilingOutputData &)*operator_state;
	if (state.rows > 0) {
		int total_counter = 0;
		int operator_counter = 1;
//		SetValue(output, total_counter++, 0, "Query: " + context.prev_profiler.query,
//		         context.prev_profiler.main_query.Elapsed(), 0, "");
		for (auto op : context.prev_profiler.tree_map) {
			SetValue(output, total_counter++, operator_counter++, op.second->name, op.second->info.time, op.second->info.elements, " ");
		}
		state.rows = 0;
		output.SetCardinality(total_counter);
	} else {
		output.SetCardinality(0);
	}
}

void PragmaLastProfilingOutput::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("pragma_last_profiling_output", {}, PragmaLastProfilingOutputFunction,
	                              PragmaLastProfilingOutputBind, PragmaLastProfilingOutputInit));
}

} // namespace duckdb
