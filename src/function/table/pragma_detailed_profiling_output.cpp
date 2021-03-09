#include "duckdb/function/table/sqlite_functions.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/planner/constraints/bound_not_null_constraint.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/main/client_context.hpp"

#include "duckdb/common/limits.hpp"
namespace duckdb {

struct PragmaDetailedProfilingOutputData : public FunctionOperatorData {
	explicit PragmaDetailedProfilingOutputData(idx_t rows) : rows(rows) {
	}
	idx_t rows;
};

static unique_ptr<FunctionData> PragmaDetailedProfilingOutputBind(ClientContext &context, vector<Value> &inputs,
                                                                  unordered_map<string, Value> &named_parameters,
                                                                  vector<LogicalType> &return_types,
                                                                  vector<string> &names) {
	names.emplace_back("OPERATOR_ID");
	return_types.push_back(LogicalType::INTEGER);

	names.emplace_back("FUNCTION_ID");
	return_types.push_back(LogicalType::INTEGER);

	names.emplace_back("NAME");
	return_types.push_back(LogicalType::VARCHAR);

	names.emplace_back("TIME");
	return_types.push_back(LogicalType::DOUBLE);

	return make_unique<TableFunctionData>();
}

unique_ptr<FunctionOperatorData> PragmaDetailedProfilingOutputInit(ClientContext &context,
                                                                   const FunctionData *bind_data,
                                                                   vector<column_t> &column_ids,
                                                                   TableFilterCollection *filters) {
	return make_unique<PragmaDetailedProfilingOutputData>(1024);
}

static void SetValue(DataChunk &output, int index, int op_id, int fun_id, string name, double time) {
	output.SetValue(0, index, op_id);
	output.SetValue(1, index, fun_id);
	output.SetValue(2, index, move(name));
	output.SetValue(3, index, time);
}

static void ExtractExpressions(ExpressionInformation &info, DataChunk &output, int &index, int op_id, int &fun_id,
                               int sample_tuples_count) {
	if (info.hasfunction) {
		SetValue(output, index++, op_id, fun_id++, info.function_name, double(info.time) / sample_tuples_count);
	}
	if (info.children.empty()) {
		return;
	}
	// extract the children of this node
	for (auto &child : info.children) {
		ExtractExpressions(*child, output, index, op_id, fun_id, sample_tuples_count);
	}
	return;
}

static void PragmaDetailedProfilingOutputFunction(ClientContext &context, const FunctionData *bind_data_p,
                                                  FunctionOperatorData *operator_state, DataChunk &output) {
	auto &state = (PragmaDetailedProfilingOutputData &)*operator_state;
	if (state.rows > 0) {
		int total_counter = 0;
		int operator_counter = 1;
		//        SetValue(output, total_counter++, 0, 0, "Query: " + context.prev_profiler.query,
		//                 context.prev_profiler.main_query.Elapsed());
		if (!context.prev_profilers.empty()) {
			for (auto op : context.prev_profilers.back().second.tree_map) {
				int function_counter = 1;
				if (op.second->info.has_executor) {
					for (auto &info : op.second->info.executors_info->roots) {
						ExtractExpressions(*info, output, total_counter, operator_counter, function_counter,
						                   op.second->info.executors_info->sample_tuples_count);
					}
				}
				operator_counter++;
			}
		}
		state.rows = 0;
		output.SetCardinality(total_counter);
	} else {
		output.SetCardinality(0);
	}
}

void PragmaDetailedProfilingOutput::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("pragma_detailed_profiling_output", {}, PragmaDetailedProfilingOutputFunction,
	                              PragmaDetailedProfilingOutputBind, PragmaDetailedProfilingOutputInit));
}

} // namespace duckdb
