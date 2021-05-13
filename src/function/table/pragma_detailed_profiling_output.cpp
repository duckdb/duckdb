#include "duckdb/function/table/sqlite_functions.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/planner/constraints/bound_not_null_constraint.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/main/client_context.hpp"

#include "duckdb/common/limits.hpp"
namespace duckdb {

struct PragmaDetailedProfilingOutputOperatorData : public FunctionOperatorData {
	explicit PragmaDetailedProfilingOutputOperatorData() : chunk_index(0), initialized(false) {
	}
	idx_t chunk_index;
	bool initialized;
};

struct PragmaDetailedProfilingOutputData : public TableFunctionData {
	explicit PragmaDetailedProfilingOutputData(vector<LogicalType> &types) : types(types) {
	}
	unique_ptr<ChunkCollection> collection;
	vector<LogicalType> types;
};

static unique_ptr<FunctionData> PragmaDetailedProfilingOutputBind(ClientContext &context, vector<Value> &inputs,
                                                                  unordered_map<string, Value> &named_parameters,
                                                                  vector<LogicalType> &input_table_types,
                                                                  vector<string> &input_table_names,
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

	return make_unique<PragmaDetailedProfilingOutputData>(return_types);
}

unique_ptr<FunctionOperatorData> PragmaDetailedProfilingOutputInit(ClientContext &context,
                                                                   const FunctionData *bind_data,
                                                                   const vector<column_t> &column_ids,
                                                                   TableFilterCollection *filters) {
	return make_unique<PragmaDetailedProfilingOutputOperatorData>();
}

static void SetValue(DataChunk &output, int index, int op_id, int fun_id, string name, double time) {
	output.SetValue(0, index, op_id);
	output.SetValue(1, index, fun_id);
	output.SetValue(2, index, move(name));
	output.SetValue(3, index, time);
}

static void ExtractExpressions(ChunkCollection &collection, ExpressionInformation &info, DataChunk &chunk, int op_id,
                               int &fun_id, int sample_tuples_count) {
	if (info.hasfunction) {
		SetValue(chunk, chunk.size(), op_id, fun_id++, info.function_name, double(info.time) / sample_tuples_count);
		chunk.SetCardinality(chunk.size() + 1);
		if (chunk.size() == STANDARD_VECTOR_SIZE) {
			collection.Append(chunk);
			chunk.Reset();
		}
	}
	if (info.children.empty()) {
		return;
	}
	// extract the children of this node
	for (auto &child : info.children) {
		ExtractExpressions(collection, *child, chunk, op_id, fun_id, sample_tuples_count);
	}
}

static void PragmaDetailedProfilingOutputFunction(ClientContext &context, const FunctionData *bind_data_p,
                                                  FunctionOperatorData *operator_state, DataChunk *input,
                                                  DataChunk &output) {
	auto &state = (PragmaDetailedProfilingOutputOperatorData &)*operator_state;
	auto &data = (PragmaDetailedProfilingOutputData &)*bind_data_p;
	if (!state.initialized) {
		// create a ChunkCollection
		auto collection = make_unique<ChunkCollection>();

		DataChunk chunk;
		chunk.Initialize(data.types);

		int operator_counter = 1;
		if (!context.query_profiler_history.GetPrevProfilers().empty()) {
			for (auto op : context.query_profiler_history.GetPrevProfilers().back().second.GetTreeMap()) {
				int function_counter = 1;
				if (op.second->info.has_executor) {
					for (auto &info : op.second->info.executors_info->roots) {
						ExtractExpressions(*collection, *info, chunk, operator_counter, function_counter,
						                   op.second->info.executors_info->sample_tuples_count);
					}
				}
				operator_counter++;
			}
		}
		collection->Append(chunk);
		data.collection = move(collection);
		state.initialized = true;
	}

	if (state.chunk_index >= data.collection->ChunkCount()) {
		output.SetCardinality(0);
		return;
	}
	output.Reference(data.collection->GetChunk(state.chunk_index++));
}

void PragmaDetailedProfilingOutput::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("pragma_detailed_profiling_output", {}, PragmaDetailedProfilingOutputFunction,
	                              PragmaDetailedProfilingOutputBind, PragmaDetailedProfilingOutputInit));
}

} // namespace duckdb
