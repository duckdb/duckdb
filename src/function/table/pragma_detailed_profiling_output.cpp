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

	names.emplace_back("ANNOTATION");
	return_types.push_back(LogicalType::VARCHAR);

	names.emplace_back("ID");
	return_types.push_back(LogicalType::INTEGER);

	names.emplace_back("NAME");
	return_types.push_back(LogicalType::VARCHAR);

	names.emplace_back("TIME/CyclePerTuple");
	return_types.push_back(LogicalType::DOUBLE);

	names.emplace_back("SAMPLE_SIZE");
	return_types.push_back(LogicalType::INTEGER);

	names.emplace_back("INPUT_SIZE");
	return_types.push_back(LogicalType::INTEGER);

	names.emplace_back("EXTRA_INFO");
	return_types.push_back(LogicalType::VARCHAR);

	return make_unique<PragmaDetailedProfilingOutputData>(return_types);
}

unique_ptr<FunctionOperatorData> PragmaDetailedProfilingOutputInit(ClientContext &context,
                                                                   const FunctionData *bind_data,
                                                                   vector<column_t> &column_ids,
                                                                   TableFilterCollection *filters) {
	return make_unique<PragmaDetailedProfilingOutputOperatorData>();
}

// Insert a row into the given datachunk
static void SetValue(DataChunk &output, int index, int op_id, string annotation, int id, string name, double time,
                     int sample_counter, int tuple_counter, string extra_info) {
	output.SetValue(0, index, op_id);
	output.SetValue(1, index, move(annotation));
	output.SetValue(2, index, id);
	output.SetValue(3, index, move(name));
	output.SetValue(4, index, time);
	output.SetValue(5, index, sample_counter);
	output.SetValue(6, index, tuple_counter);
	output.SetValue(7, index, move(extra_info));
}

static void ExtractFunctions(ChunkCollection &collection, ExpressionInfo &info, DataChunk &chunk, int op_id,
                             int &fun_id, int sample_tuples_count, int tuples_count) {
	if (info.hasfunction) {
		SetValue(chunk, chunk.size(), op_id, "Function", fun_id++, info.function_name,
		         double(info.function_time) / sample_tuples_count, sample_tuples_count, tuples_count, "");
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
		ExtractFunctions(collection, *child, chunk, op_id, fun_id, sample_tuples_count, tuples_count);
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

		// create a chunk
		DataChunk chunk;
		chunk.Initialize(data.types);

		// Initialize ids
		int operator_counter = 1;
		int function_counter = 1;
		int expression_counter = 1;
		if (!context.query_profiler_history.GetPrevProfilers().empty()) {
			// For each Operator
			for (auto op : context.query_profiler_history.GetPrevProfilers().back().second.GetTreeMap()) {
				// For each Expression Executor
				for (auto &ee : op.second->info.executors_info) {
					// For each Expression tree
					for (auto &et : ee.second->roots) {
						SetValue(chunk, chunk.size(), operator_counter, "ExpressionRoot", expression_counter++,
						         et->name, double(et->time) / et->sample_tuples_count, et->sample_tuples_count,
						         et->tuples_count, et->extra_info);
						// Increment cardinality
						chunk.SetCardinality(chunk.size() + 1);
						// Check whether data chunk is full or not
						if (chunk.size() == STANDARD_VECTOR_SIZE) {
							collection->Append(chunk);
							chunk.Reset();
						}
						// Extract all functions inside the tree
						ExtractFunctions(*collection, *et->root, chunk, operator_counter, function_counter,
						                 et->sample_tuples_count, et->tuples_count);
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
