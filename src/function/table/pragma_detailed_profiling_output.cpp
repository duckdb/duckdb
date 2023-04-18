#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/function/table/system_functions.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/query_profiler.hpp"
#include "duckdb/planner/constraints/bound_not_null_constraint.hpp"

namespace duckdb {

struct PragmaDetailedProfilingOutputOperatorData : public GlobalTableFunctionState {
	explicit PragmaDetailedProfilingOutputOperatorData() : initialized(false) {
	}

	ColumnDataScanState scan_state;
	bool initialized;
};

struct PragmaDetailedProfilingOutputData : public TableFunctionData {
	explicit PragmaDetailedProfilingOutputData(vector<LogicalType> &types) : types(types) {
	}
	unique_ptr<ColumnDataCollection> collection;
	vector<LogicalType> types;
};

static unique_ptr<FunctionData> PragmaDetailedProfilingOutputBind(ClientContext &context, TableFunctionBindInput &input,
                                                                  vector<LogicalType> &return_types,
                                                                  vector<string> &names) {
	names.emplace_back("OPERATOR_ID");
	return_types.emplace_back(LogicalType::INTEGER);

	names.emplace_back("ANNOTATION");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("ID");
	return_types.emplace_back(LogicalType::INTEGER);

	names.emplace_back("NAME");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("TIME");
	return_types.emplace_back(LogicalType::DOUBLE);

	names.emplace_back("CYCLES_PER_TUPLE");
	return_types.emplace_back(LogicalType::DOUBLE);

	names.emplace_back("SAMPLE_SIZE");
	return_types.emplace_back(LogicalType::INTEGER);

	names.emplace_back("INPUT_SIZE");
	return_types.emplace_back(LogicalType::INTEGER);

	names.emplace_back("EXTRA_INFO");
	return_types.emplace_back(LogicalType::VARCHAR);

	return make_uniq<PragmaDetailedProfilingOutputData>(return_types);
}

unique_ptr<GlobalTableFunctionState> PragmaDetailedProfilingOutputInit(ClientContext &context,
                                                                       TableFunctionInitInput &input) {
	return make_uniq<PragmaDetailedProfilingOutputOperatorData>();
}

// Insert a row into the given datachunk
static void SetValue(DataChunk &output, int index, int op_id, string annotation, int id, string name, double time,
                     int sample_counter, int tuple_counter, string extra_info) {
	output.SetValue(0, index, op_id);
	output.SetValue(1, index, std::move(annotation));
	output.SetValue(2, index, id);
	output.SetValue(3, index, std::move(name));
#if defined(RDTSC)
	output.SetValue(4, index, Value(nullptr));
	output.SetValue(5, index, time);
#else
	output.SetValue(4, index, time);
	output.SetValue(5, index, Value(nullptr));

#endif
	output.SetValue(6, index, sample_counter);
	output.SetValue(7, index, tuple_counter);
	output.SetValue(8, index, std::move(extra_info));
}

static void ExtractFunctions(ColumnDataCollection &collection, ExpressionInfo &info, DataChunk &chunk, int op_id,
                             int &fun_id) {
	if (info.hasfunction) {
		D_ASSERT(info.sample_tuples_count != 0);
		SetValue(chunk, chunk.size(), op_id, "Function", fun_id++, info.function_name,
		         int(info.function_time) / double(info.sample_tuples_count), info.sample_tuples_count,
		         info.tuples_count, "");

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
		ExtractFunctions(collection, *child, chunk, op_id, fun_id);
	}
}

static void PragmaDetailedProfilingOutputFunction(ClientContext &context, TableFunctionInput &data_p,
                                                  DataChunk &output) {
	auto &state = data_p.global_state->Cast<PragmaDetailedProfilingOutputOperatorData>();
	auto &data = data_p.bind_data->CastNoConst<PragmaDetailedProfilingOutputData>();

	if (!state.initialized) {
		// create a ColumnDataCollection
		auto collection = make_uniq<ColumnDataCollection>(context, data.types);

		// create a chunk
		DataChunk chunk;
		chunk.Initialize(context, data.types);

		// Initialize ids
		int operator_counter = 1;
		int function_counter = 1;
		int expression_counter = 1;
		auto &client_data = ClientData::Get(context);
		if (client_data.query_profiler_history->GetPrevProfilers().empty()) {
			return;
		}
		// For each Operator
		auto &tree_map = client_data.query_profiler_history->GetPrevProfilers().back().second->GetTreeMap();
		for (auto op : tree_map) {
			// For each Expression Executor
			for (auto &expr_executor : op.second.get().info.executors_info) {
				// For each Expression tree
				if (!expr_executor) {
					continue;
				}
				for (auto &expr_timer : expr_executor->roots) {
					D_ASSERT(expr_timer->sample_tuples_count != 0);
					SetValue(chunk, chunk.size(), operator_counter, "ExpressionRoot", expression_counter++,
					         // Sometimes, cycle counter is not accurate, too big or too small. return 0 for
					         // those cases
					         expr_timer->name, int(expr_timer->time) / double(expr_timer->sample_tuples_count),
					         expr_timer->sample_tuples_count, expr_timer->tuples_count, expr_timer->extra_info);
					// Increment cardinality
					chunk.SetCardinality(chunk.size() + 1);
					// Check whether data chunk is full or not
					if (chunk.size() == STANDARD_VECTOR_SIZE) {
						collection->Append(chunk);
						chunk.Reset();
					}
					// Extract all functions inside the tree
					ExtractFunctions(*collection, *expr_timer->root, chunk, operator_counter, function_counter);
				}
			}
			operator_counter++;
		}
		collection->Append(chunk);
		data.collection = std::move(collection);
		data.collection->InitializeScan(state.scan_state);
		state.initialized = true;
	}

	data.collection->Scan(state.scan_state, output);
}

void PragmaDetailedProfilingOutput::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("pragma_detailed_profiling_output", {}, PragmaDetailedProfilingOutputFunction,
	                              PragmaDetailedProfilingOutputBind, PragmaDetailedProfilingOutputInit));
}

} // namespace duckdb
