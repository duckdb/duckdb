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

struct PragmaLastProfilingOutputOperatorData : public GlobalTableFunctionState {
	PragmaLastProfilingOutputOperatorData() : initialized(false) {
	}

	ColumnDataScanState scan_state;
	bool initialized;
};

struct PragmaLastProfilingOutputData : public TableFunctionData {
	explicit PragmaLastProfilingOutputData(vector<LogicalType> &types) : types(types) {
	}
	unique_ptr<ColumnDataCollection> collection;
	vector<LogicalType> types;
};

static unique_ptr<FunctionData> PragmaLastProfilingOutputBind(ClientContext &context, TableFunctionBindInput &input,
                                                              vector<LogicalType> &return_types,
                                                              vector<string> &names) {
	names.emplace_back("OPERATOR_ID");
	return_types.emplace_back(LogicalType::INTEGER);

	names.emplace_back("NAME");
	return_types.emplace_back(LogicalType::VARCHAR);

	names.emplace_back("TIME");
	return_types.emplace_back(LogicalType::DOUBLE);

	names.emplace_back("CARDINALITY");
	return_types.emplace_back(LogicalType::BIGINT);

	names.emplace_back("DESCRIPTION");
	return_types.emplace_back(LogicalType::VARCHAR);

	return make_uniq<PragmaLastProfilingOutputData>(return_types);
}

static void SetValue(DataChunk &output, int index, int op_id, string name, double time, int64_t car,
                     string description) {
	output.SetValue(0, index, op_id);
	output.SetValue(1, index, std::move(name));
	output.SetValue(2, index, time);
	output.SetValue(3, index, car);
	output.SetValue(4, index, std::move(description));
}

unique_ptr<GlobalTableFunctionState> PragmaLastProfilingOutputInit(ClientContext &context,
                                                                   TableFunctionInitInput &input) {
	return make_uniq<PragmaLastProfilingOutputOperatorData>();
}

static void PragmaLastProfilingOutputFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<PragmaLastProfilingOutputOperatorData>();
	auto &data = data_p.bind_data->CastNoConst<PragmaLastProfilingOutputData>();
	if (!state.initialized) {
		// create a ColumnDataCollection
		auto collection = make_uniq<ColumnDataCollection>(context, data.types);

		DataChunk chunk;
		chunk.Initialize(context, data.types);
		int operator_counter = 1;
		auto &client_data = ClientData::Get(context);
		if (!client_data.query_profiler_history->GetPrevProfilers().empty()) {
			auto &tree_map = client_data.query_profiler_history->GetPrevProfilers().back().second->GetTreeMap();
			for (auto op : tree_map) {
				auto &tree_info = op.second.get();
				SetValue(chunk, chunk.size(), operator_counter++, tree_info.name, tree_info.info.time,
				         tree_info.info.elements, " ");
				chunk.SetCardinality(chunk.size() + 1);
				if (chunk.size() == STANDARD_VECTOR_SIZE) {
					collection->Append(chunk);
					chunk.Reset();
				}
			}
		}
		collection->Append(chunk);
		data.collection = std::move(collection);
		data.collection->InitializeScan(state.scan_state);
		state.initialized = true;
	}

	data.collection->Scan(state.scan_state, output);
}

void PragmaLastProfilingOutput::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("pragma_last_profiling_output", {}, PragmaLastProfilingOutputFunction,
	                              PragmaLastProfilingOutputBind, PragmaLastProfilingOutputInit));
}

} // namespace duckdb
