#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/function/table/system_functions.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/planner/constraints/bound_not_null_constraint.hpp"
#include "duckdb/main/query_profiler.hpp"

namespace duckdb {

struct PragmaLastProfilingOutputOperatorData : public GlobalTableFunctionState {
	PragmaLastProfilingOutputOperatorData() : chunk_index(0), initialized(false) {
	}
	idx_t chunk_index;
	bool initialized;
};

struct PragmaLastProfilingOutputData : public TableFunctionData {
	explicit PragmaLastProfilingOutputData(vector<LogicalType> &types) : types(types) {
	}
	unique_ptr<ChunkCollection> collection;
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

	return make_unique<PragmaLastProfilingOutputData>(return_types);
}

static void SetValue(DataChunk &output, int index, int op_id, string name, double time, int64_t car,
                     string description) {
	output.SetValue(0, index, op_id);
	output.SetValue(1, index, move(name));
	output.SetValue(2, index, time);
	output.SetValue(3, index, car);
	output.SetValue(4, index, move(description));
}

unique_ptr<GlobalTableFunctionState> PragmaLastProfilingOutputInit(ClientContext &context,
                                                                   TableFunctionInitInput &input) {
	return make_unique<PragmaLastProfilingOutputOperatorData>();
}

static void PragmaLastProfilingOutputFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = (PragmaLastProfilingOutputOperatorData &)*data_p.global_state;
	auto &data = (PragmaLastProfilingOutputData &)*data_p.bind_data;
	if (!state.initialized) {
		// create a ChunkCollection
		auto collection = make_unique<ChunkCollection>();

		DataChunk chunk;
		chunk.Initialize(data.types);
		int operator_counter = 1;
		if (!ClientData::Get(context).query_profiler_history->GetPrevProfilers().empty()) {
			for (auto op :
			     ClientData::Get(context).query_profiler_history->GetPrevProfilers().back().second->GetTreeMap()) {
				SetValue(chunk, chunk.size(), operator_counter++, op.second->name, op.second->info.time,
				         op.second->info.elements, " ");
				chunk.SetCardinality(chunk.size() + 1);
				if (chunk.size() == STANDARD_VECTOR_SIZE) {
					collection->Append(chunk);
					chunk.Reset();
				}
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

void PragmaLastProfilingOutput::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("pragma_last_profiling_output", {}, PragmaLastProfilingOutputFunction,
	                              PragmaLastProfilingOutputBind, PragmaLastProfilingOutputInit));
}

} // namespace duckdb
