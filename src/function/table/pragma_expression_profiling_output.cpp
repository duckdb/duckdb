#include "duckdb/function/table/sqlite_functions.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/planner/constraints/bound_not_null_constraint.hpp"
#include "duckdb/main/client_context.hpp"
#include "iostream"

#include "duckdb/common/limits.hpp"
namespace duckdb {

struct PragmaExpressionsProfilingOutputOperatorData : public FunctionOperatorData {
    explicit PragmaExpressionsProfilingOutputOperatorData() : chunk_index(0), initialized(false) {
    }
    idx_t chunk_index;
    bool initialized;
};

struct PragmaExpressionsProfilingOutputData : public TableFunctionData {
    explicit PragmaExpressionsProfilingOutputData(vector<LogicalType> &types) : types(types) {
    }
    unique_ptr<ChunkCollection> collection;
    vector<LogicalType> types;
};

static unique_ptr<FunctionData> PragmaExpressionsProfilingOutputBind(ClientContext &context, vector<Value> &inputs,
                                                                  unordered_map<string, Value> &named_parameters,
                                                                  vector<LogicalType> &return_types,
                                                                  vector<string> &names) {
    names.emplace_back("OPERATOR_ID");
    return_types.push_back(LogicalType::INTEGER);

    names.emplace_back("EXPRESSION_ID");
    return_types.push_back(LogicalType::INTEGER);

    names.emplace_back("NAME");
    return_types.push_back(LogicalType::VARCHAR);

    names.emplace_back("TIME");
    return_types.push_back(LogicalType::DOUBLE);

    return make_unique<PragmaExpressionsProfilingOutputData>(return_types);
}

unique_ptr<FunctionOperatorData> PragmaExpressionsProfilingOutputInit(ClientContext &context,
                                                                   const FunctionData *bind_data,
                                                                   vector<column_t> &column_ids,
                                                                   TableFilterCollection *filters) {
    return make_unique<PragmaExpressionsProfilingOutputOperatorData>();
}

static void ExpressionSetValue(DataChunk &output, int index, int op_id, int exp_id, string name, double time) {
    output.SetValue(0, index, op_id);
    output.SetValue(1, index, exp_id);
    output.SetValue(2, index, move(name));
    output.SetValue(3, index, time);
}



static void PragmaExpressionsProfilingOutputFunction(ClientContext &context, const FunctionData *bind_data_p,
                                                  FunctionOperatorData *operator_state, DataChunk &output) {
    auto &state = (PragmaExpressionsProfilingOutputOperatorData &)*operator_state;
    auto &data = (PragmaExpressionsProfilingOutputData &)*bind_data_p;
    if (!state.initialized) {
        // create a ChunkCollection
        auto collection = make_unique<ChunkCollection>();

        DataChunk chunk;
        chunk.Initialize(data.types);

        int operator_counter = 1;
        if (!context.query_profiler_history.GetPrevProfilers().empty()) {
            for (auto op : context.query_profiler_history.GetPrevProfilers().back().second.GetTreeMap()) {
                int expression_counter = 1;
                if (op.second->info.has_executor) {
                    for (auto &info : op.second->info.executors_info->roots) {
                        ExpressionSetValue(chunk, chunk.size(), operator_counter, expression_counter++, info->name, double(info->time) / op.second->info.executors_info->sample_tuples_count);
                        chunk.SetCardinality(chunk.size() + 1);
                        if (chunk.size() == STANDARD_VECTOR_SIZE) {
                            collection->Append(chunk);
                            chunk.Reset();
                        }                    }
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

void PragmaExpressionsProfilingOutput::RegisterFunction(BuiltinFunctions &set) {
    set.AddFunction(TableFunction("pragma_expressions_profiling_output", {}, PragmaExpressionsProfilingOutputFunction,
                                  PragmaExpressionsProfilingOutputBind, PragmaExpressionsProfilingOutputInit));
}

} // namespace duckdb
