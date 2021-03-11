#include "duckdb/function/table/sqlite_functions.hpp"

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"
#include "duckdb/planner/constraints/bound_not_null_constraint.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/cpu_feature.hpp"
#include "duckdb/common/limits.hpp"

namespace duckdb {

struct PragmaCpuFeaturesOutputData : public FunctionOperatorData {
    explicit PragmaCpuFeaturesOutputData(idx_t rows) : rows(rows) {
    }
    idx_t rows;
};

static unique_ptr<FunctionData> PragmaCpuFeaturesOutputBind(ClientContext &context, vector<Value> &inputs,
                                                              unordered_map<string, Value> &named_parameters,
                                                              vector<LogicalType> &return_types,
                                                              vector<string> &names) {
    names.emplace_back("CPU_FEATURE");
    return_types.push_back(LogicalType::VARCHAR);

    return make_unique<TableFunctionData>();
}

unique_ptr<FunctionOperatorData> PragmaCpuFeaturesOutputInit(ClientContext &context, const FunctionData *bind_data,
                                                               vector<column_t> &column_ids,
                                                               TableFilterCollection *filters) {
    return make_unique<PragmaCpuFeaturesOutputData>(1024);
}

static void PragmaCpuFeaturesOutputFunction(ClientContext &context, const FunctionData *bind_data_p,
                                              FunctionOperatorData *operator_state, DataChunk &output) {
    auto &state = (PragmaCpuFeaturesOutputData &)*operator_state;
    if (state.rows > 0) {
        int index = 0;
		for(auto feature : context.cpu_info.GetAvailFeatures()) {
			output.SetValue(0, index++, CPUFeatureToString(feature));
		}
        state.rows = 0;
        output.SetCardinality(index);
    } else {
        output.SetCardinality(0);
    }
}

void PragmaCpuFeaturesOutput::RegisterFunction(BuiltinFunctions &set) {
    set.AddFunction(TableFunction("pragma_cpu_features_output", {}, PragmaCpuFeaturesOutputFunction,
                                  PragmaCpuFeaturesOutputBind, PragmaCpuFeaturesOutputInit));
}

} // namespace duckdb
