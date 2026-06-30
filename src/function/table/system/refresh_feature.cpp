#include "duckdb/function/table/system_functions.hpp"

#include "duckdb/common/feature_refresh.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

struct RefreshFeatureBindData : public FunctionData {
	string feature_name;

	unique_ptr<FunctionData> Copy() const override {
		auto result = make_uniq<RefreshFeatureBindData>();
		result->feature_name = feature_name;
		return std::move(result);
	}

	bool Equals(const FunctionData &other) const override {
		return feature_name == other.Cast<RefreshFeatureBindData>().feature_name;
	}
};

struct RefreshFeatureState : public GlobalTableFunctionState {
	bool done = false;
};

static unique_ptr<FunctionData> RefreshFeatureBind(ClientContext &context, TableFunctionBindInput &input,
                                                   vector<LogicalType> &return_types, vector<string> &names) {
	auto result = make_uniq<RefreshFeatureBindData>();
	result->feature_name = input.inputs[0].GetValue<string>();

	names.emplace_back("rows_affected");
	return_types.emplace_back(LogicalType::BIGINT);

	return std::move(result);
}

static unique_ptr<GlobalTableFunctionState> RefreshFeatureInit(ClientContext &context, TableFunctionInitInput &input) {
	return make_uniq<RefreshFeatureState>();
}

static void RefreshFeatureFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &state = data_p.global_state->Cast<RefreshFeatureState>();
	if (state.done) {
		return;
	}
	state.done = true;

	auto &bind_data = data_p.bind_data->Cast<RefreshFeatureBindData>();
	auto refresh_result = RefreshFeature(context, bind_data.feature_name);

	output.SetCardinality(1);
	output.data[0].Append(Value::BIGINT(NumericCast<int64_t>(refresh_result.rows_affected)));
}

void RefreshFeatureFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(TableFunction("refresh_feature", {LogicalType::VARCHAR}, RefreshFeatureFunction, RefreshFeatureBind,
	                              RefreshFeatureInit));
}

} // namespace duckdb
