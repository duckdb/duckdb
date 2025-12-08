#include "duckdb/function/table/system_functions.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

static void EnableProfiling(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
}

static unique_ptr<FunctionData> BindEnableProfiling(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names) {
	return nullptr;
}

static void DisableProfiling(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	context.DisableProfiling();
}

static unique_ptr<FunctionData> BindDisableProfiling(ClientContext &context, TableFunctionBindInput &input,
                                                     vector<LogicalType> &return_types, vector<string> &names) {
	return_types.emplace_back(LogicalType::BOOLEAN);
	names.emplace_back("Success");

	return nullptr;
}

void EnableProfilingFun::RegisterFunction(BuiltinFunctions &set) {
	auto enable_fun = TableFunction("enable_profiling", {}, EnableProfiling, BindEnableProfiling, nullptr, nullptr);

	enable_fun.named_parameters.emplace("format", LogicalType::VARCHAR);
	enable_fun.named_parameters.emplace("coverage", LogicalType::VARCHAR);
	enable_fun.named_parameters.emplace("output", LogicalType::VARCHAR);
	enable_fun.named_parameters.emplace("mode", LogicalType::VARCHAR);
	enable_fun.named_parameters.emplace("enabled_metrics", LogicalType::ANY);

	enable_fun.varargs = LogicalType::ANY;
	set.AddFunction(enable_fun);

	auto disable_fun = TableFunction("disable_profiling", {}, DisableProfiling, BindDisableProfiling, nullptr, nullptr);
	set.AddFunction(disable_fun);
}

} // namespace duckdb
