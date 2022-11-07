#include "json_scan.hpp"

#include "duckdb/main/database.hpp"

namespace duckdb {

unique_ptr<FunctionData> JSONScanData::Bind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names) {
	auto &config = DBConfig::GetConfig(context);
	if (!config.options.enable_external_access) {
		throw PermissionException("Scanning JSON files is disabled through configuration");
	}

	auto result = make_unique<JSONScanData>();

	return result;
}

unique_ptr<GlobalTableFunctionState> JSONScanGlobalState::Init(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = (JSONScanData &)*input.bind_data;
	auto result = make_unique<JSONScanGlobalState>();
	// TODO init the buffered reader with the info in bind_data
	return move(result);
}

unique_ptr<LocalTableFunctionState> JSONScanLocalState::Init(ExecutionContext &context, TableFunctionInitInput &input,
                                                             GlobalTableFunctionState *global_state) {
	return make_unique<JSONScanLocalState>();
}

} // namespace duckdb
