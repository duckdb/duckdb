#include "shell_extension.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/planner/planner_extension.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "shell_state.hpp"
#include "duckdb/parser/tableref/column_data_ref.hpp"
#include "shell_renderer.hpp"
#include <stdio.h>
#include <stdlib.h>

#ifndef DUCKDB_API_CLI
#define DUCKDB_API_CLI "cli"
#endif

namespace duckdb {

static void GetEnvFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &heap = StringVector::GetStringHeap(result);
	UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](string_t input) {
		string env_name = input.GetString();
		auto env_value = getenv(env_name.c_str());
		if (!env_value) {
			return heap.AddString(string());
		}
		return heap.AddString(env_value);
	});
}

static unique_ptr<FunctionData> GetEnvBind(BindScalarFunctionInput &input) {
	auto &context = input.GetClientContext();
	if (!Settings::Get<EnableExternalAccessSetting>(context)) {
		throw PermissionException("getenv is disabled through configuration");
	}
	return nullptr;
}

unique_ptr<TableRef> ShellScanLastResult(ClientContext &context, ReplacementScanInput &input,
                                         optional_ptr<ReplacementScanData> data) {
	auto &table_name = input.table_name;
	if (table_name != "_") {
		return nullptr;
	}
	auto &state = duckdb_shell::ShellState::Get();
	state.last_result_referenced = true;
	if (!state.last_result) {
		throw BinderException("Failed to query last result \"_\": no result available");
	}
	return make_uniq<ColumnDataRef>(state.last_result->Collection(), state.last_result->names);
}

// Runs after the binder has finished. Releases the previous last_result early if it's
// safe — i.e. the new query doesn't statically reference `_` (the replacement scan
// would have fired)
void ShellPostBind(PlannerExtensionInput &input, BoundStatement &statement) {
	auto &state = duckdb_shell::ShellState::Get();
	if (state.last_result_referenced) {
		return;
	}
	state.last_result.reset();
}

void ShellExtension::Load(ExtensionLoader &loader) {
	loader.SetDescription("Adds CLI-specific support and functionalities");
	loader.RegisterFunction(
	    ScalarFunction("getenv", {LogicalType::VARCHAR}, LogicalType::VARCHAR, GetEnvFunction, GetEnvBind));

	auto &config = duckdb::DBConfig::GetConfig(loader.GetDatabaseInstance());
	config.SetOptionByName("duckdb_api", DUCKDB_API_CLI);
	config.replacement_scans.push_back(duckdb::ReplacementScan(duckdb::ShellScanLastResult));

	PlannerExtension planner_ext;
	planner_ext.post_bind_function = duckdb::ShellPostBind;
	PlannerExtension::Register(config, planner_ext);
}

std::string ShellExtension::Name() {
	return "shell";
}

std::string ShellExtension::Version() const {
	return DefaultVersion();
}

} // namespace duckdb
