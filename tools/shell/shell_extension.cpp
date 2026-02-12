#include "shell_extension.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/settings.hpp"
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
	UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](string_t input) {
		string env_name = input.GetString();
		auto env_value = getenv(env_name.c_str());
		if (!env_value) {
			return StringVector::AddString(result, string());
		}
		return StringVector::AddString(result, env_value);
	});
}

static unique_ptr<FunctionData> GetEnvBind(ClientContext &context, ScalarFunction &bound_function,
                                           vector<unique_ptr<Expression>> &arguments) {
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
	if (!state.last_result) {
		throw BinderException("Failed to query last result \"_\": no result available");
	}
	return make_uniq<ColumnDataRef>(state.last_result->Collection(), state.last_result->names);
}

void ShellExtension::Load(ExtensionLoader &loader) {
	loader.SetDescription("Adds CLI-specific support and functionalities");
	loader.RegisterFunction(
	    ScalarFunction("getenv", {LogicalType::VARCHAR}, LogicalType::VARCHAR, GetEnvFunction, GetEnvBind));

	auto &config = duckdb::DBConfig::GetConfig(loader.GetDatabaseInstance());
	config.SetOptionByName("duckdb_api", DUCKDB_API_CLI);
	config.replacement_scans.push_back(duckdb::ReplacementScan(duckdb::ShellScanLastResult));
}

std::string ShellExtension::Name() {
	return "shell";
}

std::string ShellExtension::Version() const {
	return DefaultVersion();
}

} // namespace duckdb
