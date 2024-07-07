#include "shell_extension.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/main/config.hpp"
#include <stdio.h>
#include <stdlib.h>

namespace duckdb {

void GetEnvFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	UnaryExecutor::Execute<string_t, string_t>(args.data[0], result, args.size(), [&](string_t input) {
		string env_name = input.GetString();
		auto env_value = getenv(env_name.c_str());
		if (!env_value) {
			return StringVector::AddString(result, string());
		}
		return StringVector::AddString(result, env_value);
	});
}

unique_ptr<FunctionData> GetEnvBind(ClientContext &context, ScalarFunction &bound_function,
                                    vector<unique_ptr<Expression>> &arguments) {
	auto &config = DBConfig::GetConfig(context);
	if (!config.options.enable_external_access) {
		throw PermissionException("getenv is disabled through configuration");
	}
	return nullptr;
}

void ShellExtension::Load(DuckDB &db) {
	ExtensionUtil::RegisterExtension(*db.instance, "shell", {"Adds CLI-specific support and functionalities"});

	ExtensionUtil::RegisterFunction(*db.instance, ScalarFunction("getenv", {LogicalType::VARCHAR}, LogicalType::VARCHAR,
	                                                             GetEnvFunction, GetEnvBind));
}

std::string ShellExtension::Name() {
	return "shell";
}

} // namespace duckdb
