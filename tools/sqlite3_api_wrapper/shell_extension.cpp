#include "shell_extension.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/main/config.hpp"
#include <stdio.h>
#include <stdlib.h>

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
	auto &config = DBConfig::GetConfig(context);
	if (!config.options.enable_external_access) {
		throw PermissionException("getenv is disabled through configuration");
	}
	return nullptr;
}

void ShellExtension::Load(ExtensionLoader &loader) {
	loader.SetDescription("Adds CLI-specific support and functionalities");
	loader.RegisterFunction(
	    ScalarFunction("getenv", {LogicalType::VARCHAR}, LogicalType::VARCHAR, GetEnvFunction, GetEnvBind));
}

std::string ShellExtension::Name() {
	return "shell";
}

std::string ShellExtension::Version() const {
	return DefaultVersion();
}

} // namespace duckdb
