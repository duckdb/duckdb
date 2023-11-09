#include "shell_extension.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
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

void ShellExtension::Load(DuckDB &db) {
	ExtensionUtil::RegisterFunction(
	    *db.instance, ScalarFunction("getenv", {LogicalType::VARCHAR}, LogicalType::VARCHAR, GetEnvFunction));
}

std::string ShellExtension::Name() {
	return "shell";
}

} // namespace duckdb
