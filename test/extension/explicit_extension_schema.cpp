#include "duckdb.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/execution/expression_executor_state.hpp"

#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/planner/extension_callback.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/common/exception/conversion_exception.hpp"
#include "duckdb/common/extension_type_info.hpp"
#include "duckdb/common/vector/struct_vector.hpp"

using namespace duckdb;

static void LoadableExtensionExplicitSchema(DataChunk &args, ExpressionState &state, Vector &result) {
	result.Reference(Value("Hello from the explicit_extension_schema schema!"), count_t(args.size()));
}

static void LoadableExtensionExplicitSchemaFunInit(ExtensionLoader &loader) {
	loader.RegisterFunction(
	    ScalarFunction("dedicated_schema_function", {}, LogicalType::VARCHAR, LoadableExtensionExplicitSchema));
}

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(explicit_extension_schema, loader) {
	DBConfig::GetConfig(loader.GetDatabaseInstance()).GetCallbackManager();
	// set the schema for the extension
	loader.UseDedicatedSchemaForExtension("explicit_schema");
	LoadableExtensionExplicitSchemaFunInit(loader);
}
}
