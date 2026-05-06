#ifndef DUCKDB_STATIC_BUILD
#define DUCKDB_STATIC_BUILD
#endif

#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/common/extension_type_info.hpp"

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
	// set the schema for the extension
	loader.SetExtensionSchema("explicit_schema");
	LoadableExtensionExplicitSchemaFunInit(loader);
}
}
