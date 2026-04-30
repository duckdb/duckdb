#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

using namespace duckdb;

static void LoadableExtensionDemoFun(DataChunk &args, ExpressionState &state, Vector &result) {
	result.Reference(Value("Hello from the loadable_extension_demo_double binary!"), count_t(args.size()));
}

static void LoadableExtensionFunInit(ExtensionLoader &loader, string &function_name) {
	loader.RegisterFunction(ScalarFunction(function_name, {}, LogicalType::VARCHAR, LoadableExtensionDemoFun));
}

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(loadable_extension_demo_double, loader) {
	auto &db = loader.GetDatabaseInstance();
	auto suffix = db.GetExtensionManager().GetExtensions().size();
	auto function_name = "loadable_extension_demo_double_" + to_string(suffix);
	LoadableExtensionFunInit(loader, function_name);
}
}
