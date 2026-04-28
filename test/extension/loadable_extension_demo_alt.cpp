#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

using namespace duckdb;

static void LoadableExtensionDemoFun(DataChunk &args, ExpressionState &state, Vector &result) {
	result.Reference(Value("Hello from the second loadable_extension_demo binary!"), count_t(args.size()));
}

static void LoadableExtensionFunInit(ExtensionLoader &loader) {
	loader.RegisterFunction(ScalarFunction("loadable_extension_second_demo", {}, LogicalType::VARCHAR, LoadableExtensionDemoFun));
}

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(loadable_extension_demo, loader) {
	LoadableExtensionFunInit(loader);
}
}
