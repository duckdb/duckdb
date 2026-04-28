#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

using namespace duckdb; // NOLINT

static void DemoSecondGreetFunc(DataChunk &args, ExpressionState &state, Vector &result) {
	result.Reference(Value("Hello from demo_second!"), count_t(args.size()));
}

extern "C" {
DUCKDB_CPP_EXTENSION_ENTRY(loadable_extension_demo_second, loader) {
	loader.RegisterFunction(ScalarFunction("demo_second_greet", {}, LogicalType::VARCHAR, DemoSecondGreetFunc));
}
}
