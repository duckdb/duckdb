// A minimal extension built against the V2 C API through the stable C++ API. It exists to exercise the V2 loader
// end-to-end: the entrypoint, the vtable handed out by get_api, and the context DuckDB lends for the duration of the
// load. There is no registration API on the V2 surface yet, so the body only reads through the context and logs.

#include "duckdb_cpp_extension.hpp"

static constexpr const char *LOG_TYPE = "CppApiDemo";

using namespace duckdb::cxx;

DUCKDB_CPP_EXTENSION_ENTRYPOINT(Extension &extension, Context &context) {
	(void)extension;

	// Binding a type proves the context is live and has a transaction: this reaches into the catalog.
	const auto type = context.ParseType("STRUCT(a INTEGER, b VARCHAR)");

	context.Log(LogLevel::LOG_INFO, "cpp_api_demo loaded, parsed " + type.ToText(), LOG_TYPE);
}
