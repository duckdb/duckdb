// A V2 C API extension linked straight into the test binary. Statically linked extensions bind DuckDB's symbols at
// link time, so there is no vtable and get_api is never called -- but the entrypoint still receives a context, and it
// is DuckDB that has to supply one, since a static extension loads before any client connection exists.

#include "duckdb_cpp_extension.hpp"

DUCKDB_CPP_EXTENSION_ENTRYPOINT(duckdb::cxx::Extension &extension, duckdb::cxx::Context &context) {
	(void)extension;

	const auto type = context.ParseType("DECIMAL(18, 3)");
	context.Log(duckdb::cxx::LogLevel::LOG_INFO, "cpp_api_static_demo loaded, parsed " + type.ToText(),
	            "CppApiStaticDemo");
}
