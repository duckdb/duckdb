// The failure counterpart of cpp_api_demo: an extension whose entrypoint throws. The V2 entrypoint returns void, so
// this checks that the exception reaches DuckDB through the error slot and aborts the load.

#include "duckdb_cpp_extension.hpp"

DUCKDB_CPP_EXTENSION_ENTRYPOINT(duckdb::cxx::Extension &extension, duckdb::cxx::Context &context) {
	(void)extension;
	(void)context;

	throw duckdb::cxx::InvalidInputException("cpp_api_demo_fail always fails to load");
}
