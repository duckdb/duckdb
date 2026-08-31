#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb/main/database.hpp"

//! Defined by test/api/capi/v2/static_extension/cpp_api_static_demo.cpp, built as a static V2 C API extension.
extern "C" void cpp_api_static_demo_init_c_api_v2(struct duckdb_v2_extension_input *input);

using namespace duckdb;

TEST_CASE("Test loading a statically linked V2 C API extension", "[capi_v2]") {
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CALL enable_logging()"));
	REQUIRE_NO_FAIL(con.Query("SET logging_level='info'"));

	db.LoadStaticCAPIExtensionV2("cpp_api_static_demo", cpp_api_static_demo_init_c_api_v2);

	auto loaded = con.Query("SELECT count(*) FROM duckdb_extensions() WHERE extension_name = 'cpp_api_static_demo' AND "
	                        "loaded");
	REQUIRE(CHECK_COLUMN(loaded, 0, {1}));

	// The entrypoint bound a type and logged through the context DuckDB opened for it, which is what proves the static
	// path hands out a usable context even though no client connection was involved in the load.
	auto logs = con.Query("SELECT message FROM duckdb_logs WHERE type = 'CppApiStaticDemo'");
	REQUIRE(CHECK_COLUMN(logs, 0, {"cpp_api_static_demo loaded, parsed DECIMAL(18,3)"}));

	// Loading it a second time is a no-op rather than an error
	db.LoadStaticCAPIExtensionV2("cpp_api_static_demo", cpp_api_static_demo_init_c_api_v2);
}
