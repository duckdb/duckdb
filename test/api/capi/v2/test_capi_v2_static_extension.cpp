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

	// The entrypoint registered a scalar function through the C++ wrapper. It reads all three data slots:
	// 5 * factor(3) + 2 + offset(3 + 7) = 27.
	auto madd = con.Query("SELECT cpp_demo_madd(5, 2)");
	REQUIRE(CHECK_COLUMN(madd, 0, {27}));

	// And it runs vectorized over a table, with the bind/init data recomputed per query.
	auto vectorized = con.Query("SELECT sum(cpp_demo_madd(r::INTEGER, 1)) FROM range(100) t(r)");
	REQUIRE(CHECK_COLUMN(vectorized, 0, {3 * 4950 + 100 * 11}));

	// Loading it a second time is a no-op rather than an error
	db.LoadStaticCAPIExtensionV2("cpp_api_static_demo", cpp_api_static_demo_init_c_api_v2);
}
