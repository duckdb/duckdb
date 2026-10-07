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

TEST_CASE("Test CONNECT to a passthrough catalog registered by a V2 C API extension", "[capi_v2]") {
	DuckDB db(nullptr);
	Connection con(db);
	db.LoadStaticCAPIExtensionV2("cpp_api_static_demo", cpp_api_static_demo_init_c_api_v2);

	// The type name is the attach prefix; the options travel to the query function as named arguments.
	REQUIRE_NO_FAIL(con.Query("ATTACH 'cpp_api_demo:remote-path' AS remote (MODE 'fast', LEVEL 3)"));
	auto type = con.Query("SELECT type FROM duckdb_databases() WHERE database_name = 'remote'");
	REQUIRE(CHECK_COLUMN(type, 0, {"cpp_api_demo"}));

	// A passthrough catalog has no tables of its own
	REQUIRE_FAIL(con.Query("SELECT * FROM remote.main.tbl"));
	REQUIRE_FAIL(con.Query("CREATE TABLE remote.tbl (i INTEGER)"));

	// While connected, statements are forwarded verbatim and the query function produces their result
	REQUIRE_NO_FAIL(con.Query("CONNECT remote"));
	auto forwarded = con.Query("SELECT 42 AS x");
	INFO(forwarded->ToString());
	REQUIRE(CHECK_COLUMN(forwarded, 0, {"remote-path"}));
	REQUIRE(CHECK_COLUMN(forwarded, 1, {"SELECT 42 AS x"}));
	REQUIRE(CHECK_COLUMN(forwarded, 2, {"level=3;mode=fast"}));

	// Text that DuckDB itself would not parse reaches the remote
	auto unparsed = con.Query("THIS IS NOT SQL");
	REQUIRE(CHECK_COLUMN(unparsed, 1, {"THIS IS NOT SQL"}));

	REQUIRE_NO_FAIL(con.Query("DISCONNECT"));
	auto local = con.Query("SELECT 1 + 1");
	REQUIRE(CHECK_COLUMN(local, 0, {2}));

	// The connection-string form attaches and connects in one step, with its own options
	REQUIRE_NO_FAIL(con.Query("CONNECT 'cpp_api_demo:other' (MODE 'slow')"));
	auto other = con.Query("hello");
	REQUIRE(CHECK_COLUMN(other, 0, {"other"}));
	REQUIRE(CHECK_COLUMN(other, 1, {"hello"}));
	REQUIRE(CHECK_COLUMN(other, 2, {"mode=slow"}));
	REQUIRE_NO_FAIL(con.Query("DISCONNECT"));

	// The hidden attachment of CONNECT '<uri>' is gone again, the explicit one stays
	auto databases = con.Query("SELECT count(*) FROM duckdb_databases() WHERE type = 'cpp_api_demo'");
	REQUIRE(CHECK_COLUMN(databases, 0, {1}));
	REQUIRE_NO_FAIL(con.Query("DETACH remote"));
}
