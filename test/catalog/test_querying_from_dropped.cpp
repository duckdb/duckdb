#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/main/extension_util.hpp"

#include "duckdb.hpp"

using namespace duckdb;

TEST_CASE("Querying from dropped catalog", "[catalog]") {
	DBConfig config;
	config.options.allow_unsigned_extensions = true;
	DuckDB db(nullptr, &config);

	Connection con1(db);
	Connection con2(db);

	REQUIRE_NO_FAIL(con1.Query("attach ':memory:' as db1"));
	REQUIRE_NO_FAIL(con1.Query("attach ':memory:' as db2"));
	REQUIRE_NO_FAIL(con1.Query("CREATE TABLE db2.tbl AS SELECT i FROM range(100) r(i);"));

	REQUIRE_NO_FAIL(con1.Query("use db1;"));

	REQUIRE_NO_FAIL(con2.Query("use db2;"));

	REQUIRE_NO_FAIL(con2.Query("detach db1"));

	REQUIRE_FAIL(con1.Query("show tables"));

	// Querying a table in different db should work
	REQUIRE_NO_FAIL(con1.Query("from db2.tbl"));
	REQUIRE(con1.Query("from db2.tbl")->RowCount() == 100);
	REQUIRE(con1.Query("from db2.main.tbl")->RowCount() == 100);

	REQUIRE_THAT(con1.Query("from db2.main.aaaaaa")->GetError(), Catch::Matchers::Contains("Table with name aaaaaa does not exist!"));

	REQUIRE_NO_FAIL(con1.Query("use db2"));
}