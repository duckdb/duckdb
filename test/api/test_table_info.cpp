#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;

TEST_CASE("Test table info api", "[api]") {
	DuckDB db(nullptr);
	Connection con(db), con2(db);

	//! table is not found!
	auto info = con.TableInfo("test");
	REQUIRE(info.get() == nullptr);

	// after creating, the table can be found
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test(i INTEGER)"));
	info = con.TableInfo("test");
	REQUIRE(info.get() != nullptr);
	REQUIRE(info->qualified_name.Name() == "test");
	REQUIRE(info->columns.size() == 1);
	REQUIRE(info->columns[0].Name() == "i");

	// table info is transaction sensitive
	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	// dropping the table in a transaction will result in the table being gone
	REQUIRE_NO_FAIL(con.Query("DROP TABLE test"));
	info = con.TableInfo("test");
	REQUIRE(info.get() == nullptr);

	// but not in a separate connection!
	info = con2.TableInfo("test");
	REQUIRE(info.get() != nullptr);

	// rolling back brings back the table info again
	REQUIRE_NO_FAIL(con.Query("ROLLBACK"));
	info = con.TableInfo("test");
	REQUIRE(info.get() != nullptr);
}

TEST_CASE("Test table info reports the resolved table location", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);

	// An unqualified lookup fills in the resolved catalog and schema.
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t(i INTEGER)"));
	auto info = con.TableInfo("t");
	REQUIRE(info);
	REQUIRE(info->qualified_name.Catalog() == "memory");
	REQUIRE(info->qualified_name.Schema() == "main");
	REQUIRE(info->qualified_name.Name() == "t");

	// The name is reported with its DDL time casing, not the lookup casing.
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE \"FooBar\"(i INTEGER)"));
	info = con.TableInfo("foobar");
	REQUIRE(info);
	REQUIRE(info->qualified_name.Name() == "FooBar");
}

TEST_CASE("Test table info on a view keeps the open transaction", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE u(k INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW vw AS SELECT 1 AS k"));

	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO u VALUES (1)"));
	REQUIRE(con.TableInfo("vw") == nullptr);
	REQUIRE_NO_FAIL(con.Query("INSERT INTO u VALUES (2)"));
	REQUIRE_NO_FAIL(con.Query("COMMIT"));

	auto result = con.Query("SELECT k FROM u ORDER BY k");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
}

TEST_CASE("Test table info on a temporary view that hides a table", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE shadowed AS SELECT 'table' AS k"));
	REQUIRE_NO_FAIL(con.Query("CREATE TEMPORARY VIEW shadowed AS SELECT 'temporary view' AS k"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE u(k INTEGER)"));

	auto result = con.Query("SELECT k FROM shadowed");
	REQUIRE(CHECK_COLUMN(result, 0, {"temporary view"}));

	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO u VALUES (1)"));
	REQUIRE(con.TableInfo("shadowed") == nullptr);
	auto info = con.TableInfo("memory", "main", "shadowed");
	REQUIRE(info);
	REQUIRE(info->qualified_name.Catalog() == "memory");
	REQUIRE(info->qualified_name.Name() == "shadowed");
	REQUIRE(info->columns.size() == 1);
	REQUIRE_NO_FAIL(con.Query("INSERT INTO u VALUES (2)"));
	REQUIRE_NO_FAIL(con.Query("COMMIT"));

	result = con.Query("SELECT k FROM u ORDER BY k");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
}
