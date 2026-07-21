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
