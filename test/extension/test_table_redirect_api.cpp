#include "catch.hpp"
#include "duckdb/main/appender.hpp"
#include "test_helpers.hpp"

using namespace duckdb;

// The by-name entry points of the C++ API describe a redirected table (Catalog::TryRedirectEntry) at the location
// of the name they were given and with the columns of the table that serves it, so a statement built from that
// description takes the same redirects: src -> mid -> dst reads and appends to dst through "src".
TEST_CASE("Relation API and Appender follow a chain of table redirects", "[extension][redirect]") {
	DBConfig config;
	config.SetOptionByName("allow_unsigned_extensions", true);
	DuckDB db(nullptr, &config);
	Connection con(db);
	REQUIRE_NO_FAIL(
	    con.Query("LOAD '" DUCKDB_BUILD_DIRECTORY "/test/extension/loadable_extension_demo.duckdb_extension'"));
	REQUIRE_NO_FAIL(con.Query("ATTACH ':memory:' AS dst"));
	REQUIRE_NO_FAIL(con.Query("ATTACH ':memory:' AS mid (TYPE table_redirect_test, TARGET 'dst')"));
	REQUIRE_NO_FAIL(con.Query("ATTACH ':memory:' AS src (TYPE table_redirect_test, TARGET 'mid')"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE dst.redirect_t AS SELECT 3 AS i"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE mid.redirect_t AS SELECT 2 AS i, 'extra' AS s"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE src.redirect_t(j INTEGER)"));

	auto info = con.TableInfo("src", "main", "redirect_t");
	REQUIRE(info);
	REQUIRE(info->qualified_name.Catalog() == "src");
	REQUIRE(info->qualified_name.Name() == "redirect_t");
	REQUIRE(info->columns.size() == 1);
	REQUIRE(info->columns[0].Name() == "i");
	REQUIRE(!info->readonly);

	auto result = con.Table("src", "main", "redirect_t")->Execute();
	REQUIRE(CHECK_COLUMN(result, 0, {3}));

	{
		Appender appender(con, "src", "main", "redirect_t");
		appender.AppendRow(5);
		appender.Close();
	}
	// the row went to the end of the chain; mid's own content is only reachable by size
	result = con.Query("SELECT database_name, estimated_size FROM duckdb_tables() WHERE table_name = 'redirect_t' "
	                   "ORDER BY database_name");
	REQUIRE(CHECK_COLUMN(result, 0, {"dst", "mid", "src"}));
	REQUIRE(CHECK_COLUMN(result, 1, {2, 1, 0}));
	result = con.Table("src", "main", "redirect_t")->Order("i")->Execute();
	REQUIRE(CHECK_COLUMN(result, 0, {3, 5}));

	// a READ_ONLY redirecting database refuses the Appender like it refuses INSERT
	auto ro_path = TestCreatePath("table_redirect_ro.db");
	REQUIRE_NO_FAIL(con.Query("ATTACH '" + ro_path + "' AS setup"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE setup.redirect_t(j INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("DETACH setup"));
	REQUIRE_NO_FAIL(con.Query("ATTACH '" + ro_path + "' AS ro (TYPE table_redirect_test, TARGET 'mid', READ_ONLY)"));
	info = con.TableInfo("ro", "main", "redirect_t");
	REQUIRE(info);
	REQUIRE(info->readonly);
	REQUIRE_THROWS(Appender(con, "ro", "main", "redirect_t"));
	result = con.Table("src", "main", "redirect_t")->Order("i")->Execute();
	REQUIRE(CHECK_COLUMN(result, 0, {3, 5}));
}
