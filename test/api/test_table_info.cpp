#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

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
	REQUIRE(info->table == "test");
	REQUIRE(info->columns.size() == 1);
	REQUIRE(info->columns[0].name == "i");

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
