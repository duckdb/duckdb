#include "catch.hpp"
#include "duckdb.hpp"
#include "test_helpers.hpp"

#include <chrono>
#include <thread>

using namespace duckdb;

TEST_CASE("An update of a row conflicts with an update in flight while ALTER TABLE ran", "[persistence]") {
	auto path = TestCreatePath("alter_update_race.db");
	DeleteDatabase(path);
	{
		DuckDB db(path);
		Connection con(db);
		// writing 'y' evaluates a slow CHECK constraint, which keeps the UPDATE in flight while ALTER runs
		REQUIRE_NO_FAIL(
		    con.Query("CREATE TABLE t (id INTEGER, a VARCHAR CHECK (a <> 'y' OR md5(repeat(a, 50000000)) <> ''))"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t SELECT i, 'x' FROM range(10) r(i)"));
	}
	// reopened, the row group has no version info
	DuckDB db(path);
	Connection con1(db), con2(db);
	// load column a, so that ALTER shares it with the new table
	REQUIRE_NO_FAIL(con2.Query("SELECT sum(length(a) + id) FROM t"));
	REQUIRE_NO_FAIL(con1.Query("BEGIN"));
	REQUIRE_NO_FAIL(con2.Query("BEGIN"));

	bool con1_updated = false;
	std::thread updater([&]() { con1_updated = !con1.Query("UPDATE t SET a = 'y' WHERE id = 5")->HasError(); });
	std::this_thread::sleep_for(std::chrono::milliseconds(20));
	bool con2_updated = !con2.Query("ALTER TABLE t ADD COLUMN c INTEGER")->HasError();
	updater.join();
	con2_updated = con2_updated && !con2.Query("UPDATE t SET a = 'z' WHERE id = 5")->HasError();
	bool con2_committed = !con2.Query("COMMIT")->HasError() && con2_updated;
	bool con1_committed = !con1.Query("COMMIT")->HasError() && con1_updated;

	// at most one of the updates commits, and the row holds its value
	REQUIRE(!(con1_committed && con2_committed));
	auto result = con1.Query("SELECT a FROM t WHERE id = 5");
	REQUIRE(CHECK_COLUMN(result, 0, {con1_committed ? "y" : (con2_committed ? "z" : "x")}));
}
