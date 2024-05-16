#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "test_helpers.hpp"
#include "duckdb/main/appender.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test write-write conflict on WAL replay", "[storage]") {
	duckdb::unique_ptr<QueryResult> result;
	auto storage_database = TestCreatePath("wal_replay_ww_conflict");

	DeleteDatabase(storage_database);

	DBConfig config;
	DuckDB db("", &config);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("attach '" + storage_database + "' AS test"));
	REQUIRE_NO_FAIL(con.Query("create table test.tbl as select 1 as a;"));
	REQUIRE_NO_FAIL(con.Query("detach test"));
	REQUIRE_NO_FAIL(con.Query("attach '" + storage_database + "' AS test"));
	REQUIRE_NO_FAIL(con.Query("select * from test.tbl"));

	DBConfig config2;
	DuckDB db2("", &config2);
	Connection con2(db2);
	REQUIRE_NO_FAIL(con2.Query("attach '" + storage_database + "' AS test"));
	REQUIRE_NO_FAIL(con2.Query("select * from test.tbl"));
}
