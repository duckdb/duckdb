#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/storage_info.hpp"
#include "test_helpers.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test in-memory database scanning from tables", "[memoryleak]") {
	if (!TestMemoryLeaks()) {
		return;
	}
	DuckDB db;
	Connection con(db);
	REQUIRE_NO_FAIL(
	    con.Query("create table t1 as select i, concat('thisisalongstring', i) s from range(1000000) t(i);"));
	while (true) {
		REQUIRE_NO_FAIL(con.Query("SELECT * FROM t1"));
	}
}

TEST_CASE("Rollback create table", "[memoryleak]") {
	if (!TestMemoryLeaks()) {
		return;
	}
	DBConfig config;
	config.options.load_extensions = false;
	DuckDB db(":memory:", &config);
	Connection con(db);
	while (true) {
		REQUIRE_NO_FAIL(con.Query("BEGIN"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t2(i INT);"));
		REQUIRE_NO_FAIL(con.Query("ROLLBACK"));
	}
}

TEST_CASE("DB temporary table insertion", "[memoryleak]") {
	if (!TestMemoryLeaks()) {
		return;
	}
	auto db_path = TestCreatePath("memory_leak_temp_table.db");
	DeleteDatabase(db_path);

	DuckDB db(db_path);
	{
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("SET threads=8;"));
		REQUIRE_NO_FAIL(con.Query("SET preserve_insertion_order=false;"));
		REQUIRE_NO_FAIL(con.Query("SET force_compression='uncompressed';"));
		REQUIRE_NO_FAIL(con.Query("create table t1 as select i from range(1000000) t(i);"));
	}
	Connection con(db);
	while (true) {
		REQUIRE_NO_FAIL(con.Query("BEGIN"));
		REQUIRE_NO_FAIL(con.Query("CREATE OR REPLACE TEMPORARY TABLE t2(i int)"));
		REQUIRE_NO_FAIL(con.Query("insert into t2 SELECT * FROM t1;"));
		REQUIRE_NO_FAIL(con.Query("ROLLBACk"));
	}
}

// FIXME: broken right now - we need to flush/merge deletes to fix this
// TEST_CASE("Insert and delete data repeatedly", "[memoryleak]") {
//	if (!TestMemoryLeaks()) {
//		return;
//	}
//	DBConfig config;
//	config.options.load_extensions = false;
//	DuckDB db(":memory:", &config);
//	Connection con(db);
//	REQUIRE_NO_FAIL(con.Query("CREATE TABLE t1(i INT);"));
//	while (true) {
//		REQUIRE_NO_FAIL(con.Query("INSERT INTO t1 SELECT * FROM range(100000)"));
//		REQUIRE_NO_FAIL(con.Query("DELETE FROM t1"));
//	}
//}
