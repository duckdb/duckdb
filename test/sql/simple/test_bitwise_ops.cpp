#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test scalar bitwise ops", "[bitop]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// left shift
	result = con.Query("SELECT 1 << 2, NULL << 2, 2 << NULL");
	REQUIRE(CHECK_COLUMN(result, 0, {4}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value()}));

	// right shift
	result = con.Query("SELECT 16 >> 2, 1 >> 2, NULL >> 2, 2 >> NULL");
	REQUIRE(CHECK_COLUMN(result, 0, {4}));
	REQUIRE(CHECK_COLUMN(result, 1, {0}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 3, {Value()}));

	// bitwise and
	result = con.Query("SELECT 1 & 1, 1 & 0, 0 & 0, NULL & 1, 1 & NULL");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {0}));
	REQUIRE(CHECK_COLUMN(result, 2, {0}));
	REQUIRE(CHECK_COLUMN(result, 3, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 4, {Value()}));

	// bitwise or
	result = con.Query("SELECT 1 | 1, 1 | 0, 0 | 0, NULL | 1, 1 | NULL");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {1}));
	REQUIRE(CHECK_COLUMN(result, 2, {0}));
	REQUIRE(CHECK_COLUMN(result, 3, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 4, {Value()}));

	// bitwise xor
	result = con.Query("SELECT 1 # 1, 1 # 0, 0 # 0, NULL # 1, 1 # NULL");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
	REQUIRE(CHECK_COLUMN(result, 1, {1}));
	REQUIRE(CHECK_COLUMN(result, 2, {0}));
	REQUIRE(CHECK_COLUMN(result, 3, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 4, {Value()}));

	// out of range shifts return 0
	result = con.Query("SELECT 1::TINYINT << -1::TINYINT, 1::TINYINT >> -1::TINYINT, 1::TINYINT << 12::TINYINT, "
	                   "1::TINYINT >> 12::TINYINT");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
	REQUIRE(CHECK_COLUMN(result, 1, {0}));
	REQUIRE(CHECK_COLUMN(result, 2, {0}));
	REQUIRE(CHECK_COLUMN(result, 3, {0}));
	result = con.Query("SELECT 1::SMALLINT << -1::SMALLINT, 1::SMALLINT >> -1::SMALLINT, 1::SMALLINT << 20::SMALLINT, "
	                   "1::SMALLINT >> 20::SMALLINT");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
	REQUIRE(CHECK_COLUMN(result, 1, {0}));
	REQUIRE(CHECK_COLUMN(result, 2, {0}));
	REQUIRE(CHECK_COLUMN(result, 3, {0}));
	result = con.Query("SELECT 1::INT << -1::INT, 1::INT >> -1::INT, 1::INT << 40::INT, 1::INT >> 40::INT");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
	REQUIRE(CHECK_COLUMN(result, 1, {0}));
	REQUIRE(CHECK_COLUMN(result, 2, {0}));
	REQUIRE(CHECK_COLUMN(result, 3, {0}));
	result = con.Query("SELECT 1::BIGINT << -1::BIGINT, 1::BIGINT >> -1::BIGINT, 1::BIGINT << 1000::BIGINT, 1::BIGINT "
	                   ">> 1000::BIGINT");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
	REQUIRE(CHECK_COLUMN(result, 1, {0}));
	REQUIRE(CHECK_COLUMN(result, 2, {0}));
	REQUIRE(CHECK_COLUMN(result, 3, {0}));
}

TEST_CASE("Test bitwise ops with tables and different types", "[bitop]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	vector<string> types = {"TINYINT", "SMALLINT", "INTEGER", "BIGINT"};
	for (auto &type : types) {
		REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE bitwise_test(i " + type + ", j " + type + ")"));
		REQUIRE_NO_FAIL(con.Query(
		    "INSERT INTO bitwise_test VALUES (1, 1), (1, 0), (0, 1), (0, 0), (1, NULL), (NULL, 1), (NULL, NULL)"));

		result = con.Query("SELECT i << j, i >> j, i & j, i | j, i # j FROM bitwise_test");
		REQUIRE(CHECK_COLUMN(result, 0, {2, 1, 0, 0, Value(), Value(), Value()}));
		REQUIRE(CHECK_COLUMN(result, 1, {0, 1, 0, 0, Value(), Value(), Value()}));
		REQUIRE(CHECK_COLUMN(result, 2, {1, 0, 0, 0, Value(), Value(), Value()}));
		REQUIRE(CHECK_COLUMN(result, 3, {1, 1, 1, 0, Value(), Value(), Value()}));
		REQUIRE(CHECK_COLUMN(result, 4, {0, 1, 1, 0, Value(), Value(), Value()}));

		REQUIRE_NO_FAIL(con.Query("ROLLBACK"));
	}
}

TEST_CASE("Test invalid ops for bitwise operations", "[bitop]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_FAIL(con.Query("SELECT 'hello' << 3"));
	REQUIRE_FAIL(con.Query("SELECT 3 << 'hello'"));
	REQUIRE_FAIL(con.Query("SELECT 2.0 << 1"));
}
