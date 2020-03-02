#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Insert big varchar strings", "[varchar]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a VARCHAR);"));
	// insert a big varchar
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES ('aaaaaaaaaa')"));
	// sizes: 10, 100, 1000, 10000
	for (idx_t i = 0; i < 3; i++) {
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test SELECT a||a||a||a||a||a||a||a||a||a FROM test WHERE "
		                          "LENGTH(a)=(SELECT MAX(LENGTH(a)) FROM test)"));
	}

	result = con.Query("SELECT LENGTH(a) FROM test ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {10, 100, 1000, 10000}));
}

TEST_CASE("Test scanning many big varchar strings with limited memory", "[varchar][.]") {
	auto temp_dir = TestCreatePath("temp_buf");

	auto config = GetTestConfig();
	config->maximum_memory = 100000000;
	config->temporary_directory = temp_dir;

	unique_ptr<QueryResult> result;
	DuckDB db(nullptr, config.get());
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a VARCHAR);"));
	// create a big varchar (10K characters)
	REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES ('aaaaaaaaaa')"));
	// sizes: 10, 100, 1000, 10000
	for (idx_t i = 0; i < 3; i++) {
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test SELECT a||a||a||a||a||a||a||a||a||a FROM test WHERE "
		                          "LENGTH(a)=(SELECT MAX(LENGTH(a)) FROM test)"));
	}
	// now create a second table, we only insert the big varchar string in there
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE bigtable (a VARCHAR);"));
	REQUIRE_NO_FAIL(
	    con.Query("INSERT INTO bigtable SELECT a FROM test WHERE LENGTH(a)=(SELECT MAX(LENGTH(a)) FROM test)"));

	idx_t entries = 1;

	// verify that the append worked
	result = con.Query("SELECT COUNT(*), COUNT(a), MAX(LENGTH(a)), SUM(LENGTH(a)) FROM bigtable");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(entries)}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value::BIGINT(entries)}));
	REQUIRE(CHECK_COLUMN(result, 2, {10000}));
	REQUIRE(CHECK_COLUMN(result, 3, {Value::BIGINT(entries * 10000)}));
	// we create a total of 16K entries in the big table
	// the total size of this table is 16K*10K = 160MB
	// we then scan the table at every step, as our buffer pool is limited to 100MB not all strings fit in memory
	while (entries < 10000) {
		REQUIRE_NO_FAIL(con.Query("INSERT INTO bigtable SELECT * FROM bigtable"));
		entries *= 2;

		result = con.Query("SELECT COUNT(*), COUNT(a), MAX(LENGTH(a)), SUM(LENGTH(a)) FROM bigtable");
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(entries)}));
		REQUIRE(CHECK_COLUMN(result, 1, {Value::BIGINT(entries)}));
		REQUIRE(CHECK_COLUMN(result, 2, {10000}));
		REQUIRE(CHECK_COLUMN(result, 3, {Value::BIGINT(entries * 10000)}));
	}

	TestDeleteDirectory(temp_dir);
}
