#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test deletions", "[delete]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE a(i INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO a VALUES (42);"));

	result = con.Query("SELECT COUNT(*) FROM a;");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));

	// delete everything
	result = con.Query("DELETE FROM a;");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));

	// nothing left
	result = con.Query("SELECT COUNT(*) FROM a;");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));

	//////////////
	// ROLLBACK //
	//////////////
	REQUIRE_NO_FAIL(con.Query("INSERT INTO a VALUES (42);"));
	result = con.Query("SELECT COUNT(*) FROM a;");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));

	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
	// delete everything
	result = con.Query("DELETE FROM a;");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	result = con.Query("SELECT COUNT(*) FROM a;");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
	REQUIRE_NO_FAIL(con.Query("ROLLBACK"));

	// after rollback, the data is back
	result = con.Query("SELECT COUNT(*) FROM a;");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
}

TEST_CASE("Test scan with large deletions", "[delete][.]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION;"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE a(i INTEGER);"));
	for (idx_t i = 0; i < 10000; i++) {
		REQUIRE_NO_FAIL(con.Query("INSERT INTO a VALUES (" + to_string(i) + ")"));
	}
	REQUIRE_NO_FAIL(con.Query("COMMIT;"));

	// delete a segment of the table
	REQUIRE_NO_FAIL(con.Query("DELETE FROM a WHERE i >= 2000 AND i < 5000;"));

	result = con.Query("SELECT COUNT(*) FROM a;");
	REQUIRE(CHECK_COLUMN(result, 0, {7000}));
}

TEST_CASE("Test scan with many segmented deletions", "[delete][.]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	idx_t n = 20;
	idx_t val_count = 1024;
	vector<idx_t> tested_values = {0, 1, val_count - 2, val_count - 1};

	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION;"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE a(i INTEGER);"));
	for (idx_t k = 0; k < n; k++) {
		for (idx_t i = 0; i < val_count; i++) {
			REQUIRE_NO_FAIL(con.Query("INSERT INTO a VALUES (" + to_string(i) + ")"));
		}
	}
	REQUIRE_NO_FAIL(con.Query("COMMIT;"));

	// verify the initial count
	for (idx_t j = 0; j < 2; j++) {
		// verify the initial count again twice
		result = con.Query("SELECT COUNT(*) FROM a");
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(n * val_count)}));
	}

	// for every value, delete it, verify the count and then roll back
	for (auto &i : tested_values) {
		// begin a transaction and delete tuples of a specific value
		REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
		REQUIRE_NO_FAIL(con.Query("DELETE FROM a WHERE i = " + to_string(i)));
		// verify the count
		result = con.Query("SELECT COUNT(*) FROM a");
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(n * (val_count - 1))}));
		// rollback
		REQUIRE_NO_FAIL(con.Query("ROLLBACK"));

		for (idx_t j = 0; j < 2; j++) {
			// verify the initial count again twice
			result = con.Query("SELECT COUNT(*) FROM a");
			REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(n * val_count)}));
		}
	}

	// for every value, delete it in a separate connection and verify the count
	vector<unique_ptr<Connection>> cons;
	for (auto &i : tested_values) {
		auto new_connection = make_unique<Connection>(db);
		// begin a transaction and delete tuples of a specific value
		REQUIRE_NO_FAIL(new_connection->Query("BEGIN TRANSACTION"));
		REQUIRE_NO_FAIL(new_connection->Query("DELETE FROM a WHERE i = " + to_string(i)));
		// verify the count
		result = new_connection->Query("SELECT COUNT(*) FROM a");
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(n * (val_count - 1))}));
		// store the connection for now
		cons.push_back(move(new_connection));
	}
	result = con.Query("SELECT COUNT(*) FROM a");
	REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(n * val_count)}));
}
