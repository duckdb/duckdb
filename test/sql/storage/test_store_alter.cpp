#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test storage of alter table rename column", "[storage]") {
	unique_ptr<QueryResult> result;
	auto storage_database = TestCreatePath("storage_test");
	auto config = GetTestConfig();

	// make sure the database does not exist
	DeleteDatabase(storage_database);
	{
		// create a database and insert values
		DuckDB db(storage_database, config.get());
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b INTEGER);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (11, 22), (13, 22), (12, 21)"));
		for (idx_t i = 0; i < 2; i++) {
			REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION"));
			result = con.Query("SELECT a FROM test ORDER BY a");
			REQUIRE(CHECK_COLUMN(result, 0, {11, 12, 13}));

			REQUIRE_NO_FAIL(con.Query("ALTER TABLE test RENAME COLUMN a TO k"));

			result = con.Query("SELECT k FROM test ORDER BY k");
			REQUIRE(CHECK_COLUMN(result, 0, {11, 12, 13}));
			REQUIRE_NO_FAIL(con.Query(i == 0 ? "ROLLBACK" : "COMMIT"));
		}
	}
	// reload the database from disk
	for (idx_t i = 0; i < 2; i++) {
		DuckDB db(storage_database, config.get());
		Connection con(db);
		result = con.Query("SELECT k FROM test ORDER BY k");
		REQUIRE(CHECK_COLUMN(result, 0, {11, 12, 13}));
		REQUIRE_FAIL(con.Query("SELECT a FROM test"));
	}
	DeleteDatabase(storage_database);
}

TEST_CASE("Test storage of alter table add column", "[storage]") {
	unique_ptr<QueryResult> result;
	auto storage_database = TestCreatePath("storage_test");
	auto config = GetTestConfig();

	// make sure the database does not exist
	DeleteDatabase(storage_database);
	{
		// create a database and insert values
		DuckDB db(storage_database, config.get());
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b INTEGER);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (11, 22), (13, 22), (12, 21)"));
		REQUIRE_NO_FAIL(con.Query("ALTER TABLE test ADD COLUMN k INTEGER DEFAULT 2"));

		result = con.Query("SELECT k FROM test ORDER BY k");
		REQUIRE(CHECK_COLUMN(result, 0, {2, 2, 2}));
	}
	// reload the database from disk
	for (idx_t i = 0; i < 2; i++) {
		DuckDB db(storage_database, config.get());
		Connection con(db);
		result = con.Query("SELECT k FROM test ORDER BY k");
		REQUIRE(CHECK_COLUMN(result, 0, {2, 2, 2}));
	}
	DeleteDatabase(storage_database);
}

TEST_CASE("Add column to persistent table", "[storage]") {
	unique_ptr<QueryResult> result;
	auto storage_database = TestCreatePath("storage_test");
	auto config = GetTestConfig();

	// make sure the database does not exist
	DeleteDatabase(storage_database);
	{
		// create a database and insert values
		DuckDB db(storage_database, config.get());
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b INTEGER);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (11, 22), (13, 22), (12, 21)"));
	}
	// reload and alter
	{
		DuckDB db(storage_database, config.get());
		Connection con(db);

		REQUIRE_NO_FAIL(con.Query("ALTER TABLE test ADD COLUMN k INTEGER DEFAULT 2"));

		result = con.Query("SELECT k FROM test ORDER BY k");
		REQUIRE(CHECK_COLUMN(result, 0, {2, 2, 2}));
	}
	// now reload
	for (idx_t i = 0; i < 2; i++) {
		DuckDB db(storage_database, config.get());
		Connection con(db);

		result = con.Query("SELECT k FROM test ORDER BY k");
		REQUIRE(CHECK_COLUMN(result, 0, {2, 2, 2}));
	}
	DeleteDatabase(storage_database);
}

TEST_CASE("Remove column from persistent table", "[storage]") {
	unique_ptr<QueryResult> result;
	auto storage_database = TestCreatePath("storage_test");
	auto config = GetTestConfig();

	// make sure the database does not exist
	DeleteDatabase(storage_database);
	{
		// create a database and insert values
		DuckDB db(storage_database, config.get());
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b INTEGER);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (11, 22), (13, 22), (12, 21)"));
	}
	// reload and alter
	{
		DuckDB db(storage_database, config.get());
		Connection con(db);

		REQUIRE_NO_FAIL(con.Query("ALTER TABLE test DROP COLUMN b"));

		result = con.Query("SELECT * FROM test ORDER BY 1");
		REQUIRE(CHECK_COLUMN(result, 0, {11, 12, 13}));
		REQUIRE(result->names.size() == 1);
	}
	// now reload
	for (idx_t i = 0; i < 2; i++) {
		DuckDB db(storage_database, config.get());
		Connection con(db);

		result = con.Query("SELECT * FROM test ORDER BY 1");
		REQUIRE(CHECK_COLUMN(result, 0, {11, 12, 13}));
		REQUIRE(result->names.size() == 1);
	}
	DeleteDatabase(storage_database);
}

TEST_CASE("Alter column type of persistent table", "[storage]") {
	unique_ptr<QueryResult> result;
	auto storage_database = TestCreatePath("storage_test");
	auto config = GetTestConfig();

	// make sure the database does not exist
	DeleteDatabase(storage_database);
	{
		// create a database and insert values
		DuckDB db(storage_database, config.get());
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (a INTEGER, b INTEGER);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO test VALUES (11, 22), (13, 22), (12, 21)"));
	}
	// reload and alter
	{
		DuckDB db(storage_database, config.get());
		Connection con(db);

		REQUIRE_NO_FAIL(con.Query("ALTER TABLE test ALTER b TYPE VARCHAR"));

		result = con.Query("SELECT * FROM test ORDER BY 1");
		REQUIRE(CHECK_COLUMN(result, 0, {11, 12, 13}));
		REQUIRE(CHECK_COLUMN(result, 1, {"22", "21", "22"}));
		REQUIRE(result->names.size() == 2);
	}
	// now reload
	for (idx_t i = 0; i < 2; i++) {
		DuckDB db(storage_database, config.get());
		Connection con(db);

		result = con.Query("SELECT * FROM test ORDER BY 1");
		REQUIRE(CHECK_COLUMN(result, 0, {11, 12, 13}));
		REQUIRE(CHECK_COLUMN(result, 1, {"22", "21", "22"}));
		REQUIRE(result->names.size() == 2);
	}
	DeleteDatabase(storage_database);
}
