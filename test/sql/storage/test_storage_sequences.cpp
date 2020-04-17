#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Use sequences over different runs", "[storage]") {
	auto config = GetTestConfig();
	unique_ptr<QueryResult> result;
	auto storage_database = TestCreatePath("storage_test");

	// make sure the database does not exist
	DeleteDatabase(storage_database);
	{
		// create a database and insert values
		DuckDB db(storage_database, config.get());
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE SEQUENCE seq;"));
		REQUIRE_NO_FAIL(con.Query("CREATE SEQUENCE seq_cycle INCREMENT 1 MAXVALUE 3 START 2 CYCLE;"));
		result = con.Query("SELECT nextval('seq')");
		REQUIRE(CHECK_COLUMN(result, 0, {1}));
		result = con.Query("SELECT nextval('seq_cycle')");
		REQUIRE(CHECK_COLUMN(result, 0, {2}));
	}
	// reload the database from disk twice
	{
		DuckDB db(storage_database, config.get());
		Connection con(db);
	}
	{
		DuckDB db(storage_database, config.get());
		Connection con(db);
		result = con.Query("SELECT nextval('seq')");
		REQUIRE(CHECK_COLUMN(result, 0, {2}));
		result = con.Query("SELECT nextval('seq_cycle')");
		REQUIRE(CHECK_COLUMN(result, 0, {3}));
	}
	// reload again
	{
		DuckDB db(storage_database, config.get());
		Connection con(db);
		result = con.Query("SELECT nextval('seq'), nextval('seq');");
		REQUIRE(CHECK_COLUMN(result, 0, {3}));
		REQUIRE(CHECK_COLUMN(result, 1, {4}));
		result = con.Query("SELECT nextval('seq_cycle')");
		REQUIRE(CHECK_COLUMN(result, 0, {1}));

		// drop sequence
		REQUIRE_NO_FAIL(con.Query("DROP SEQUENCE seq;"));
	}
	{
		// reload
		DuckDB db(storage_database, config.get());
		Connection con(db);
		// the sequence is gone now
		REQUIRE_FAIL(con.Query("SELECT nextval('seq')"));
		// the other sequence is still there
		REQUIRE_NO_FAIL(con.Query("SELECT nextval('seq_cycle')"));
	}
	DeleteDatabase(storage_database);
}

TEST_CASE("Use sequences over different runs without checkpointing", "[storage]") {
	unique_ptr<QueryResult> result;
	auto storage_database = TestCreatePath("storage_test");

	// make sure the database does not exist
	DeleteDatabase(storage_database);
	{
		// create a database and insert values
		DuckDB db(storage_database);
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE SEQUENCE seq;"));
		REQUIRE_NO_FAIL(con.Query("CREATE SEQUENCE seq_cycle INCREMENT 1 MAXVALUE 3 START 2 CYCLE;"));
		result = con.Query("SELECT nextval('seq')");
		REQUIRE(CHECK_COLUMN(result, 0, {1}));
		result = con.Query("SELECT nextval('seq_cycle')");
		REQUIRE(CHECK_COLUMN(result, 0, {2}));
	}
	// reload the database from disk twice
	{
		DuckDB db(storage_database);
		Connection con(db);
	}
	{
		DuckDB db(storage_database);
		Connection con(db);
		result = con.Query("SELECT nextval('seq')");
		REQUIRE(CHECK_COLUMN(result, 0, {2}));
		result = con.Query("SELECT nextval('seq_cycle')");
		REQUIRE(CHECK_COLUMN(result, 0, {3}));
	}
	// reload again
	{
		DuckDB db(storage_database);
		Connection con(db);
		result = con.Query("SELECT nextval('seq'), nextval('seq');");
		REQUIRE(CHECK_COLUMN(result, 0, {3}));
		REQUIRE(CHECK_COLUMN(result, 1, {4}));
		result = con.Query("SELECT nextval('seq_cycle')");
		REQUIRE(CHECK_COLUMN(result, 0, {1}));

		// drop sequence
		REQUIRE_NO_FAIL(con.Query("DROP SEQUENCE seq;"));
	}
	{
		// reload
		DuckDB db(storage_database);
		Connection con(db);
		// the sequence is gone now
		REQUIRE_FAIL(con.Query("SELECT nextval('seq')"));
		// the other sequence is still there
		REQUIRE_NO_FAIL(con.Query("SELECT nextval('seq_cycle')"));
	}
	DeleteDatabase(storage_database);
}

TEST_CASE("Use sequences with uncommited transaction", "[storage]") {
	auto config = GetTestConfig();
	unique_ptr<QueryResult> result;
	auto storage_database = TestCreatePath("storage_test");

	// make sure the database does not exist
	DeleteDatabase(storage_database);
	{
		// create a database and insert values
		DuckDB db(storage_database, config.get());
		Connection con(db);
		Connection con2(db);
		REQUIRE_NO_FAIL(con.Query("CREATE SEQUENCE seq;"));
		REQUIRE_NO_FAIL(con2.Query("BEGIN TRANSACTION"));
		result = con2.Query("SELECT nextval('seq')");
		REQUIRE(CHECK_COLUMN(result, 0, {1}));
		result = con.Query("SELECT nextval('seq')");
		REQUIRE(CHECK_COLUMN(result, 0, {2}));
	}
	// reload the database from disk
	{
		DuckDB db(storage_database, config.get());
		Connection con(db);
		result = con.Query("SELECT nextval('seq')");
		REQUIRE(CHECK_COLUMN(result, 0, {3}));
	}
	// reload again
	{
		DuckDB db(storage_database, config.get());
		Connection con(db);
		result = con.Query("SELECT nextval('seq'), nextval('seq');");
		REQUIRE(CHECK_COLUMN(result, 0, {4}));
		REQUIRE(CHECK_COLUMN(result, 1, {5}));
	}
	DeleteDatabase(storage_database);
}
