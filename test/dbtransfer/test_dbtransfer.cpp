#include "sqlite_transfer.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb.hpp"
#include "sqlite3.h"
#include "catch.hpp"
#include "test_helpers.hpp"

#include <string>
#include <sstream>

TEST_CASE("Pass nullptr to transfer function", "[dbtransfer]") {
	duckdb::DuckDB source_db(nullptr);
	duckdb::Connection con(source_db);

	REQUIRE_FALSE(sqlite::TransferDatabase(con, nullptr));
}

TEST_CASE("Check transfer from DuckDB to sqlite database", "[dbtransfer]") {
	duckdb::DuckDB source_db(nullptr);
	duckdb::Connection con(source_db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE items(field1 VARCHAR, field2 INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO items VALUES ('a', 1)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO items VALUES ('b', 2)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO items VALUES ('c', 3)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO items VALUES ('d', 4)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO items VALUES ('e', 5)"));

	sqlite3 *destination_db = nullptr;
	if (sqlite3_open(":memory:", &destination_db) != SQLITE_OK) {
		REQUIRE(false);
		return;
	}

	REQUIRE(sqlite::TransferDatabase(con, destination_db));

	const char *sql = "SELECT field1, field2 FROM items";
	auto check_callback = [](void *unused, int argc, char **values, char **columns) {
		static char field1 = 'a';
		static char field2 = '1';
		unused = 0;

		REQUIRE(strcmp(columns[0], "field1") == 0);
		REQUIRE(strcmp(columns[1], "field2") == 0);
		char f1[2] = {field1, '\0'};
		REQUIRE(strcmp(values[0], f1) == 0);
		char f2[2] = {field2, '\0'};
		REQUIRE(strcmp(values[1], f2) == 0);

		++field1;
		++field2;

		return 0;
	};
	char *err = 0;

	if (sqlite3_exec(destination_db, sql, check_callback, 0, &err) != SQLITE_OK) {
		sqlite3_free(err);
		sqlite3_close(destination_db);

		REQUIRE(false);
		return;
	}

	sqlite3_close(destination_db);
}

TEST_CASE("Pass pointer to sqlite3 as nullptr to QueryDatabase", "[dbtransfer]") {
	duckdb::vector<duckdb::SQLType> result_types = {duckdb::SQLTypeId::VARCHAR, duckdb::SQLTypeId::INTEGER};
	std::string query = "SELECT * from items";
	int interrupt = 0;
	REQUIRE_FALSE(sqlite::QueryDatabase(result_types, nullptr, std::move(query), interrupt));
}

TEST_CASE("Check getting query from sqlite database", "[dbtransfer]") {
	sqlite3 *source_db = nullptr;
	if (sqlite3_open(":memory:", &source_db) != SQLITE_OK) {
		REQUIRE(false);
		return;
	}

	const char *sql_create_table = "CREATE TABLE items (field1 VARCHAR, field2 INTEGER)";
	auto empty_callback = [](void *, int, char **, char **) { return 0; };
	char *err = 0;
	if (sqlite3_exec(source_db, sql_create_table, empty_callback, 0, &err) != SQLITE_OK) {
		sqlite3_free(err);
		sqlite3_close(source_db);

		REQUIRE(false);
		return;
	}

	// Inserting 5 rows in sqlite db
	char field1 = 'a';
	char field2 = '1';
	for (int i = 0; i < 5; ++i, ++field1, ++field2) {
		std::ostringstream sql_insert;
		sql_insert << "INSERT INTO items VALUES ('" << field1 << "', " << field2 << ")";
		if (sqlite3_exec(source_db, sql_insert.str().c_str(), empty_callback, 0, &err) != SQLITE_OK) {
			sqlite3_free(err);
			sqlite3_close(source_db);

			REQUIRE(false);
			return;
		}
	}

	duckdb::vector<duckdb::SQLType> result_types = {duckdb::SQLTypeId::VARCHAR, duckdb::SQLTypeId::INTEGER};
	std::string query = "SELECT * from items";
	int interrupt = 0;
	auto result = sqlite::QueryDatabase(result_types, source_db, std::move(query), interrupt);

	sqlite3_close(source_db);

	REQUIRE(result->success);
	REQUIRE(CHECK_COLUMN(result, 0, {"a", "b", "c", "d", "e"}));
	REQUIRE(CHECK_COLUMN(result, 1, {1, 2, 3, 4, 5}));
}
