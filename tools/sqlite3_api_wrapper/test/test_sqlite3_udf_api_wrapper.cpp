#include "catch.hpp"
#include "sqlite3.h"
#include <string>
#include <thread>
#include <string.h>

#include "sqlite_db_wrapper.hpp"
#include "sqlite_stmt_wrapper.hpp"

// All UDFs are implemented in "udf_scalar_functions.hpp"
#include "udf_scalar_functions.hpp"

using namespace std;

TEST_CASE("SQLite UDF wrapper: basic usage", "[sqlite3wrapper]") {
	SQLiteDBWrapper db_w;

	// open an in-memory db
	REQUIRE(db_w.Open(":memory:"));

	// create and populate table
	REQUIRE(db_w.Execute("CREATE TABLE integers(i INTEGER)"));
	for (int i = -5; i <= 5; ++i) {
		// Insert values: -5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5
		REQUIRE(db_w.Execute("INSERT INTO integers VALUES (" + std::to_string(i) + ")"));
	}

	// create sqlite udf
	REQUIRE(sqlite3_create_function(db_w.db, "multiply10", 1, 0, nullptr, &multiply10, nullptr, nullptr) == SQLITE_OK);
	REQUIRE(db_w.Execute("SELECT multiply10(i) FROM integers"));
	REQUIRE(db_w.CheckColumn(0, {"-50", "-40", "-30", "-20", "-10", "0", "10", "20", "30", "40", "50"}));

}

TEST_CASE("SQLite UDF wrapper: testing NULL values", "[sqlite3wrapper]") {
	SQLiteDBWrapper db_w;

	// open an in-memory db
	REQUIRE(db_w.Open(":memory:"));

	// testing null values
	REQUIRE(db_w.Execute("SELECT NULL"));
	REQUIRE(db_w.CheckColumn(0, {"NULL"}));

	// insert NULL value and test
	REQUIRE(db_w.Execute("CREATE TABLE integers(i INTEGER, j INTEGER, k INTEGER, l INTEGER)"));
	REQUIRE(db_w.Execute("INSERT INTO integers VALUES (NULL, NULL, NULL, NULL), (NULL, NULL, NULL, NULL)"));

	REQUIRE(sqlite3_create_function(db_w.db, "sum_cols_int_check_nulls", 4, 0, nullptr, &sum_cols_int_check_nulls, nullptr, nullptr) == SQLITE_OK);

	REQUIRE(db_w.Execute("SELECT sum_cols_int_check_nulls(i, j, k, l) FROM integers"));
	REQUIRE(db_w.CheckColumn(0, {"NULL", "NULL"}));

	// insert valid values
	REQUIRE(db_w.Execute("INSERT INTO integers VALUES (1, 1, 1, 1), (2, 2, 2, 2)"));
	REQUIRE(db_w.Execute("SELECT sum_cols_int_check_nulls(i, j, k, l) FROM integers"));
	REQUIRE(db_w.CheckColumn(0, {"NULL", "NULL", "4", "8"}));

	// insert valid values with NULL ones
	REQUIRE(db_w.Execute("INSERT INTO integers VALUES (NULL, 1, 1, 1), (2, 2, 2, NULL)"));
	REQUIRE(db_w.Execute("SELECT sum_cols_int_check_nulls(i, j, k, l) FROM integers"));
	REQUIRE(db_w.CheckColumn(0, {"NULL", "NULL", "4", "8", "NULL", "NULL"}));

	// UDF that threats NULL entries as zero
	REQUIRE(sqlite3_create_function(db_w.db, "sum_cols_int", 4, 0, nullptr, &sum_cols_int, nullptr, nullptr) == SQLITE_OK);
	REQUIRE(db_w.Execute("DELETE FROM integers"));
	REQUIRE(db_w.Execute("INSERT INTO integers VALUES (NULL, NULL, 1, 1), (2, 2, 2, NULL)"));
	REQUIRE(db_w.Execute("SELECT sum_cols_int(i, j, k, l) FROM integers"));
	REQUIRE(db_w.CheckColumn(0, {"2", "6"}));
}

TEST_CASE("SQLite UDF wrapper: multiple arguments", "[sqlite3wrapper]") {
	SQLiteDBWrapper db_w;

	// open an in-memory db
	REQUIRE(db_w.Open(":memory:"));

	// create and populate table "integers"
	REQUIRE(db_w.Execute("CREATE TABLE integers(t_int TINYINT, s_int SMALLINT, i_int INTEGER, b_int BIGINT)"));

	for (int i = -5; i <= 5; ++i) {
		// Insert values: -5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5
		REQUIRE(db_w.Execute("INSERT INTO integers VALUES (" + std::to_string(i) + "," + std::to_string(i) + "," +
		                     std::to_string(i) + "," + std::to_string(i) + ")"));
	}

	// One argument: TINYINT
	int argc = 1;
	REQUIRE(sqlite3_create_function(db_w.db, "sum_cols_int", argc, 0, nullptr, &sum_cols_int, nullptr, nullptr) ==
	        SQLITE_OK);
	REQUIRE(db_w.Execute("SELECT sum_cols_int(t_int) FROM integers"));
	REQUIRE(db_w.CheckColumn(0, {"-5", "-4", "-3", "-2", "-1", "0", "1", "2", "3", "4", "5"}));

	// Two arguments: TINYINT + SMALLINT
	argc = 2;
	REQUIRE(sqlite3_create_function(db_w.db, "sum_cols_int2", argc, 0, nullptr, &sum_cols_int, nullptr, nullptr) ==
	        SQLITE_OK);
	REQUIRE(db_w.Execute("SELECT sum_cols_int2(t_int, s_int) FROM integers"));
	REQUIRE(db_w.CheckColumn(0, {"-10", "-8", "-6", "-4", "-2", "0", "2", "4", "6", "8", "10"}));

	// Three arguments: TINYINT + SMALLINT + INTEGER
	argc = 3;
	REQUIRE(sqlite3_create_function(db_w.db, "sum_cols_int3", argc, 0, nullptr, &sum_cols_int, nullptr, nullptr) ==
	        SQLITE_OK);
	REQUIRE(db_w.Execute("SELECT sum_cols_int3(t_int, s_int, i_int) FROM integers"));
	REQUIRE(db_w.CheckColumn(0, {"-15", "-12", "-9", "-6", "-3", "0", "3", "6", "9", "12", "15"}));

	// Four arguments: TINYINT + SMALLINT + INTEGER + BITINT
	argc = 4;
	REQUIRE(sqlite3_create_function(db_w.db, "sum_cols_int4", argc, 0, nullptr, &sum_cols_int, nullptr, nullptr) ==
	        SQLITE_OK);
	REQUIRE(db_w.Execute("SELECT sum_cols_int4(t_int, s_int, i_int, b_int) FROM integers"));
	REQUIRE(db_w.CheckColumn(0, {"-20", "-16", "-12", "-8", "-4", "0", "4", "8", "12", "16", "20"}));
}

TEST_CASE("SQLite UDF wrapper: double values", "[sqlite3wrapper]") {
	SQLiteDBWrapper db_w;

	// open an in-memory db
	REQUIRE(db_w.Open(":memory:"));

	// create and populate table "floats"
	REQUIRE(db_w.Execute("CREATE TABLE floats(f FLOAT, d DOUBLE)"));

	for (int i = -5; i <= 5; ++i) {
		// Insert values: -5.0, -4.0, -3.0, -2.0, -1.0, 0.0, 1.0, 2.0, 3.0, 4.0, 5.0
		REQUIRE(db_w.Execute("INSERT INTO floats VALUES (" + std::to_string(i) + "," + std::to_string(i) + ")"));
	}

	// create function
	REQUIRE(sqlite3_create_function(db_w.db, "sum_cols_double", 2, 0, nullptr, &sum_cols_double, nullptr, nullptr) ==
	        SQLITE_OK);

	// FLOAT + DOUBLE
	REQUIRE(db_w.Execute("SELECT sum_cols_double(f, d) FROM floats"));
	REQUIRE(db_w.CheckColumn(0, {"-10.0", "-8.0", "-6.0", "-4.0", "-2.0", "0.0", "2.0", "4.0", "6.0", "8.0", "10.0"}));
}

TEST_CASE("SQLite UDF wrapper: text and blob values", "[sqlite3wrapper]") {
	SQLiteDBWrapper db_w;

	// open an in-memory db
	REQUIRE(db_w.Open(":memory:"));

	// create function check_text
	REQUIRE(sqlite3_create_function(db_w.db, "check_text", 1, 0, nullptr, &check_text, nullptr, nullptr) == SQLITE_OK);
	// create function check_blob
	REQUIRE(sqlite3_create_function(db_w.db, "check_blob", 1, 0, nullptr, &check_blob, nullptr, nullptr) == SQLITE_OK);

	// TEXT
	REQUIRE(db_w.Execute("SELECT check_text('XXXX'::VARCHAR)"));
	REQUIRE(db_w.CheckColumn(0, {"TTTT"}));

	// BLOB
	REQUIRE(db_w.Execute("SELECT check_blob('XXXX'::BLOB)"));
	REQUIRE(db_w.CheckColumn(0, {"BBBB"}));
}

TEST_CASE("SQLite UDF wrapper: check type", "[sqlite3wrapper]") {
	SQLiteDBWrapper db_w;

	// open an in-memory db
	REQUIRE(db_w.Open(":memory:"));

	// create function
	REQUIRE(sqlite3_create_function(db_w.db, "check_type", 1, 0, nullptr, &check_type, nullptr, nullptr) == SQLITE_OK);

	// INT
	REQUIRE(db_w.Execute("SELECT check_type('4'::INTEGER)"));
	REQUIRE(db_w.CheckColumn(0, {"40"}));

	// FLOAT
	REQUIRE(db_w.Execute("SELECT check_type('4.0'::DOUBLE)"));
	REQUIRE(db_w.CheckColumn(0, {"400.0"}));

	// TEXT
	REQUIRE(db_w.Execute("SELECT check_type('aaaa'::VARCHAR)"));
	REQUIRE(db_w.CheckColumn(0, {"TEXT"}));

	// BLOB
	REQUIRE(db_w.Execute("SELECT check_type('aaaa'::BLOB)"));
	REQUIRE(db_w.CheckColumn(0, {"BLOB"}));
}

TEST_CASE("SQLite UDF wrapper: set null", "[sqlite3wrapper]") {
	SQLiteDBWrapper db_w;

	// open an in-memory db
	REQUIRE(db_w.Open(":memory:"));

	// create function
	REQUIRE(sqlite3_create_function(db_w.db, "set_null", 1, 0, nullptr, &set_null, nullptr, nullptr) == SQLITE_OK);

	// INT
	REQUIRE(db_w.Execute("SELECT set_null('4'::INTEGER)"));
	REQUIRE(db_w.CheckColumn(0, {"NULL"}));

	// FLOAT
	REQUIRE(db_w.Execute("SELECT set_null('4.0'::DOUBLE)"));
	REQUIRE(db_w.CheckColumn(0, {"NULL"}));

	// TEXT
	REQUIRE(db_w.Execute("SELECT set_null('aaaa'::VARCHAR)"));
	REQUIRE(db_w.CheckColumn(0, {"NULL"}));

	// BLOB
	REQUIRE(db_w.Execute("SELECT set_null('aaaa'::BLOB)"));
	REQUIRE(db_w.CheckColumn(0, {"NULL"}));
}

TEST_CASE("SQLite UDF wrapper: get user data", "[sqlite3wrapper]") {
	SQLiteDBWrapper db_w;

	// open an in-memory db
	REQUIRE(db_w.Open(":memory:"));

	char user_data[] = {"TEST"}; // user data to be used along the UDF
	// create function that gets user data (string) and replace the input value
	REQUIRE(sqlite3_create_function(db_w.db, "get_user_data", 1, 0, user_data, &get_user_data, nullptr, nullptr) ==
	        SQLITE_OK);

	REQUIRE(db_w.Execute("SELECT get_user_data('DUCKDB ____'::VARCHAR)"));
	REQUIRE(db_w.CheckColumn(0, {"DUCKDB TEST"}));
}
