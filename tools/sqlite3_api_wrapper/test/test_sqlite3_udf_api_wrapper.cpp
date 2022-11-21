#include "catch.hpp"
#ifdef USE_DUCKDB_SHELL_WRAPPER
#include "duckdb_shell_wrapper.h"
#endif
#include "sqlite3.h"
#include <string>
#include <thread>
#include <string.h>

#include "sqlite_db_wrapper.hpp"
#include "sqlite_stmt_wrapper.hpp"

// All UDFs are implemented in "udf_scalar_functions.hpp"
#include "udf_scalar_functions.hpp"

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

	REQUIRE(sqlite3_create_function(db_w.db, "sum_cols_int_check_nulls", 4, 0, nullptr, &sum_cols_int_check_nulls,
	                                nullptr, nullptr) == SQLITE_OK);

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
	REQUIRE(sqlite3_create_function(db_w.db, "sum_cols_int", 4, 0, nullptr, &sum_cols_int, nullptr, nullptr) ==
	        SQLITE_OK);
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
	// create function check_null-terminated_string
	REQUIRE(sqlite3_create_function(db_w.db, "check_null_terminated_string", 1, 0, nullptr,
	                                &check_null_terminated_string, nullptr, nullptr) == SQLITE_OK);

	// TEXT
	REQUIRE(db_w.Execute("SELECT check_text('XXXX'::VARCHAR)"));
	REQUIRE(db_w.CheckColumn(0, {"TTTT"}));

	// BLOB
	REQUIRE(db_w.Execute("SELECT check_blob('XXXX'::BLOB)"));
	REQUIRE(db_w.CheckColumn(0, {"BBBB"}));

	// check_null_terminated_string
	REQUIRE(db_w.Execute("SELECT check_null_terminated_string('Hello world')"));
	REQUIRE(db_w.CheckColumn(0, {"Hello world"}));
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

TEST_CASE("SQLite UDF wrapper: testing sqlite cast numbers to text", "[sqlite3wrapper]") {
	SQLiteDBWrapper db_w;

	// open an in-memory db
	REQUIRE(db_w.Open(":memory:"));
	REQUIRE(sqlite3_create_function(db_w.db, "cast_numbers_to_text", 1, 0, nullptr, &cast_numbers_to_text, nullptr,
	                                nullptr) == SQLITE_OK);

	// testing conversion of integers to text
	REQUIRE(db_w.Execute("CREATE TABLE integers(t_int TINYINT, s_int SMALLINT, i_int INTEGER, b_int BIGINT)"));
	REQUIRE(db_w.Execute(
	    "INSERT INTO integers VALUES (99, 9999, 99999999, 999999999999), (88, 8888, 88888888, 888888888888)"));

	REQUIRE(db_w.Execute("SELECT cast_numbers_to_text(t_int) FROM integers"));
	REQUIRE(db_w.CheckColumn(0, {"99", "88"}));

	REQUIRE(db_w.Execute("SELECT cast_numbers_to_text(s_int) FROM integers"));
	REQUIRE(db_w.CheckColumn(0, {"9999", "8888"}));

	REQUIRE(db_w.Execute("SELECT cast_numbers_to_text(i_int) FROM integers"));
	REQUIRE(db_w.CheckColumn(0, {"99999999", "88888888"}));

	REQUIRE(db_w.Execute("SELECT cast_numbers_to_text(b_int) FROM integers"));
	REQUIRE(db_w.CheckColumn(0, {"999999999999", "888888888888"}));

	// testing conversion of floats to text
	REQUIRE(db_w.Execute("CREATE TABLE floats(f FLOAT, d DOUBLE)"));
	REQUIRE(db_w.Execute("INSERT INTO floats VALUES (11111.0, 11111.0), (22222.0, 22222.0)"));

	REQUIRE(db_w.Execute("SELECT cast_numbers_to_text(f) FROM floats"));
	REQUIRE(db_w.CheckColumn(0, {"11111.0", "22222.0"}));

	REQUIRE(db_w.Execute("SELECT cast_numbers_to_text(d) FROM floats"));
	REQUIRE(db_w.CheckColumn(0, {"11111.0", "22222.0"}));
}

TEST_CASE("SQLite UDF wrapper: testing more casts", "[sqlite3wrapper]") {
	SQLiteDBWrapper db_w;

	// open an in-memory db
	REQUIRE(db_w.Open(":memory:"));

	REQUIRE(db_w.Execute("CREATE TABLE tbl(str VARCHAR, blob BLOB, big BIGINT, f_real FLOAT)"));
	REQUIRE(db_w.Execute("INSERT INTO tbl VALUES('DuckDB string', 'DuckDB blob', 999999999999999999, 55.0)"));

	REQUIRE(sqlite3_create_function(db_w.db, "cast_to_int32", 1, 0, nullptr, &cast_to_int32, nullptr, nullptr) ==
	        SQLITE_OK);
	REQUIRE(db_w.Execute("SELECT cast_to_int32(str) FROM tbl")); // invalid string
	REQUIRE(db_w.CheckColumn(0, {"NULL"}));

	REQUIRE(db_w.Execute("SELECT cast_to_int32(blob) FROM tbl")); // invalid blob
	REQUIRE(db_w.CheckColumn(0, {"NULL"}));

	REQUIRE(db_w.Execute("SELECT cast_to_int32(big) FROM tbl")); // big int out of int-32 range
	REQUIRE(db_w.CheckColumn(0, {"NULL"}));

	REQUIRE(db_w.Execute("SELECT cast_to_int32(f_real) FROM tbl")); // float to int-32
	REQUIRE(db_w.CheckColumn(0, {"55"}));

	REQUIRE(sqlite3_create_function(db_w.db, "cast_to_int64", 1, 0, nullptr, &cast_to_int64, nullptr, nullptr) ==
	        SQLITE_OK);
	REQUIRE(db_w.Execute("SELECT cast_to_int64(str) FROM tbl"));
	REQUIRE(db_w.CheckColumn(0, {"NULL"}));

	REQUIRE(db_w.Execute("SELECT cast_to_int64(blob) FROM tbl"));
	REQUIRE(db_w.CheckColumn(0, {"NULL"}));

	REQUIRE(db_w.Execute("SELECT cast_to_int64(big) FROM tbl"));
	REQUIRE(db_w.CheckColumn(0, {"999999999999999999"}));

	REQUIRE(db_w.Execute("SELECT cast_to_int64(f_real) FROM tbl")); // float to int-64
	REQUIRE(db_w.CheckColumn(0, {"55"}));

	REQUIRE(sqlite3_create_function(db_w.db, "cast_to_float", 1, 0, nullptr, &cast_to_float, nullptr, nullptr) ==
	        SQLITE_OK);
	REQUIRE(db_w.Execute("SELECT cast_to_float(str) FROM tbl"));
	REQUIRE(db_w.CheckColumn(0, {"NULL"}));

	REQUIRE(db_w.Execute("SELECT cast_to_float(blob) FROM tbl"));
	REQUIRE(db_w.CheckColumn(0, {"NULL"}));

	REQUIRE(db_w.Execute("SELECT cast_to_float(big) FROM tbl"));
	REQUIRE(db_w.CheckColumn(0, {"1e+18"}));
}

TEST_CASE("SQLite UDF wrapper: overload function", "[sqlite3wrapper]") {
	SQLiteDBWrapper db_w;

	// open an in-memory db
	REQUIRE(db_w.Open(":memory:"));

	int argc = 1;
	REQUIRE(sqlite3_create_function(db_w.db, "sum_overload_function", argc, 0, nullptr, &sum_overload_function, nullptr,
	                                nullptr) == SQLITE_OK);

	argc = 2;
	REQUIRE(sqlite3_create_function(db_w.db, "sum_overload_function", argc, 0, nullptr, &sum_overload_function, nullptr,
	                                nullptr) == SQLITE_OK);

	argc = 3;
	REQUIRE(sqlite3_create_function(db_w.db, "sum_overload_function", argc, 0, nullptr, &sum_overload_function, nullptr,
	                                nullptr) == SQLITE_OK);

	// testing with constant
	REQUIRE(db_w.Execute("SELECT sum_overload_function(100)"));
	REQUIRE(db_w.CheckColumn(0, {"100"}));

	REQUIRE(db_w.Execute("SELECT sum_overload_function(100, 100)"));
	REQUIRE(db_w.CheckColumn(0, {"200"}));

	REQUIRE(db_w.Execute("SELECT sum_overload_function(100, 100, 100)"));
	REQUIRE(db_w.CheckColumn(0, {"300"}));

	REQUIRE(db_w.Execute("CREATE TABLE tbl(i INTEGER, j INTEGER, k INTEGER)"));
	REQUIRE(db_w.Execute("INSERT INTO tbl VALUES(1, 2, 3)"));
	REQUIRE(db_w.Execute("INSERT INTO tbl VALUES(1, 2, 3)"));

	// testing with flat vectors
	REQUIRE(db_w.Execute("SELECT sum_overload_function(i) FROM tbl"));
	REQUIRE(db_w.CheckColumn(0, {"1", "1"}));

	REQUIRE(db_w.Execute("SELECT sum_overload_function(i, j) FROM tbl"));
	REQUIRE(db_w.CheckColumn(0, {"3", "3"}));

	REQUIRE(db_w.Execute("SELECT sum_overload_function(i, j, k) FROM tbl"));
	REQUIRE(db_w.CheckColumn(0, {"6", "6"}));
}

TEST_CASE("SQLite UDF wrapper: calling sqlite3_value_text() multiple times", "[sqlite3wrapper]") {
	SQLiteDBWrapper db_w;

	// open an in-memory db
	REQUIRE(db_w.Open(":memory:"));

	int argc = 1;
	REQUIRE(sqlite3_create_function(db_w.db, "calling_value_text_multiple_times", argc, 0, nullptr,
	                                &calling_value_text_multiple_times, nullptr, nullptr) == SQLITE_OK);

	// testing with integer
	REQUIRE(db_w.Execute("SELECT calling_value_text_multiple_times(9999::INTEGER)"));
	REQUIRE(db_w.CheckColumn(0, {"9999"}));

	// testing with float
	REQUIRE(db_w.Execute("SELECT calling_value_text_multiple_times(9999.0::FLOAT)"));
	REQUIRE(db_w.CheckColumn(0, {"9999.0"}));

	// testing with string
	REQUIRE(db_w.Execute("SELECT calling_value_text_multiple_times('Hello world'::TEXT)"));
	REQUIRE(db_w.CheckColumn(0, {"Hello world"}));
}
