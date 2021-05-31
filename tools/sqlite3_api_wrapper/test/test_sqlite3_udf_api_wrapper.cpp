#include "catch.hpp"
#include "sqlite3.h"
#include <string>
#include <thread>

#include "sqlite_db_wrapper.hpp"
#include "sqlite_stmt_wrapper.hpp"

using namespace std;

// SQLite UDF to be register on DuckDB
void multiply10(sqlite3_context *context, int argc, sqlite3_value **argv) {
	assert(argc == 1);
	int v = sqlite3_value_int(argv[0]);
	v *= 10;
	sqlite3_result_int(context, v);
}

void sum_cols_int(sqlite3_context *context, int argc, sqlite3_value **argv) {
	assert(argc > 0);
	auto sum = sqlite3_value_int(argv[0]);
	for(int i=1; i < argc; ++i) {
		sum += sqlite3_value_int(argv[i]);
	}
	sqlite3_result_int(context, sum);
}

void sum_cols_double(sqlite3_context *context, int argc, sqlite3_value **argv) {
	assert(argc > 0);
	auto sum = sqlite3_value_double(argv[0]);
	for(int i=1; i < argc; ++i) {
		sum += sqlite3_value_double(argv[i]);
	}
	sqlite3_result_double(context, sum);
}

TEST_CASE("SQLite UDF wrapper: basic usage", "[sqlite3wrapper]") {
	SQLiteDBWrapper db_w;

	// open an in-memory db
	REQUIRE(db_w.Open(":memory:"));

	// create and populate table
	REQUIRE(db_w.Execute("CREATE TABLE integers(i INTEGER)"));
	for(int i=-5; i<=5; ++i) {
		// Insert values: -5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5
		REQUIRE(db_w.Execute("INSERT INTO integers VALUES (" + std::to_string(i) +")"))	;
	}

	// create sqlite udf
	REQUIRE(sqlite3_create_function(db_w.db, "multiply10", 1, 0, nullptr, &multiply10, nullptr, nullptr) == SQLITE_OK);
	REQUIRE(db_w.Execute("SELECT multiply10(i) FROM integers"));
	REQUIRE(db_w.CheckColumn(0, {"-50", "-40", "-30", "-20", "-10", "0", "10", "20", "30", "40", "50"}));
}


TEST_CASE("SQLite UDF wrapper: multiple arguments", "[sqlite3wrapper]") {
	SQLiteDBWrapper db_w;

	// open an in-memory db
	REQUIRE(db_w.Open(":memory:"));

	// create and populate table "integers"
	REQUIRE(db_w.Execute("CREATE TABLE integers(t_int TINYINT, s_int SMALLINT, i_int INTEGER, b_int BIGINT)"));

	for(int i=-5; i<=5; ++i) {
		// Insert values: -5, -4, -3, -2, -1, 0, 1, 2, 3, 4, 5
		REQUIRE(db_w.Execute("INSERT INTO integers VALUES (" + std::to_string(i) + "," + std::to_string(i) + "," + std::to_string(i) + "," + std::to_string(i) + ")"));
	}

	//One argument: TINYINT
	int argc = 1;
	REQUIRE(sqlite3_create_function(db_w.db, "sum_cols_int", argc, 0, nullptr, &sum_cols_int, nullptr, nullptr) == SQLITE_OK);
	REQUIRE(db_w.Execute("SELECT sum_cols_int(t_int) FROM integers"));
	REQUIRE(db_w.CheckColumn(0, {"-5", "-4", "-3", "-2", "-1", "0", "1", "2", "3", "4", "5"}));

	//Two arguments: TINYINT + SMALLINT
	argc = 2;
	REQUIRE(sqlite3_create_function(db_w.db, "sum_cols_int2", argc, 0, nullptr, &sum_cols_int, nullptr, nullptr) == SQLITE_OK);
	REQUIRE(db_w.Execute("SELECT sum_cols_int2(t_int, s_int) FROM integers"));
	REQUIRE(db_w.CheckColumn(0, {"-10", "-8", "-6", "-4", "-2", "0", "2", "4", "6", "8", "10"}));

	//Three arguments: TINYINT + SMALLINT + INTEGER
	argc = 3;
	REQUIRE(sqlite3_create_function(db_w.db, "sum_cols_int3", argc, 0, nullptr, &sum_cols_int, nullptr, nullptr) == SQLITE_OK);
	REQUIRE(db_w.Execute("SELECT sum_cols_int3(t_int, s_int, i_int) FROM integers"));
	REQUIRE(db_w.CheckColumn(0, {"-15", "-12", "-9", "-6", "-3", "0", "3", "6", "9", "12", "15"}));

	//Four arguments: TINYINT + SMALLINT + INTEGER + BITINT
	argc = 4;
	REQUIRE(sqlite3_create_function(db_w.db, "sum_cols_int4", argc, 0, nullptr, &sum_cols_int, nullptr, nullptr) == SQLITE_OK);
	REQUIRE(db_w.Execute("SELECT sum_cols_int4(t_int, s_int, i_int, b_int) FROM integers"));
	REQUIRE(db_w.CheckColumn(0, {"-20", "-16", "-12", "-8", "-4", "0", "4", "8", "12", "16", "20"}));
}

TEST_CASE("SQLite UDF wrapper: double values", "[sqlite3wrapper]") {
	SQLiteDBWrapper db_w;

	// open an in-memory db
	REQUIRE(db_w.Open(":memory:"));

	// create and populate table "floats"
	REQUIRE(db_w.Execute("CREATE TABLE floats(f FLOAT, d DOUBLE)"));

	for(int i=-5; i<=5; ++i) {
		// Insert values: -5.0, -4.0, -3.0, -2.0, -1.0, 0.0, 1.0, 2.0, 3.0, 4.0, 5.0
		REQUIRE(db_w.Execute("INSERT INTO floats VALUES (" + std::to_string(i) + "," + std::to_string(i) + ")"));
	}

	//create function
	REQUIRE(sqlite3_create_function(db_w.db, "sum_cols_double", 2, 0, nullptr, &sum_cols_double, nullptr, nullptr) == SQLITE_OK);

	//FLOAT + DOUBLE
	REQUIRE(db_w.Execute("SELECT sum_cols_double(f, d) FROM floats"));
	REQUIRE(db_w.CheckColumn(0, {"-10.0", "-8.0", "-6.0", "-4.0", "-2.0", "0.0", "2.0", "4.0", "6.0", "8.0", "10.0"}));
}
