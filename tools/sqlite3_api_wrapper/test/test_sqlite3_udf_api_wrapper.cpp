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

TEST_CASE("Basic sqlite UDF wrapper usage", "[sqlite3wrapper]") {
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
