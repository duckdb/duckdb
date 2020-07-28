#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test BLOBs with persistent storage", "[blob]") {
	auto config = GetTestConfig();
	unique_ptr<QueryResult> result;
	auto storage_database = TestCreatePath("blob_storage_test");

	// make sure the database does not exist
	DeleteDatabase(storage_database);
	{
		// create a database and insert values
		DuckDB db(storage_database, config.get());
		Connection con(db);
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE blobs (b BLOB);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO blobs VALUES('a'), ('\\xAA'), ('\\xAAFFAA'),  (''),"
								  "(NULL), ('55AAFF55AAFF55AAFF01'), ('\\x55AAFF55AAFF55AAFF01'),"
								  "('abc \153\154\155 \052\251\124'::BLOB)"));
	}
	// reload the database from disk a few times
	for (idx_t i = 0; i < 2; i++) {
		DuckDB db(storage_database, config.get());
		Connection con(db);
		result = con.Query("SELECT * FROM blobs");
		REQUIRE(CHECK_COLUMN(result, 0, {"a", Value::BLOB("\\xAA"), Value::BLOB("\\xAAFFAA"), (""),
							Value(nullptr), ("55AAFF55AAFF55AAFF01"), Value::BLOB("\\x55AAFF55AAFF55AAFF01"),
							Value::BLOB("abc \153\154\155 \052\251\124") }));
	}
	DeleteDatabase(storage_database);
}
