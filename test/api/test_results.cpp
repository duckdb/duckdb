#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test results API", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);

	// result equality
	auto result = con.Query("SELECT 42");
	auto result2 = con.Query("SELECT 42");
	REQUIRE(result->Equals(*result2));

	// result inequality
	result = con.Query("SELECT 42");
	result2 = con.Query("SELECT 43");
	REQUIRE(!result->Equals(*result2));

	// stream query to string
	auto stream_result = con.SendQuery("SELECT 42");
	auto str = stream_result->ToString();
	REQUIRE(!str.empty());

	// materialized query to string
	result = con.Query("SELECT 42");
	str = result->ToString();
	REQUIRE(!str.empty());

	// error to string
	result = con.Query("SELEC 42");
	str = result->ToString();
	REQUIRE(!str.empty());
}

TEST_CASE("Test iterating over results", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE data(i INTEGER, j VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO data VALUES (1, 'hello'), (2, 'test')"));

	vector<int> i_values = {1, 2};
	vector<string> j_values = {"hello", "test"};
	idx_t row_count = 0;
	auto result = con.Query("SELECT * FROM data;");
	for (auto &row : *result) {
		REQUIRE(row.GetValue<int>(0) == i_values[row.row]);
		REQUIRE(row.GetValue<string>(1) == j_values[row.row]);
		row_count++;
	}
	REQUIRE(row_count == 2);
}

TEST_CASE("Error in streaming result after initial query", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);

	// create a big table with strings that are numbers
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE strings(v VARCHAR)"));
	for (size_t i = 0; i < STANDARD_VECTOR_SIZE * 2 - 1; i++) {
		REQUIRE_NO_FAIL(con.Query("INSERT INTO strings VALUES ('" + to_string(i) + "')"));
	}
	// now insert one non-numeric value
	REQUIRE_NO_FAIL(con.Query("INSERT INTO strings VALUES ('hello')"));

	// now create a streaming result
	auto result = con.SendQuery("SELECT CAST(v AS INTEGER) FROM strings");
	REQUIRE_NO_FAIL(*result);
	// initial query does not fail!
	auto chunk = result->Fetch();
	REQUIRE(chunk);
	// but subsequent query fails!
	chunk = result->Fetch();
	REQUIRE(!chunk);
	REQUIRE(!result->success);
	auto str = result->ToString();
	REQUIRE(!str.empty());
}
