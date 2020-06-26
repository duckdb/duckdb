#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/main/appender.hpp"
#include "dbgen.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test FULL OUTER JOIN", "[join]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER, j INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1, 1), (3, 3)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers2(k INTEGER, l INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers2 VALUES (1, 10), (2, 20)"));

	// equality join
	result = con.Query("SELECT i, j, k, l FROM integers FULL OUTER JOIN integers2 ON integers.i=integers2.k ORDER BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 1, 3}));
	REQUIRE(CHECK_COLUMN(result, 2, {2, 1, Value()}));
	REQUIRE(CHECK_COLUMN(result, 3, {20, 10, Value()}));

	// equality join with additional non-equality predicate
	result = con.Query("SELECT i, j, k, l FROM integers FULL OUTER JOIN integers2 ON integers.i=integers2.k AND integers.j > integers2.l ORDER BY 1, 2, 3, 4");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), Value(), 1, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), Value(), 1, 3}));
	REQUIRE(CHECK_COLUMN(result, 2, {1, 2, Value(), Value()}));
	REQUIRE(CHECK_COLUMN(result, 3, {10, 20, Value(), Value()}));

	// equality join with varchar values
	result = con.Query("SELECT i, j, k, l FROM integers FULL OUTER JOIN (SELECT k, l::VARCHAR AS l FROM integers2) integers2 ON integers.i=integers2.k ORDER BY 1, 2, 3, 4");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 1, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), 1, 3}));
	REQUIRE(CHECK_COLUMN(result, 2, {2, 1, Value()}));
	REQUIRE(CHECK_COLUMN(result, 3, {"20", "10", Value()}));
}

TEST_CASE("Test FULL OUTER JOIN with many matches", "[join][.]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER, j INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers2(k INTEGER, l INTEGER)"));
	int32_t row_count = 2000;
	vector<Value> expected_values_i, expected_values_j, expected_values_k, expected_values_l;
	{
		Appender a(con, "integers"), a2(con, "integers2");
		for(int32_t i = 0; i < row_count; i++) {
			a.AppendRow(i, 1);
			a2.AppendRow(row_count + i, 2);
		}
		for(int32_t i = 0; i < row_count; i++) {
			// first row_count entries are (NULL, NULL) for integers, and (row_count + i, 2) for integers2
			expected_values_i.push_back(Value());
			expected_values_j.push_back(Value());
			expected_values_k.push_back(Value::INTEGER(row_count + i));
			expected_values_l.push_back(2);
		}
		for(int32_t i = 0; i < row_count; i++) {
			// next row_count entries are (i, 1) for integers, and (NULL, NULL) for integers2
			expected_values_i.push_back(Value::INTEGER(i));
			expected_values_j.push_back(1);
			expected_values_k.push_back(Value());
			expected_values_l.push_back(Value());
		}
	}
	result = con.Query("SELECT i, j, k, l FROM integers FULL OUTER JOIN integers2 ON integers.i=integers2.k ORDER BY 1, 2, 3, 4");
	REQUIRE(CHECK_COLUMN(result, 0, expected_values_i));
	REQUIRE(CHECK_COLUMN(result, 1, expected_values_j));
	REQUIRE(CHECK_COLUMN(result, 2, expected_values_k));
	REQUIRE(CHECK_COLUMN(result, 3, expected_values_l));
}
