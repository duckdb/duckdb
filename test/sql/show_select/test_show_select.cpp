#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test SHOW SELECT query", "[show_select]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers (i INTEGER, j INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers2 (i INTEGER, j INTEGER, st VARCHAR, d DATE)"));
	//REQUIRE_NO_FAIL(con.Query("CREATE VIEW v1 AS SELECT DATE '1992-01-01' AS k"));

	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1, 10), (2, 12), (3, 14), (4, 16), (5, NULL), (NULL, NULL)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers2 VALUES (1, 30, 'a', '1992-01-01'), (8, 12, 'b', '1992-01-01'), (3, 24, 'c', '1992-01-01'), (9, 16, 'd', '1992-01-01'), (10, NULL, 'e', '1992-01-01')"));

	// SHOW and DESCRIBE are aliases
	result = con.Query("SHOW SELECT * FROM integers");
  result->Print();
  REQUIRE(CHECK_COLUMN(result, 0, {"i", "j"}));
  REQUIRE(CHECK_COLUMN(result, 1, {"INTEGER", "INTEGER"}));
  REQUIRE(CHECK_COLUMN(result, 2, {Value::BOOLEAN(false), Value::BOOLEAN(false)}));
  REQUIRE(CHECK_COLUMN(result, 3, {Value(), Value()}));
  REQUIRE(CHECK_COLUMN(result, 4, {Value::BOOLEAN(false), Value::BOOLEAN(false)}));

  result = con.Query("SHOW SELECT i FROM integers");
  result->Print();
  REQUIRE(CHECK_COLUMN(result, 0, {"i"}));
  REQUIRE(CHECK_COLUMN(result, 1, {"INTEGER"}));
  REQUIRE(CHECK_COLUMN(result, 2, {Value::BOOLEAN(false)}));
  REQUIRE(CHECK_COLUMN(result, 3, {Value()}));
  REQUIRE(CHECK_COLUMN(result, 4, {Value::BOOLEAN(false)}));

  result = con.Query("SHOW SELECT integers.i, integers2.st, integers2.d FROM integers, integers2 WHERE integers.i=integers2.i");
  result->Print();
  REQUIRE(CHECK_COLUMN(result, 0, {"i", "st", "d"}));
  REQUIRE(CHECK_COLUMN(result, 1, {"INTEGER", "VARCHAR", "DATE"}));
  REQUIRE(CHECK_COLUMN(result, 2, {Value::BOOLEAN(false), Value::BOOLEAN(false), Value::BOOLEAN(false)}));
  REQUIRE(CHECK_COLUMN(result, 3, {Value(), Value(), Value()}));
  REQUIRE(CHECK_COLUMN(result, 4, {Value::BOOLEAN(false), Value::BOOLEAN(false), Value::BOOLEAN(false)}));

	result = con.Query("CREATE VIEW test AS SELECT integers.i, integers2.st, integers2.d FROM integers, integers2 WHERE integers.i=integers2.i");
	result = con.Query("DESCRIBE test");
  result->Print();

	result = con.Query("SHOW SELECT SUM(i) AS sum1, j FROM integers GROUP BY j HAVING j < 10");
	result->Print();
  REQUIRE(CHECK_COLUMN(result, 0, {"sum1", "j"}));
  REQUIRE(CHECK_COLUMN(result, 1, {"HUGEINT", "INTEGER"}));
  REQUIRE(CHECK_COLUMN(result, 2, {Value::BOOLEAN(false), Value::BOOLEAN(false)}));
  REQUIRE(CHECK_COLUMN(result, 3, {Value(), Value()}));
  REQUIRE(CHECK_COLUMN(result, 4, {Value::BOOLEAN(false), Value::BOOLEAN(false)}));


}
