#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test SHOW SELECT query", "[show_select]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers (i INTEGER, j INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers2 (i INTEGER, j INTEGER, st VARCHAR)"));
	//REQUIRE_NO_FAIL(con.Query("CREATE VIEW v1 AS SELECT DATE '1992-01-01' AS k"));

	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1, 10), (2, 12), (3, 14), (4, 16), (5, NULL), (NULL, NULL)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers2 VALUES (1, 30, 'a'), (8, 12, 'b'), (3, 24, 'c'), (9, 16, 'c'), (10, NULL, 'e')"));

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

  result = con.Query("SHOW SELECT integers.i, integers2.st FROM integers, integers2 WHERE integers.i=integers2.i");
  result->Print();
  REQUIRE(CHECK_COLUMN(result, 0, {"i", "st"}));
  REQUIRE(CHECK_COLUMN(result, 1, {"INTEGER", "VARCHAR"}));
  REQUIRE(CHECK_COLUMN(result, 2, {Value::BOOLEAN(false), Value::BOOLEAN(false)}));
  REQUIRE(CHECK_COLUMN(result, 3, {Value(), Value()}));
  REQUIRE(CHECK_COLUMN(result, 4, {Value::BOOLEAN(false), Value::BOOLEAN(false)}));

	/*result = con.Query("SHOW integers");
	// Field | Type | Null | Key | Default | Extra
	REQUIRE(CHECK_COLUMN(result, 0, {"i", "j"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"INTEGER", "INTEGER"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"YES", "YES"}));
	REQUIRE(CHECK_COLUMN(result, 3, {Value(), Value()}));
	REQUIRE(CHECK_COLUMN(result, 4, {Value(), Value()}));
	REQUIRE(CHECK_COLUMN(result, 5, {Value(), Value()}));*/
	// equivalent to PRAGMA SHOW('integers')
/*	result = con.Query("PRAGMA SHOW('integers')");
	// Field | Type | Null | Key | Default | Extra
	REQUIRE(CHECK_COLUMN(result, 0, {"i", "j"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"INTEGER", "INTEGER"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"YES", "YES"}));
	REQUIRE(CHECK_COLUMN(result, 3, {Value(), Value()}));
	REQUIRE(CHECK_COLUMN(result, 4, {Value(), Value()}));
	REQUIRE(CHECK_COLUMN(result, 5, {Value(), Value()}));
	// we can also describe views
	result = con.Query("DESCRIBE v1");
	// Field | Type | Null | Key | Default | Extra
	REQUIRE(CHECK_COLUMN(result, 0, {"k"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"DATE"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"YES"}));
	REQUIRE(CHECK_COLUMN(result, 3, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 4, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 5, {Value()}));*/

	//result = con.Query("SHOW SELECT integers.i, integers2.st FROM integers, integers2 WHERE integers.i=integers2.i");
	//result = con.Query("DESCRIBE integers;");

}
