#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

// The tests in this file are taken from https://www.manuelrigger.at/dbms-bugs/
TEST_CASE("Test queries found by Rigger that cause problems in other systems", "[rigger]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// TiDB
	SECTION("#5 A predicate column1 = -column2 incorrectly evaluates to false for 0 values") {
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 FLOAT);"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t1(c0 FLOAT);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t1(c0) VALUES (0);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0(c0) VALUES (0);"));
		result = con.Query("SELECT t1.c0 FROM t1, t0 WHERE t0.c0=-t1.c0;");
		REQUIRE(CHECK_COLUMN(result, 0, {0}));
	}
	SECTION("#6 Join on tables with redundant indexes causes a server panic") {
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 INT PRIMARY KEY);"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t1(c0 INT PRIMARY KEY);"));
		REQUIRE_NO_FAIL(con.Query("CREATE INDEX i0 ON t1(c0);"));
		REQUIRE_NO_FAIL(con.Query("CREATE INDEX i1 ON t0(c0);"));
		REQUIRE_NO_FAIL(con.Query("SELECT * FROM t0, t1 WHERE t1.c0=t0.c0;"));
	}
	SECTION("#7 Incorrect result for LEFT JOIN and NULL values") {
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(t0 INT UNIQUE);"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t1(c0 FLOAT);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t1(c0) VALUES (0);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0(t0) VALUES (NULL), (NULL);"));
		result = con.Query("SELECT t1.c0 FROM t1 LEFT JOIN t0 ON 1;");
		REQUIRE(CHECK_COLUMN(result, 0, {0, 0}));
	}
	SECTION("#8 Query with RIGHT JOIN causes a server panic") {
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 INT);"));
		REQUIRE_NO_FAIL(con.Query("CREATE VIEW v0(c0) AS SELECT 0 FROM t0 ORDER BY -t0.c0;"));
		// FIXME: right join not supported
		REQUIRE_FAIL(con.Query("SELECT * FROM v0 RIGHT JOIN t0 ON false;"));
	}
	// SQLite
	SECTION("#15 './' LIKE './' does not match") {
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 VARCHAR UNIQUE);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0(c0) VALUES ('./');"));
		result = con.Query("SELECT * FROM t0 WHERE t0.c0 LIKE './';");
		REQUIRE(CHECK_COLUMN(result, 0, {"./"}));
	}
	SECTION("#22 REAL rounding seems to depend on FROM clause") {
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0 (c0 VARCHAR);"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t1 (c1 REAL);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t1(c1) VALUES (8366271098608253588);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0(c0) VALUES ('a');"));
		result = con.Query("SELECT * FROM t1 WHERE (t1.c1 = CAST(8366271098608253588 AS REAL));");
		REQUIRE(CHECK_COLUMN(result, 0, {Value::FLOAT(8366271098608253588)}));
		result = con.Query("SELECT * FROM t0, t1 WHERE (t1.c1 = CAST(8366271098608253588 AS REAL));");
		REQUIRE(CHECK_COLUMN(result, 0, {"a"}));
		REQUIRE(CHECK_COLUMN(result, 1, {Value::FLOAT(8366271098608253588)}));
		result = con.Query("SELECT * FROM t0, t1 WHERE (t1.c1 >= CAST(8366271098608253588 AS REAL) AND t1.c1 <= CAST(8366271098608253588 AS REAL));");
		REQUIRE(CHECK_COLUMN(result, 0, {"a"}));
		REQUIRE(CHECK_COLUMN(result, 1, {Value::FLOAT(8366271098608253588)}));
	}
	SECTION("#24 Query results in a SEGFAULT") {
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0 (c0 INT, c1 INT, PRIMARY KEY (c0, c1));"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t1 (c0 INT);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t1 VALUES (2);"));
		result = con.Query("SELECT * FROM t0, t1 WHERE (t0.c1 >= 1 OR t0.c1 < 1) AND t0.c0 IN (1, t1.c0) ORDER BY 1;");
		REQUIRE(CHECK_COLUMN(result, 0, {}));
	}
	SECTION("#26 Nested boolean formula with IN operator computes an incorrect result") {
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 VARCHAR);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0(c0) VALUES ('val');"));
		result = con.Query("SELECT * FROM t0 WHERE (((0 <> FALSE) OR NOT (0 = FALSE OR (t0.c0 IN (-1)))) = 0);");
		REQUIRE(CHECK_COLUMN(result, 0, {"val"}));
	}
	SECTION("#57 Null pointer dereference caused by window functions in result-set of EXISTS(SELECT ...)") {
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 INTEGER);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0(c0) VALUES (0);"));
		result = con.Query("SELECT * FROM t0 WHERE EXISTS (SELECT MIN(c0) OVER (), CUME_DIST() OVER () FROM t0);");
		REQUIRE(CHECK_COLUMN(result, 0, {0}));
		result = con.Query("SELECT * FROM t0 WHERE EXISTS (SELECT MIN(c0) OVER (), CUME_DIST() OVER () FROM t0) BETWEEN 1 AND 1;");
		REQUIRE(CHECK_COLUMN(result, 0, {0}));
	}
	SECTION("#61 DISTINCT malfunctions for IS NULL") {
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0 (c0 INTEGER, c1 INTEGER NOT NULL DEFAULT 1, c2 VARCHAR);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0(c2) VALUES (NULL), (NULL), (NULL), (NULL), (NULL), (NULL), (NULL), (NULL), (NULL), (NULL), (NULL);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0(c2) VALUES ('a');"));
		result = con.Query("SELECT DISTINCT * FROM t0 WHERE t0.c0 IS NULL ORDER BY 1, 2, 3;");
		REQUIRE(CHECK_COLUMN(result, 0, {Value(), Value()}));
		REQUIRE(CHECK_COLUMN(result, 1, {1, 1}));
		REQUIRE(CHECK_COLUMN(result, 2, {Value(), "a"}));
	}
	// CockroachDB
	SECTION("#1 Internal error for NATURAL JOIN on INT and INT4 column for VECTORIZE=experimental_on") {
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 INT UNIQUE);"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t1(c0 INT4 UNIQUE);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0(c0) VALUES(0);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t1(c0) VALUES(0);"));
		result = con.Query("SELECT * FROM t0 NATURAL JOIN t1;");
		REQUIRE(CHECK_COLUMN(result, 0, {0}));
		REQUIRE(CHECK_COLUMN(result, 1, {0}));
	}
	SECTION("#1 Internal error for NATURAL JOIN on INT and INT4 column for VECTORIZE=experimental_on") {
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 VARCHAR UNIQUE);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0 (c0) VALUES ('\\a');"));
		result = con.Query("SELECT * FROM t0 WHERE c0 LIKE '\\a';");
		REQUIRE(CHECK_COLUMN(result, 0, {"\\a"}));
	}
	SECTION("#4 Incorrect result for IS NULL query on VIEW using SELECT DISTINCT") {
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 INT UNIQUE);"));
		REQUIRE_NO_FAIL(con.Query("CREATE VIEW v0(c0) AS SELECT DISTINCT t0.c0 FROM t0;"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0 (c0) VALUES (NULL), (NULL);"));
		result = con.Query("SELECT * FROM v0 WHERE v0.c0 IS NULL;");
		REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
	}
	SECTION("#9 NATURAL JOIN fails with \"duplicate column name\" on view") {
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 INT);"));
		REQUIRE_NO_FAIL(con.Query("CREATE VIEW v0(c0, c1) AS SELECT DISTINCT c0, c0 FROM t0;"));
		result = con.Query("SELECT * FROM v0 NATURAL JOIN t0;");
		REQUIRE(CHECK_COLUMN(result, 0, {}));
	}
}
