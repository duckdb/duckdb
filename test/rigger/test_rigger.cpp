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
		REQUIRE_NO_FAIL(con.Query("SELECT * FROM v0 RIGHT JOIN t0 ON false;"));
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
		result = con.Query("SELECT * FROM t0, t1 WHERE (t1.c1 >= CAST(8366271098608253588 AS REAL) AND t1.c1 <= "
		                   "CAST(8366271098608253588 AS REAL));");
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
		result = con.Query(
		    "SELECT * FROM t0 WHERE EXISTS (SELECT MIN(c0) OVER (), CUME_DIST() OVER () FROM t0) BETWEEN 1 AND 1;");
		REQUIRE(CHECK_COLUMN(result, 0, {0}));
	}
	SECTION("#61 DISTINCT malfunctions for IS NULL") {
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0 (c0 INTEGER, c1 INTEGER NOT NULL DEFAULT 1, c2 VARCHAR);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0(c2) VALUES (NULL), (NULL), (NULL), (NULL), (NULL), (NULL), (NULL), "
		                          "(NULL), (NULL), (NULL), (NULL);"));
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

TEST_CASE("Tests found by Rigger", "[rigger]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();
	SECTION("489") {
		// A predicate NOT(NULL OR TRUE) unexpectedly evaluates to TRUE
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 INT);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0(c0) VALUES (0);"));
		result = con.Query("SELECT * FROM t0 WHERE NOT(NULL OR TRUE);");
		REQUIRE(CHECK_COLUMN(result, 0, {}));
		result = con.Query("SELECT NULL OR TRUE;");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
		result = con.Query("SELECT NOT(NULL OR TRUE);");
		REQUIRE(CHECK_COLUMN(result, 0, {false}));
	}
	SECTION("490") {
		// A comparison column=column unexpectedly evaluates to TRUE for column=NULL
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 INT);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0(c0) VALUES (NULL);"));
		result = con.Query("SELECT * FROM t0 WHERE c0 = c0;");
		REQUIRE(CHECK_COLUMN(result, 0, {}));
	}
	SECTION("491") {
		// PRAGMA table_info provides no output
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 INT);"));
		result = con.Query("PRAGMA table_info('t0');");
		REQUIRE(CHECK_COLUMN(result, 0, {0}));
		REQUIRE(CHECK_COLUMN(result, 1, {"c0"}));
		REQUIRE(CHECK_COLUMN(result, 2, {"INTEGER"}));
		REQUIRE(CHECK_COLUMN(result, 3, {false}));
		REQUIRE(CHECK_COLUMN(result, 4, {Value()}));
		REQUIRE(CHECK_COLUMN(result, 5, {false}));
		result = con.Query("SELECT * FROM pragma_table_info('t0');");
		REQUIRE(CHECK_COLUMN(result, 0, {0}));
		REQUIRE(CHECK_COLUMN(result, 1, {"c0"}));
		REQUIRE(CHECK_COLUMN(result, 2, {"INTEGER"}));
		REQUIRE(CHECK_COLUMN(result, 3, {false}));
		REQUIRE(CHECK_COLUMN(result, 4, {Value()}));
		REQUIRE(CHECK_COLUMN(result, 5, {false}));
	}
	SECTION("493") {
		// SIMILAR TO results in an "Unknown error -1
		result = con.Query("SELECT '' SIMILAR TO '';");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
	}
	SECTION("495") {
		//		 Comparison on UNIQUE NUMERIC column causes a query to omit a row in the result set
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 NUMERIC UNIQUE);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0(c0) VALUES (1163404482), (0), (488566);"));
		result = con.Query("SELECT * FROM t0 WHERE c0 > 0.1 ORDER BY 1;");
		REQUIRE(CHECK_COLUMN(result, 0, {488566, 1163404482}));
		result = con.Query("SELECT * FROM t0 WHERE c0 >= 0.1 ORDER BY 1;");
		REQUIRE(CHECK_COLUMN(result, 0, {488566, 1163404482}));
		result = con.Query("SELECT * FROM t0 WHERE 0.1 < c0 ORDER BY 1;");
		REQUIRE(CHECK_COLUMN(result, 0, {488566, 1163404482}));
		result = con.Query("SELECT * FROM t0 WHERE 0.1 <= c0 ORDER BY 1;");
		REQUIRE(CHECK_COLUMN(result, 0, {488566, 1163404482}));
	}
	SECTION("497") {
		// Comparison of two boolean columns in different tables results in an error "Not implemented: Unimplemented
		// type for sort"
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 BOOL);"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t1(c0 BOOL);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t1(c0) VALUES (0);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0(c0) VALUES (0);"));
		result = con.Query("SELECT t0.c0 FROM t0, t1 WHERE t1.c0 < t0.c0;");
		REQUIRE(CHECK_COLUMN(result, 0, {}));
	}
	SECTION("503") {
		// RIGHT JOIN with a predicate that compares two integer columns results in an "Unhandled type" error
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 INT);"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t1(c0 INT);"));
		result = con.Query("SELECT * FROM t0 RIGHT JOIN t1 ON t0.c0!=t1.c0;");
		REQUIRE(CHECK_COLUMN(result, 0, {}));
		REQUIRE(CHECK_COLUMN(result, 1, {}));
	}
	SECTION("504") {
		// INSERT results in an error "Not implemented: Cannot create data from this type"
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 BOOLEAN, c1 INT, PRIMARY KEY(c0, c1));"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0(c1, c0) VALUES (0, 0);"));
		result = con.Query("SELECT * FROM t0;");
		REQUIRE(CHECK_COLUMN(result, 0, {false}));
		REQUIRE(CHECK_COLUMN(result, 1, {0}));
	}
	SECTION("505") {
		// A RIGHT JOIN unexpectedly fetches rows
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 INT);"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t1(c1 BOOLEAN);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0(c0) VALUES (1);"));
		result = con.Query("SELECT * FROM t0 RIGHT JOIN t1 on true;");
		REQUIRE(CHECK_COLUMN(result, 0, {}));
		REQUIRE(CHECK_COLUMN(result, 1, {}));
	}
	SECTION("506") {
		// Query results in an error "INTERNAL: Failed to bind column reference "c0" [5.0] (bindings: [6.0])"
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 INT);"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t1(c0 INT);"));
		REQUIRE_NO_FAIL(con.Query("SELECT * FROM t1 JOIN t0 ON t1.c0 < t1.c0 - t0.c0 WHERE t0.c0 <= t1.c0;"));
		REQUIRE_NO_FAIL(con.Query("SELECT * FROM t1 JOIN t0 ON t0.c0 + t1.c0 < t1.c0 - t0.c0;"));
	}
	SECTION("507") {
		// Creating an empty table results in a crash
		REQUIRE_FAIL(con.Query("CREATE TABLE t0();"));
	}
	SECTION("508") {
		// LEFT JOIN on column with NULL value results in a segmentation fault
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 INT);"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t1(c0 INT);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0(c0) VALUES (0);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t1(c0) VALUES (NULL);"));
		result = con.Query("SELECT * FROM t1 LEFT JOIN t0 ON t0.c0=t1.c0;");
		REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
		REQUIRE(CHECK_COLUMN(result, 1, {Value()}));
	}
	SECTION("510") {
		// SIMILAR TO results in an incorrect result
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 INT);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0(c0) VALUES (-10);"));
		result = con.Query("SELECT '-10' SIMILAR TO '0';");
		REQUIRE(CHECK_COLUMN(result, 0, {false}));
		result = con.Query("SELECT t0.c0 SIMILAR TO 0 FROM t0;");
		REQUIRE(CHECK_COLUMN(result, 0, {false}));
		result = con.Query("SELECT t0.c0 NOT SIMILAR TO 0 FROM t0;");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));
		result = con.Query("SELECT * FROM t0 WHERE t0.c0 NOT SIMILAR TO 0;");
		REQUIRE(CHECK_COLUMN(result, 0, {-10}));
	}
	SECTION("513") {
		// LEFT JOIN with comparison on integer columns results in "Not implemented: Unimplemented type for nested loop
		// join!"
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 INT);"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t1(c0 INT);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t1(c0) VALUES (0);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0(c0) VALUES (0);"));
		result = con.Query("SELECT * FROM t0 LEFT JOIN t1 ON t0.c0 <= t1.c0;");
		REQUIRE(CHECK_COLUMN(result, 0, {0}));
		REQUIRE(CHECK_COLUMN(result, 1, {0}));
	}
	SECTION("514") {
		// Incorrect result after an INSERT violates a UNIQUE constraint
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 INT);"));
		REQUIRE_NO_FAIL(con.Query("CREATE UNIQUE INDEX i0 ON t0(c0);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0(c0) VALUES (1);"));
		result = con.Query("SELECT * FROM t0 WHERE t0.c0 = 1;");
		REQUIRE(CHECK_COLUMN(result, 0, {1}));
		REQUIRE_FAIL(con.Query("INSERT INTO t0(c0) VALUES (1);"));
		result = con.Query("SELECT * FROM t0 WHERE t0.c0 = 1;");
		REQUIRE(CHECK_COLUMN(result, 0, {1}));

		// verify correct behavior here too when we have multiple nodes
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0(c0) VALUES (2);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0(c0) VALUES (3);"));
		REQUIRE_FAIL(con.Query("INSERT INTO t0(c0) VALUES (2);"));
		result = con.Query("SELECT * FROM t0 WHERE t0.c0 = 2;");
		REQUIRE(CHECK_COLUMN(result, 0, {2}));
	}
	SECTION("515") {
		// Query with a negative shift predicate yields an incorrect result
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 INT);"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t1(c0 INT8, c1 DOUBLE);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t1(c0) VALUES (0);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t1(c1, c0) VALUES (1, 1);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0 VALUES (0);"));
		result = con.Query("SELECT * FROM t1 JOIN t0 ON t1.c1 WHERE NOT (t1.c0<<-1);");
		REQUIRE(CHECK_COLUMN(result, 0, {1}));
		REQUIRE(CHECK_COLUMN(result, 1, {1}));
		REQUIRE(CHECK_COLUMN(result, 2, {0}));
		result = con.Query("SELECT * FROM t1 JOIN t0 ON t1.c1 WHERE (t1.c0<<-1);");
		REQUIRE(CHECK_COLUMN(result, 0, {}));
		REQUIRE(CHECK_COLUMN(result, 1, {}));
		REQUIRE(CHECK_COLUMN(result, 2, {}));
		result = con.Query("SELECT NOT (t1.c0<<-1) FROM t1;");
		REQUIRE(CHECK_COLUMN(result, 0, {true, true}));
	}
	SECTION("516") {
		// Query with comparison on boolean column results in "Invalid type: Invalid Type [BOOL]: Invalid type for
		// index"
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 BOOL UNIQUE);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0(c0) VALUES (0);"));
		result = con.Query("SELECT * FROM t0 WHERE t0.c0 = true;");
		REQUIRE(CHECK_COLUMN(result, 0, {}));
	}
	SECTION("517") {
		// Query with an AND predicate, NOT and comparison yields an incorrect result
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 INT);"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t1(c0 INT);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0(c0) VALUES (0);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t1(c0) VALUES (0);"));
		result = con.Query("SELECT * FROM t1, t0 WHERE NOT ((t1.c0 AND t0.c0) < 0);");
		REQUIRE(CHECK_COLUMN(result, 0, {0}));
		REQUIRE(CHECK_COLUMN(result, 0, {0}));
		result = con.Query("SELECT * FROM t1, t0 WHERE ((t1.c0 AND t0.c0) < 0);");
		REQUIRE(CHECK_COLUMN(result, 0, {}));
		REQUIRE(CHECK_COLUMN(result, 0, {}));
	}
	SECTION("518") {
		// Query using the LN() function does not terminate
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 INT);"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t1(c0 INT);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0(c0) VALUES (0);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t1(c0) VALUES (0), (0), (1), (-1);"));
		result = con.Query("SELECT LN(t1.c0) FROM t0, t1 WHERE LN(t1.c0) < t0.c0;");
		REQUIRE(CHECK_COLUMN(result, 0, {}));
		result = con.Query("SELECT t1.c0, LN(t1.c0) FROM t1 ORDER BY t1.c0;");
		REQUIRE(CHECK_COLUMN(result, 0, {-1, 0, 0, 1}));
		REQUIRE(CHECK_COLUMN(result, 1, {Value(), Value(), Value(), 0}));
	}
	SECTION("521") {
		// ROUND() evaluates to -nan
		result = con.Query("SELECT ROUND(0.1, 1000);");
		REQUIRE(CHECK_COLUMN(result, 0, {0.1}));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 INT);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0(c0) VALUES (0);"));
		result = con.Query("SELECT * FROM t0 WHERE t0.c0 > ROUND(0.1, 1000);");
		REQUIRE(CHECK_COLUMN(result, 0, {}));
		result = con.Query("SELECT * FROM t0 WHERE t0.c0 <= ROUND(0.1, 1000);");
		REQUIRE(CHECK_COLUMN(result, 0, {0}));
	}
	SECTION("522") {
		// Casting a large number to REAL and multiplying it with zero results in -nan
		REQUIRE_FAIL(con.Query("SELECT 1e100::real*0;"));
	}
	SECTION("523") {
		// The trigonometric functions can result in -nan
		REQUIRE_FAIL(con.Query("SELECT SIN(1e1000);"));
	}
	SECTION("525") {
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 INT);"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t1(c0 FLOAT);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0(c0) VALUES (1), (0);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t1(c0) VALUES (1);"));
		result = con.Query("SELECT t1.c0 FROM t1 JOIN t0 ON t1.c0 IN (t0.c0) WHERE t1.c0<=t0.c0;");
		REQUIRE(CHECK_COLUMN(result, 0, {1.0}));
	}
	SECTION("526") {
		// Query that uses the CONCAT() function and OR expression crashes
		// FIXME: #398
		con.DisableQueryVerification();
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 REAL);"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t1(c0 INT2);"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t2(c0 INT);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0 VALUES (-1);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t1 VALUES (0);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t2 VALUES (0), (0);"));
		result = con.Query("SELECT * FROM t1, t2, t0 WHERE CONCAT(t1.c0) OR t0.c0;");
		REQUIRE(CHECK_COLUMN(result, 0, {0, 0}));
		REQUIRE(CHECK_COLUMN(result, 1, {0, 0}));
		REQUIRE(CHECK_COLUMN(result, 2, {-1.0, -1.0}));
	}
	SECTION("527") {
		// Query with JOIN and WHERE condition unexpectedly fetches a value not present in the table
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 INT);"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t1(c0 INT);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0 VALUES (0);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t1 VALUES (1), (1);"));
		result = con.Query("SELECT t0.c0 FROM t0 JOIN t1 ON t0.c0=(t1.c0 IS NULL) WHERE t0.c0 NOT IN (t1.c0);");
		REQUIRE(CHECK_COLUMN(result, 0, {0, 0}));
		result = con.Query("SELECT t0.c0 FROM t0 JOIN t1 ON t0.c0=(t1.c0 IS NULL);");
		REQUIRE(CHECK_COLUMN(result, 0, {0, 0}));
	}
	SECTION("528") {
		// Incorrect result for query that uses REGEXP_MATCHES()
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 VARCHAR);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0(c0) VALUES (0.1);"));
		result = con.Query("SELECT * FROM t0 WHERE REGEXP_MATCHES(t0.c0, '1');");
		REQUIRE(CHECK_COLUMN(result, 0, {"0.1"}));
		result = con.Query("SELECT * FROM t0 WHERE NOT REGEXP_MATCHES(t0.c0, '1');");
		REQUIRE(CHECK_COLUMN(result, 0, {}));
		result = con.Query("SELECT REGEXP_MATCHES(t0.c0, '1') FROM t0;");
		REQUIRE(CHECK_COLUMN(result, 0, {true}));

		result = con.Query("SELECT * FROM t0 WHERE REGEXP_FULL_MATCH(t0.c0, '1');");
		REQUIRE(CHECK_COLUMN(result, 0, {}));
		result = con.Query("SELECT * FROM t0 WHERE NOT REGEXP_FULL_MATCH(t0.c0, '1');");
		REQUIRE(CHECK_COLUMN(result, 0, {"0.1"}));
		result = con.Query("SELECT REGEXP_FULL_MATCH(t0.c0, '1') FROM t0;");
		REQUIRE(CHECK_COLUMN(result, 0, {false}));
	}
	SECTION("531") {
		// SELECT on DATE column with a large negative value results in a "double free or corruption"
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 DATE);"));
		REQUIRE_FAIL(con.Query("INSERT INTO t0 VALUES (-10000000);"));
		REQUIRE_FAIL(con.Query("SELECT (-10000000)::DATE;"));
	}
	SECTION("533") {
		// Overflow when casting from REAL to INT results in "Invalid TypeId -1"
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 REAL);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0(c0) VALUES (10000000000);"));
		REQUIRE_FAIL(con.Query("SELECT t0.c0 ::INT FROM t0;"));
	}
	SECTION("534") {
		// UPDATE causes subsequent query to segfault
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 VARCHAR, c1 DOUBLE UNIQUE);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0(c0) VALUES (NULL);"));
		REQUIRE_NO_FAIL(con.Query("UPDATE t0 SET c0=0;"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0(c0, c1) VALUES (0, 0);"));
		result = con.Query("SELECT * FROM t0 WHERE 1 > c1;");
		REQUIRE(CHECK_COLUMN(result, 0, {"0"}));
		REQUIRE(CHECK_COLUMN(result, 1, {0}));
	}
	SECTION("535") {
		// GROUP BY clause results in non-deterministic result
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 NUMERIC);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0(c0) VALUES (-515965088);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0(c0) VALUES (1), (-5.15965088E8);"));
		REQUIRE_NO_FAIL(con.Query("CREATE INDEX i0 ON t0(c0);"));
		result = con.Query("SELECT t0.c0 FROM t0 GROUP BY t0.c0, REVERSE(t0.c0) ORDER BY 1;");
		REQUIRE(CHECK_COLUMN(result, 0, {-515965088, 1}));
	}
	SECTION("536") {
		// Nondeterministic clause when using an UNION query and HAVING clause
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 INT);"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t1(c0 VARCHAR);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t1 VALUES (0.9201898334673894), (0);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0 VALUES (0);"));
		result = con.Query("SELECT * FROM t0, t1 GROUP BY t0.c0, t1.c0 HAVING t1.c0!=MAX(t1.c0);");
		REQUIRE(CHECK_COLUMN(result, 0, {}));
		result = con.Query("SELECT * FROM t0, t1 GROUP BY t0.c0, t1.c0 HAVING t1.c0!=MAX(t1.c0) UNION ALL SELECT * "
		                   "FROM t0, t1 GROUP BY t0.c0, t1.c0 HAVING NOT t1.c0>MAX(t1.c0) ORDER BY 1, 2;");
		REQUIRE(CHECK_COLUMN(result, 0, {0, 0}));
		REQUIRE(CHECK_COLUMN(result, 1, {"0", "0.9201898334673894"}));
	}
	SECTION("537") {
		// Fetching from table and view results in a crash
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 INT);"));
		REQUIRE_NO_FAIL(con.Query("CREATE VIEW v0 AS SELECT 0, 1 FROM t0 ORDER BY t0.c0;"));
		result = con.Query("SELECT t0.c0 FROM t0, v0;");
		REQUIRE(CHECK_COLUMN(result, 0, {}));
	}
	SECTION("538") {
		// Incorrect result for predicate with shift on a BIGINT column
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 BIGINT);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0(c0) VALUES (-1);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0(c0) VALUES (0);"));
		result = con.Query("SELECT t0.c0 AND (t0.c0<<64) FROM t0;");
		REQUIRE(CHECK_COLUMN(result, 0, {false, false}));

		result = con.Query("SELECT * FROM t0 WHERE t0.c0 AND (t0.c0<<64);");
		REQUIRE(CHECK_COLUMN(result, 0, {}));
	}
	SECTION("540") {
		// Nested MAX() results in nondeterministic result or double free
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 VARCHAR);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0 VALUES ('aaaaaaaaaaaa');"));
		result = con.Query("SELECT MAX(agg0) FROM (SELECT MAX(t0.c0) AS agg0 FROM t0) as s0;");
		REQUIRE(CHECK_COLUMN(result, 0, {"aaaaaaaaaaaa"}));
	}
	SECTION("543") {
		// Unexpected result for SUM() upon overflow
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c1 BIGINT);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0(c1) VALUES (2);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0(c1) VALUES (9223372036854775807);"));
		result = con.Query("SELECT SUM(t0.c1) FROM t0;");
		REQUIRE(CHECK_COLUMN(result, 0, {9223372036854775809.0}));

		REQUIRE_NO_FAIL(con.Query("DROP TABLE t0;"));

		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c1 BIGINT);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0(c1) VALUES (1);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0(c1) VALUES (9223372036854775807);"));
		result = con.Query("SELECT SUM(t0.c1) FROM t0;");
		REQUIRE(CHECK_COLUMN(result, 0, {9223372036854775808.0}));
	}
	SECTION("544") {
		// SELECT on view with text constant in ORDER BY crashes
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 INT);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0(c0) VALUES (0);"));
		REQUIRE_NO_FAIL(con.Query("CREATE VIEW v0(c0) AS SELECT 1 FROM t0;"));
		result = con.Query("SELECT * FROM v0 ORDER BY 'a';");
		REQUIRE(CHECK_COLUMN(result, 0, {1}));
	}
	SECTION("547") {
		// Query with SIMILAR TO results in "Assertion `strlen(dataptr) == length' failed"
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 INT);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0 VALUES (0);"));
		result = con.Query("SELECT * FROM t0 WHERE t0.c0 SIMILAR TO '.';");
		REQUIRE(CHECK_COLUMN(result, 0, {"0"}));
	}
	SECTION("549") {
		// Nested CASE expression results in Assertion `other.auxiliary->type == VectorBufferType::STRING_BUFFER' failed
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 INT);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0 VALUES (NULL), (0), (1); "));
		REQUIRE_FAIL(con.Query("SELECT * FROM t0 WHERE CASE WHEN c0 THEN 0 ELSE CASE '0.1' WHEN c0 THEN '' END END;"));
		result = con.Query("SELECT CASE WHEN c0 THEN 0 ELSE (CASE '0' WHEN c0 THEN '0.6' END) END FROM t0;");
		REQUIRE(CHECK_COLUMN(result, 0, {Value(), "0.6", "0"}));
	}
	SECTION("552") {
		// RIGHT JOIN results in Assertion `filter->expressions.size() == 1'
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 INT);"));
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t1(c0 INT);"));
		result = con.Query("SELECT * FROM t0 RIGHT JOIN t1 ON 0 WHERE t0.c0 OR t1.c0 BETWEEN t0.c0 AND 1;");
		REQUIRE(CHECK_COLUMN(result, 0, {}));
	}
	SECTION("560") {
		// Incorrect result for SUM() and negative number
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0 (c0 INT);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0 VALUES (0);"));
		result = con.Query("SELECT SUM(-1) FROM t0;");
		REQUIRE(CHECK_COLUMN(result, 0, {-1}));
	}
	SECTION("562") {
		// SELECT with CASE expression causes an assertion failure "Assertion `!entry.first->Equals(&expr)' failed"
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 INT);"));
		REQUIRE_FAIL(
		    con.Query("SELECT * FROM t0 GROUP BY -4.40304405E8 ORDER BY (CASE 1 WHEN 0 THEN 0 ELSE -440304405 END);"));
		result =
		    con.Query("SELECT 1 FROM t0 GROUP BY -4.40304405E8 ORDER BY (CASE 1 WHEN 0 THEN 0 ELSE -440304405 END);");
		REQUIRE(CHECK_COLUMN(result, 0, {}));
	}
	SECTION("567") {
		// REVERSE() on special character results in "Assertion `strcmp(dataptr, normalized) == 0' failed."
		result = con.Query("SELECT REVERSE('S̈a︍');");
		REQUIRE(CHECK_COLUMN(result, 0, {"a︍S̈"}));
	}
	SECTION("571") {
		// Incorrect result for BETWEEN query that casts column to boolean #571
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 INT);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0 VALUES (-1);"));
		result = con.Query("SELECT t0.c0 FROM t0 WHERE NOT (0 BETWEEN 0 AND t0.c0::BOOL);");
		REQUIRE(CHECK_COLUMN(result, 0, {}));
	}
	SECTION("579") {
		// Updated value in column is not visible in a SELECT
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 VARCHAR, c1 VARCHAR);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0(c0) VALUES(0), ('');"));
		REQUIRE_NO_FAIL(con.Query("UPDATE t0 SET c1 = 1;"));
		// -- expected: {1}, actual: {''}
		result = con.Query("SELECT t0.c1 FROM t0 WHERE '' = t0.c0;");
		REQUIRE(CHECK_COLUMN(result, 0, {"1"}));
	}
	SECTION("580") {
		// SUBSTRING with an invalid start position causes a segmentation fault #580
		result = con.Query("SELECT SUBSTRING(0, 3, 0)");
		REQUIRE(CHECK_COLUMN(result, 0, {""}));
	}
	SECTION("581") {
		// DISTINCT malfunctions for BOOLEAN
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 BOOLEAN);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0 VALUES (NULL), (false);"));
		result = con.Query("SELECT DISTINCT t0.c0 FROM t0 ORDER BY 1;");
		REQUIRE(CHECK_COLUMN(result, 0, {Value(), false}));
	}
	SECTION("583") {
		// Comparing a string with a boolean yields an incorrect result after UPDATE
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 VARCHAR);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0(c0) VALUES (0);"));
		REQUIRE_NO_FAIL(con.Query("UPDATE t0 SET c0=0;"));
		REQUIRE_NO_FAIL(con.Query("UPDATE t0 SET c0=true;"));
		// -- expected: {true}, actual: {}
		result = con.Query("SELECT * FROM t0 WHERE t0.c0 = true;");
		REQUIRE(CHECK_COLUMN(result, 0, {"true"}));
	}
	SECTION("584") {
		// A select with BETWEEN and VARCHAR cast results in an incorrect result
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 INTEGER);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0(c0) VALUES (-2);"));
		result = con.Query("SELECT t0.c0 FROM t0 WHERE -1 BETWEEN t0.c0::VARCHAR AND 1;");
		REQUIRE(CHECK_COLUMN(result, 0, {-2}));
	}
	SECTION("585") {
		// Predicate checking for an empty string yields an incorrect result
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 VARCHAR);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0(c0) VALUES (''), (0)"));
		result = con.Query("SELECT * FROM t0 WHERE t0.c0 = ''; ");
		//  -- expected: {''}, actual: {}
		REQUIRE(CHECK_COLUMN(result, 0, {""}));
	}
	SECTION("586") {
		// NOACCENT.NOCASE comparison with a special character results in a segmentation fault
		result = con.Query("SELECT ''='Ʇ';");
		REQUIRE(CHECK_COLUMN(result, 0, {false}));
		result = con.Query("SELECT '' COLLATE NOACCENT.NOCASE='Ʇ';");
		REQUIRE(CHECK_COLUMN(result, 0, {false}));
	}
	SECTION("587") {
		// A negative DATE results in a "double free or corruption" crash
		result = con.Query("SELECT (DATE '-10000-01-01')::VARCHAR;");
		REQUIRE(CHECK_COLUMN(result, 0, {"10000-01-01 (BC)"}));
	}
	SECTION("588") {
		// Query with complex ORDER BY causes an incorrect rowid value
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 INT);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0 VALUES (1), (0), (1);"));
		result = con.Query(
		    "SELECT t0.rowid FROM t0 WHERE t0.rowid ORDER BY CASE ((t0.c0) ::BOOL) WHEN 1 THEN t0.rowid END;");
		REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
	}
	SECTION("589") {
		// Creating an index on rowid results in an internal error "Failed to bind column reference"
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 INT);"));
		REQUIRE_FAIL(con.Query("CREATE INDEX i0 ON t0(rowid, c0);"));
	}
	SECTION("590") {
		// Comparison with a DATE yields an incorrect result
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 VARCHAR);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0(c0) VALUES (DATE '2000-01-02');"));
		result = con.Query("SELECT * FROM t0 WHERE DATE '2000-01-01' < t0.c0;");
		REQUIRE(CHECK_COLUMN(result, 0, {"2000-01-02"}));
	}
	SECTION("591") {
		// Subtracting a large integer from a DATE results in a "double free or corruption"
		result = con.Query("SELECT (- 41756167 + '1969-12-11 032657' ::DATE)::VARCHAR;");
		REQUIRE(CHECK_COLUMN(result, 0, {"112356-06-10 (BC)"}));
	}
	SECTION("592") {
		// Expression with LIKE and comparison causes an assertion failure
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 VARCHAR);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0 VALUES (0);"));
		result = con.Query("SELECT * FROM t0 WHERE c0 LIKE '' AND c0 < true;");
		REQUIRE(CHECK_COLUMN(result, 0, {}));
	}
	SECTION("596") {
		// STDDEV_POP unexpectedly does not fetch any rows
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 DOUBLE);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0(c0) VALUES(1E200), (0);"));
		REQUIRE_FAIL(con.Query("SELECT STDDEV_POP(c0) FROM t0;"));
	}
	SECTION("599") {
		// UPDATE results in crash or ASan error
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 INT, c1 VARCHAR);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0 VALUES (0, 0), (NULL, 0);"));
		REQUIRE_NO_FAIL(con.Query("UPDATE t0 SET c1 = c0;"));
		result = con.Query("SELECT * FROM t0 ORDER BY 1");
		REQUIRE(CHECK_COLUMN(result, 0, {Value(), 0}));
		REQUIRE(CHECK_COLUMN(result, 1, {Value(), "0"}));
	}
	SECTION("602") {
		// GROUP BY does not take COLLATE into account
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 VARCHAR COLLATE NOCASE);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0(c0) VALUES ('a'), ('A');"));
		result = con.Query("SELECT t0.c0 FROM t0 GROUP BY t0.c0;");
		REQUIRE(CHECK_COLUMN(result, 0, {"a"}));
	}
	SECTION("603") {
		// BETWEEN with COLLATE NOACCENT.NOCASE expression results in a segfault/ASan failure
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 DATE, c1 VARCHAR);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0(c0) VALUES (NULL), ('2000-01-01');"));
		result = con.Query("SELECT * FROM t0 WHERE 'a' BETWEEN c0 AND c1 COLLATE NOACCENT.NOCASE;");
		REQUIRE(CHECK_COLUMN(result, 0, {}));
	}
	SECTION("609") {
		// Incorrect result for MIN() on expression involving rowid
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 INT, c1 INT);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0(c0) VALUES (0), (0), (0), (0), (0), (0), (0), (0), (0), (0), (0), "
		                          "(0), (0), (0), (0), (0),  (0), (0), (0), (0), (0), (0), (NULL), (NULL);"));
		REQUIRE_NO_FAIL(con.Query("CREATE INDEX b ON t0(c1);"));
		REQUIRE_NO_FAIL(con.Query("UPDATE t0 SET c1 = NULL;"));
		result = con.Query("SELECT MIN(100000000000000000<<t0.rowid) FROM t0;");
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(-9223372036854775807LL - 1)}));
		result = con.Query("SELECT MIN(100000000000000000<<t0.rowid) FROM t0 WHERE NOT c0;");
		REQUIRE(CHECK_COLUMN(result, 0, {Value::BIGINT(-8802109549835190272LL)}));
	}
	SECTION("618") {
		// Failed ALTER COLUMN results in a "Transaction conflict" error that cannot be aborted
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 DATE);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0 VALUES (DATE '2000-01-01');"));
		REQUIRE_FAIL(con.Query("ALTER TABLE t0 ALTER COLUMN c0 SET DATA TYPE INT;"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0 VALUES (DEFAULT);"));
	}
	SECTION("619") {
		// Query on altered table results in a segmentation fault
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 INT UNIQUE, c1 DATE);"));
		REQUIRE_NO_FAIL(con.Query("ALTER TABLE t0 ALTER c1 TYPE INT;"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0(c0) VALUES(-1);"));

		result = con.Query("SELECT * FROM t0 WHERE c0 < 0;");
		REQUIRE(CHECK_COLUMN(result, 0, {-1}));
		REQUIRE(CHECK_COLUMN(result, 1, {Value()}));
	}
	SECTION("622") {
		// UPDATE on altered table results in an error "Could not find node in column segment tree"
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 TIMESTAMP);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0 VALUES(NULL);"));
		REQUIRE_NO_FAIL(con.Query("DELETE FROM t0;"));
		REQUIRE_NO_FAIL(con.Query("ALTER TABLE t0 ALTER c0 TYPE DATE;"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0 VALUES(NULL);"));
		REQUIRE_NO_FAIL(con.Query("UPDATE t0 SET c0 = '1969-12-18'; "));
	}
	SECTION("624") {
		// ALTER TABLE results in an assertion failure "Assertion `expr.return_type == vector.type' failed"
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 INT, c1 VARCHAR);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0(c1) VALUES(NULL);"));
		REQUIRE_NO_FAIL(con.Query("ALTER TABLE t0 ALTER c1 TYPE TIMESTAMP;"));
	}
	SECTION("625") {
		// DROP column results in an assertion failure unique.index < base.columns.size()
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 INT, c INT UNIQUE);"));
		// we don't support this case yet
		REQUIRE_FAIL(con.Query("ALTER TABLE t0 DROP c0;"));
		// check that unique constraint still works
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0 (c) VALUES (1);"));
		REQUIRE_FAIL(con.Query("INSERT INTO t0 (c) VALUES (1);"));
	}
	SECTION("628") {
		// DROP column results in an assertion failure unique.index < base.columns.size()
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 INT, c1 INT UNIQUE);"));
		REQUIRE_FAIL(con.Query("ALTER TABLE t0 DROP c1;"));
	}
	SECTION("629") {
		// ALTER TYPE with USING results in an assertion failure "types.size() > 0"
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 INT);"));
		REQUIRE_NO_FAIL(con.Query("ALTER TABLE t0 ALTER c0 TYPE VARCHAR USING ''; "));
	}
	SECTION("629") {
		// ALTER TYPE with USING results in an assertion failure "types.size() > 0"
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 INT);"));
		REQUIRE_NO_FAIL(con.Query("ALTER TABLE t0 ALTER c0 TYPE VARCHAR USING ''; "));
	}
	SECTION("633") {
		// Query using LEFT() results in a segmentation fault
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE t0(c0 BOOL);"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO t0(c0) VALUES (NULL);"));
		result = con.Query("SELECT LEFT(t0.c0, -1) FROM t0;");
		REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
		result = con.Query("SELECT RIGHT(t0.c0, -1) FROM t0;");
		REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
	}
}
