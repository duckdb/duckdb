#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test Row IDs", "[rowid]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("create table a(i integer);"));
	REQUIRE_NO_FAIL(con.Query("insert into a values (42), (44);"));

	// we can query row ids
	result = con.Query("SELECT rowid, * FROM a");
	REQUIRE(CHECK_COLUMN(result, 0, {0, 1}));
	REQUIRE(CHECK_COLUMN(result, 1, {42, 44}));
	REQUIRE(result->types.size() == 2);

	result = con.Query("SELECT rowid+1 FROM a WHERE CASE WHEN i=42 THEN rowid=0 ELSE rowid=1 END;");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));

	// rowid isn't expanded in *
	result = con.Query("SELECT * FROM a");
	REQUIRE(CHECK_COLUMN(result, 0, {42, 44}));
	REQUIRE(result->types.size() == 1);

	// we can't update rowids
	REQUIRE_FAIL(con.Query("UPDATE a SET rowid=5"));
	// we also can't insert with explicit row ids
	REQUIRE_FAIL(con.Query("INSERT INTO a (rowid, i)  VALUES (5, 6)"));

	// we can use rowid as column name
	REQUIRE_NO_FAIL(con.Query("create table b(rowid integer);"));
	REQUIRE_NO_FAIL(con.Query("insert into b values (42), (22);"));

	// this rowid is expanded
	result = con.Query("SELECT * FROM b ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {22, 42}));
	REQUIRE(result->types.size() == 1);

	// selecting rowid just selects the column
	result = con.Query("SELECT rowid FROM b ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {22, 42}));
	REQUIRE(result->types.size() == 1);
	// now we can update
	REQUIRE_NO_FAIL(con.Query("UPDATE b SET rowid=5"));
	// and insert
	REQUIRE_NO_FAIL(con.Query("INSERT INTO b (rowid) VALUES (5)"));

	result = con.Query("SELECT * FROM b");
	REQUIRE(CHECK_COLUMN(result, 0, {5, 5, 5}));
}

TEST_CASE("Test Row IDs used in different types of operations", "[rowid]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// test row ids on different operations
	// this is interesting because rowids are emitted as compressed vectors
	// hence this is really a test of correct handling of compressed vectors in the execution engine
	REQUIRE_NO_FAIL(con.Query("create table a(i integer);"));
	REQUIRE_NO_FAIL(con.Query("insert into a values (42);"));

	// arithmetic
	result = con.Query("SELECT rowid + 1, rowid - 1, rowid + rowid, i + rowid FROM a");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {-1}));
	REQUIRE(CHECK_COLUMN(result, 2, {0}));
	REQUIRE(CHECK_COLUMN(result, 3, {42}));

	// unary ops
	result = con.Query("SELECT -rowid, +rowid, abs(rowid) FROM a");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
	REQUIRE(CHECK_COLUMN(result, 1, {0}));
	REQUIRE(CHECK_COLUMN(result, 2, {0}));

	// ternary ops
	result = con.Query("SELECT rowid BETWEEN -1 AND 1, 0 BETWEEN rowid AND 1, 1 BETWEEN -3 AND rowid FROM a");
	REQUIRE(CHECK_COLUMN(result, 0, {true}));
	REQUIRE(CHECK_COLUMN(result, 1, {true}));
	REQUIRE(CHECK_COLUMN(result, 2, {false}));

	// comparisons
	result = con.Query("SELECT rowid < i, rowid = NULL, rowid = i, rowid <> 0 FROM a");
	REQUIRE(CHECK_COLUMN(result, 0, {true}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 2, {false}));
	REQUIRE(CHECK_COLUMN(result, 3, {false}));

	// simple (ungrouped) aggregates
	result = con.Query("SELECT SUM(rowid), MIN(rowid), MAX(rowid), COUNT(rowid), FIRST(rowid) FROM a");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
	REQUIRE(CHECK_COLUMN(result, 1, {0}));
	REQUIRE(CHECK_COLUMN(result, 2, {0}));
	REQUIRE(CHECK_COLUMN(result, 3, {1}));
	REQUIRE(CHECK_COLUMN(result, 4, {0}));

	result = con.Query("SELECT COUNT(*) FROM a");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));

	// grouped aggregates
	result = con.Query("SELECT SUM(rowid), MIN(rowid), MAX(rowid), COUNT(rowid), FIRST(rowid) FROM a GROUP BY i");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
	REQUIRE(CHECK_COLUMN(result, 1, {0}));
	REQUIRE(CHECK_COLUMN(result, 2, {0}));
	REQUIRE(CHECK_COLUMN(result, 3, {1}));
	REQUIRE(CHECK_COLUMN(result, 4, {0}));

	// group by rowid
	result = con.Query("SELECT SUM(i) FROM a GROUP BY rowid");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));

	// joins
	// equality
	result = con.Query("SELECT * FROM a, a a2 WHERE a.rowid=a2.rowid");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
	// inequality
	result = con.Query("SELECT * FROM a, a a2 WHERE a.rowid<>a2.rowid");
	REQUIRE(CHECK_COLUMN(result, 0, {}));
	// range
	result = con.Query("SELECT * FROM a, a a2 WHERE a.rowid>=a2.rowid");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));

	// order by
	result = con.Query("SELECT * FROM a ORDER BY rowid");
	REQUIRE(CHECK_COLUMN(result, 0, {42}));

	// insert into table
	REQUIRE_NO_FAIL(con.Query("INSERT INTO a SELECT rowid FROM a"));
	result = con.Query("SELECT * FROM a ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {0, 42}));

	// update value
	REQUIRE_NO_FAIL(con.Query("UPDATE a SET i=rowid"));
	result = con.Query("SELECT * FROM a ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {0, 1}));

	// use rowid in filter
	result = con.Query("SELECT * FROM a WHERE rowid=0");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
	result = con.Query("SELECT * FROM a WHERE rowid BETWEEN -100 AND 100 ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {0, 1}));
	result = con.Query("SELECT * FROM a WHERE rowid=0 OR rowid=1");
	REQUIRE(CHECK_COLUMN(result, 0, {0, 1}));

	// window expressions
	result = con.Query("SELECT row_number() OVER (PARTITION BY rowid) FROM a ORDER BY rowid");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 1}));
	result = con.Query("SELECT row_number() OVER (ORDER BY rowid) FROM a ORDER BY rowid");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2}));
	result = con.Query("SELECT row_number() OVER (ORDER BY rowid DESC) FROM a ORDER BY rowid");
	REQUIRE(CHECK_COLUMN(result, 0, {2, 1}));

	// uncorrelated subqueries
	result = con.Query("SELECT (SELECT rowid FROM a LIMIT 1)");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
	result = con.Query("SELECT 0 IN (SELECT rowid FROM a)");
	REQUIRE(CHECK_COLUMN(result, 0, {true}));
	result = con.Query("SELECT EXISTS(SELECT rowid FROM a)");
	REQUIRE(CHECK_COLUMN(result, 0, {true}));

	// correlated subqueries
	result = con.Query("SELECT (SELECT a2.rowid FROM a a2 WHERE a.rowid=a2.rowid) FROM a");
	REQUIRE(CHECK_COLUMN(result, 0, {0, 1}));
	result = con.Query("SELECT a.rowid IN (SELECT a2.rowid FROM a a2 WHERE a.rowid>=a2.rowid) FROM a");
	REQUIRE(CHECK_COLUMN(result, 0, {true, true}));
	result = con.Query("SELECT EXISTS(SELECT a2.rowid FROM a a2 WHERE a.rowid>=a2.rowid) FROM a");
	REQUIRE(CHECK_COLUMN(result, 0, {true, true}));
}
