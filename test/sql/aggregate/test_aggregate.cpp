#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/main/appender.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test BIT_AND operator", "[aggregate]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// test on scalar values
	result = con.Query("SELECT BIT_AND(3), BIT_AND(NULL)");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));
	REQUIRE(CHECK_COLUMN(result, 1, {-1}));

	// test on a sequence
	REQUIRE_NO_FAIL(con.Query("CREATE SEQUENCE seq;"));
	result = con.Query("SELECT BIT_AND(nextval('seq'))");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	result = con.Query("SELECT BIT_AND(nextval('seq'))");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));

	// test on a set of integers
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (3), (7), (15), (31), (3), (15)"));
	result = con.Query("SELECT BIT_AND(i), BIT_AND(1), BIT_AND(DISTINCT i), BIT_AND(NULL) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));
	REQUIRE(CHECK_COLUMN(result, 1, {1}));
	REQUIRE(CHECK_COLUMN(result, 2, {3}));
	REQUIRE(CHECK_COLUMN(result, 3, {-1}));

	// test on an empty set
	result = con.Query("SELECT BIT_AND(i) FROM integers WHERE i > 100");
	REQUIRE(CHECK_COLUMN(result, 0, {-1}));

	// test incorrect usage
	REQUIRE_FAIL(con.Query("SELECT BIT_AND()"));
	REQUIRE_FAIL(con.Query("SELECT BIT_AND(1, 2, 3)"));
	REQUIRE_FAIL(con.Query("SELECT BIT_AND(BIT_AND(1))"));
}

TEST_CASE("Test BIT_OR operator", "[aggregate]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// test on scalar values
	result = con.Query("SELECT BIT_OR(3), BIT_OR(NULL)");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));
	REQUIRE(CHECK_COLUMN(result, 1, {0}));

	// test on a sequence
	REQUIRE_NO_FAIL(con.Query("CREATE SEQUENCE seq;"));
	result = con.Query("SELECT BIT_OR(nextval('seq'))");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	result = con.Query("SELECT BIT_OR(nextval('seq'))");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));

	// test on a set of integers
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (3), (7), (15), (31), (3), (15)"));
	result = con.Query("SELECT BIT_OR(i), BIT_OR(1), BIT_OR(DISTINCT i), BIT_OR(NULL) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {31}));
	REQUIRE(CHECK_COLUMN(result, 1, {1}));
	REQUIRE(CHECK_COLUMN(result, 2, {31}));
	REQUIRE(CHECK_COLUMN(result, 3, {0}));

	// test on an empty set
	result = con.Query("SELECT BIT_OR(i) FROM integers WHERE i > 100");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));

	// test incorrect usage
	REQUIRE_FAIL(con.Query("SELECT BIT_OR()"));
	REQUIRE_FAIL(con.Query("SELECT BIT_OR(1, 2, 3)"));
	REQUIRE_FAIL(con.Query("SELECT BIT_OR(BIT_AND(1))"));
}

TEST_CASE("Test BIT_XOR operator", "[aggregate]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// test on scalar values
	result = con.Query("SELECT BIT_XOR(3), BIT_XOR(NULL)");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));
	REQUIRE(CHECK_COLUMN(result, 1, {0}));

	// test on a sequence
	REQUIRE_NO_FAIL(con.Query("CREATE SEQUENCE seq;"));
	result = con.Query("SELECT BIT_XOR(nextval('seq'))");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	result = con.Query("SELECT BIT_XOR(nextval('seq'))");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));

	// test on a set of integers
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (3), (7), (15), (31), (3), (15)"));
	result = con.Query("SELECT BIT_XOR(i), BIT_XOR(1), BIT_XOR(DISTINCT i), BIT_XOR(NULL) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {24}));
	REQUIRE(CHECK_COLUMN(result, 1, {0}));
	REQUIRE(CHECK_COLUMN(result, 2, {20}));
	REQUIRE(CHECK_COLUMN(result, 3, {0}));

	// test on an empty set
	result = con.Query("SELECT BIT_XOR(i) FROM integers WHERE i > 100");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));

	// test incorrect usage
	REQUIRE_FAIL(con.Query("SELECT BIT_XOR()"));
	REQUIRE_FAIL(con.Query("SELECT BIT_XOR(1, 2, 3)"));
	REQUIRE_FAIL(con.Query("SELECT BIT_XOR(BIT_XOR(1))"));
}

TEST_CASE("Test COUNT operator", "[aggregate]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// test counts on scalar values
	result = con.Query("SELECT COUNT(*), COUNT(1), COUNT(100), COUNT(NULL), COUNT(DISTINCT 1)");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {1}));
	REQUIRE(CHECK_COLUMN(result, 2, {1}));
	REQUIRE(CHECK_COLUMN(result, 3, {0}));
	REQUIRE(CHECK_COLUMN(result, 4, {1}));

	// test counts on a set of values
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (NULL)"));
	result = con.Query("SELECT COUNT(*), COUNT(1), COUNT(i), COUNT(COALESCE(i, 1)), COUNT(DISTINCT i), COUNT(DISTINCT "
	                   "1) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));
	REQUIRE(CHECK_COLUMN(result, 1, {3}));
	REQUIRE(CHECK_COLUMN(result, 2, {2}));
	REQUIRE(CHECK_COLUMN(result, 3, {3}));
	REQUIRE(CHECK_COLUMN(result, 4, {2}));
	REQUIRE(CHECK_COLUMN(result, 5, {1}));

	// ORDERED aggregates are not supported
	REQUIRE_FAIL(con.Query("SELECT COUNT(1 ORDER BY 1)"));
	// FILTER clause not supported
	REQUIRE_FAIL(con.Query("SELECT COUNT(1) FILTER (WHERE false)"));
	// cannot do DISTINCT *
	REQUIRE_FAIL(con.Query("SELECT COUNT(DISTINCT *) FROM integers"));
}

TEST_CASE("Test aggregates with scalar inputs", "[aggregate]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// test aggregate on scalar values
	result = con.Query("SELECT COUNT(1), MIN(1), FIRST(1), MAX(1), SUM(1), STRING_AGG('hello', ',')");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {1}));
	REQUIRE(CHECK_COLUMN(result, 2, {1}));
	REQUIRE(CHECK_COLUMN(result, 3, {1}));
	REQUIRE(CHECK_COLUMN(result, 4, {1}));
	REQUIRE(CHECK_COLUMN(result, 5, {"hello"}));

	// test aggregate on scalar NULLs
	result = con.Query("SELECT COUNT(NULL), MIN(NULL), FIRST(NULL), MAX(NULL), SUM(NULL), STRING_AGG(NULL, NULL)");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 3, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 4, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 5, {Value()}));

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (NULL)"));

	// test aggregates on a set of values with scalar inputs
	result = con.Query("SELECT COUNT(1), MIN(1), FIRST(1), MAX(1), SUM(1), STRING_AGG('hello', ',') FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));
	REQUIRE(CHECK_COLUMN(result, 1, {1}));
	REQUIRE(CHECK_COLUMN(result, 2, {1}));
	REQUIRE(CHECK_COLUMN(result, 3, {1}));
	REQUIRE(CHECK_COLUMN(result, 4, {3}));
	REQUIRE(CHECK_COLUMN(result, 5, {"hello,hello,hello"}));

	// test aggregates on a set of values with scalar NULL values as inputs
	result = con.Query(
	    "SELECT COUNT(NULL), MIN(NULL), FIRST(NULL), MAX(NULL), SUM(NULL), STRING_AGG(NULL, NULL) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 3, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 4, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 5, {Value()}));
}

TEST_CASE("Test COVAR operators", "[aggregate]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// test incorrect usage of COVAR_POP function
	REQUIRE_FAIL(con.Query("SELECT COVAR_POP()"));
	REQUIRE_FAIL(con.Query("SELECT COVAR_POP(1, 2, 3)"));
	REQUIRE_FAIL(con.Query("SELECT COVAR_POP(COVAR_POP(1))"));

	// test incorrect usage of COVAR_SAMP function
	REQUIRE_FAIL(con.Query("SELECT COVAR_SAMP()"));
	REQUIRE_FAIL(con.Query("SELECT COVAR_SAMP(1, 2, 3)"));
	REQUIRE_FAIL(con.Query("SELECT COVAR_SAMP(COVAR_SAMP(1))"));

	// test population covariance on scalar values
	result = con.Query("SELECT COVAR_POP(3,3), COVAR_POP(NULL,3), COVAR_POP(3,NULL), COVAR_POP(NULL,NULL)");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 3, {Value()}));

	// test sample covariance on scalar values
	result = con.Query("SELECT COVAR_SAMP(3,3), COVAR_SAMP(NULL,3), COVAR_SAMP(3,NULL), COVAR_SAMP(NULL,NULL)");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 3, {Value()}));

	// test population covariance on a sequence
	REQUIRE_NO_FAIL(con.Query("CREATE SEQUENCE seqx;"));
	REQUIRE_NO_FAIL(con.Query("CREATE SEQUENCE seqy;"));
	result = con.Query("SELECT COVAR_POP(nextval('seqx'),nextval('seqy'))");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
	result = con.Query("SELECT COVAR_POP(nextval('seqx'),nextval('seqy'))");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));

	// test population covariance on a set of values
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(x INTEGER, y INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (10,NULL), (10,11), (20,22), (25,NULL), (30,35)"));

	result = con.Query(
	    "SELECT COVAR_POP(x,y), COVAR_POP(x,1), COVAR_POP(1,y), COVAR_POP(x,NULL), COVAR_POP(NULL,y) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {80.0}));
	REQUIRE(CHECK_COLUMN(result, 1, {0.0}));
	REQUIRE(CHECK_COLUMN(result, 2, {0.0}));
	REQUIRE(CHECK_COLUMN(result, 3, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 4, {Value()}));

	result = con.Query("SELECT COVAR_SAMP(x,y), COVAR_SAMP(x,1), COVAR_SAMP(1,y), COVAR_SAMP(x,NULL), "
	                   "COVAR_SAMP(NULL,y) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {120.0}));
	REQUIRE(CHECK_COLUMN(result, 1, {0.0}));
	REQUIRE(CHECK_COLUMN(result, 2, {0.0}));
	REQUIRE(CHECK_COLUMN(result, 3, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 4, {Value()}));

	// test covar on empty set
	result = con.Query("SELECT COVAR_POP(x,y), COVAR_SAMP(x,y) FROM integers WHERE x > 100");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value()}));

	// test covar with only null inputs
	result = con.Query("SELECT COVAR_POP(NULL, NULL), COVAR_SAMP(NULL, NULL) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value()}));
}

TEST_CASE("Test STRING_AGG operator", "[aggregate]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// test incorrect usage of STRING_AGG function
	REQUIRE_FAIL(con.Query("SELECT STRING_AGG()"));
	REQUIRE_FAIL(con.Query("SELECT STRING_AGG(1)"));
	REQUIRE_FAIL(con.Query("SELECT STRING_AGG(1, 2, 3)"));
	REQUIRE_FAIL(con.Query("SELECT STRING_AGG(STRING_AGG('a',','))"));

	// test string aggregation on scalar values
	result = con.Query("SELECT STRING_AGG('a',',')");
	REQUIRE(CHECK_COLUMN(result, 0, {"a"}));

	// test string aggregation on scalar values
	result = con.Query("SELECT STRING_AGG('a',','), STRING_AGG(NULL,','), STRING_AGG('a',NULL), STRING_AGG(NULL,NULL)");
	REQUIRE(CHECK_COLUMN(result, 0, {"a"}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 3, {Value()}));

	// test string aggregation on a set of values
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE strings(g INTEGER, x VARCHAR, y VARCHAR);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO strings VALUES (1,'a','/'), (1,'b','-'), "
	                          "(2,'i','/'), (2,NULL,'-'), (2,'j','+'), "
	                          "(3,'p','/'), "
	                          "(4,'x','/'), (4,'y','-'), (4,'z','+')"));

	result = con.Query("SELECT STRING_AGG(x,','), STRING_AGG(x,y) FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {"a,b,i,j,p,x,y,z"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"a-b/i+j/p/x-y+z"}));

	result = con.Query("SELECT g, STRING_AGG(x,','), STRING_AGG(x,y) FROM strings GROUP BY g ORDER BY g");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3, 4}));
	REQUIRE(CHECK_COLUMN(result, 1, {"a,b", "i,j", "p", "x,y,z"}));
	REQUIRE(CHECK_COLUMN(result, 2, {"a-b", "i+j", "p", "x-y+z"}));

	// test agg on empty set
	result = con.Query("SELECT STRING_AGG(x,','), STRING_AGG(x,y) FROM strings WHERE g > 100");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value()}));

	// numerics are auto cast to strings
	result = con.Query("SELECT STRING_AGG(1, 2)");
	REQUIRE(CHECK_COLUMN(result, 0, {"1"}));
}

TEST_CASE("Test distinct STRING_AGG operator", "[aggregate]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE strings(s VARCHAR);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO strings VALUES ('a'), ('b'), ('a');"));

	result = con.Query("SELECT STRING_AGG(s,','), STRING_AGG(DISTINCT s, ',') FROM strings");
	REQUIRE(CHECK_COLUMN(result, 0, {"a,b,a"}));
	REQUIRE(CHECK_COLUMN(result, 1, {"a,b"}));
}

TEST_CASE("Test STRING_AGG operator with many groups", "[aggregate][.]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("BEGIN TRANSACTION;"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE strings(g INTEGER, x VARCHAR);"));
	vector<Value> expected_g, expected_h;
	string expected_large_value;
	Appender appender(con, "strings");
	for (idx_t i = 0; i < 10000; i++) {
		appender.AppendRow((int)i, "hello");
		expected_g.push_back(Value::INTEGER(i));
		expected_h.push_back(Value("hello"));
		expected_large_value += (i > 0 ? "," : "") + string("hello");
	}
	appender.Close();
	REQUIRE_NO_FAIL(con.Query("COMMIT;"));

	// many small groups
	result = con.Query("SELECT g, STRING_AGG(x, ',') FROM strings GROUP BY g ORDER BY g");
	REQUIRE(CHECK_COLUMN(result, 0, expected_g));
	REQUIRE(CHECK_COLUMN(result, 1, expected_h));

	// one begin group
	result = con.Query("SELECT 1, STRING_AGG(x, ',') FROM strings GROUP BY 1 ORDER BY 1");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(expected_large_value)}));

	// now test exception in the middle of an aggregate
	REQUIRE_FAIL(con.Query("SELECT STRING_AGG(k, ','), SUM(CAST(k AS BIGINT)) FROM (SELECT CAST(g AS VARCHAR) FROM "
	                       "strings UNION ALL SELECT CAST(x AS VARCHAR) FROM strings) tbl1(k)"));
}

TEST_CASE("STRING_AGG big", "[aggregate]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// test string aggregation on a set of values
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE strings(g VARCHAR, x VARCHAR);"));

	std::stringstream query_string;
	query_string << "INSERT INTO strings VALUES ";
	for (int c = 0; c < 100; ++c) {
		for (int e = 0; e < 100; ++e) {
			query_string << "(";

			query_string << c;

			query_string << ",";

			query_string << "'";
			query_string << c * 10 + e;
			query_string << "'";

			query_string << "),";
		}
	}
	std::string query_string_str = query_string.str();
	query_string_str.pop_back();

	REQUIRE_NO_FAIL(con.Query(query_string_str));

	result = con.Query("SELECT g, STRING_AGG(x,',') FROM strings GROUP BY g");
	REQUIRE_NO_FAIL(std::move(result));
}

TEST_CASE("Test AVG operator", "[aggregate]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// test average on a scalar value
	result = con.Query("SELECT AVG(3), AVG(NULL)");
	REQUIRE(CHECK_COLUMN(result, 0, {3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value()}));

	// test average on a sequence
	REQUIRE_NO_FAIL(con.Query("CREATE SEQUENCE seq;"));
	result = con.Query("SELECT AVG(nextval('seq'))");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));
	result = con.Query("SELECT AVG(nextval('seq'))");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));

	// test average on a set of values
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (2), (3)"));
	result = con.Query("SELECT AVG(i), AVG(1), AVG(DISTINCT i), AVG(NULL) FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {2}));
	REQUIRE(CHECK_COLUMN(result, 1, {1}));
	REQUIRE(CHECK_COLUMN(result, 2, {2}));
	REQUIRE(CHECK_COLUMN(result, 3, {Value()}));

	// test average on empty set
	result = con.Query("SELECT AVG(i) FROM integers WHERE i > 100");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));

	// test incorrect usage of AVG function
	REQUIRE_FAIL(con.Query("SELECT AVG()"));
	REQUIRE_FAIL(con.Query("SELECT AVG(1, 2, 3)"));
	REQUIRE_FAIL(con.Query("SELECT AVG(AVG(1))"));
}

TEST_CASE("Test implicit aggregate operators", "[aggregate]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// test implicit aggregates on empty set
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER);"));
	result = con.Query("SELECT COUNT(*), COUNT(i), STDDEV_SAMP(i), SUM(i), SUM(DISTINCT i), FIRST(i), MAX(i), MIN(i) "
	                   "FROM integers WHERE i > 100");
	REQUIRE(CHECK_COLUMN(result, 0, {0}));
	REQUIRE(CHECK_COLUMN(result, 1, {0}));
	REQUIRE(CHECK_COLUMN(result, 2, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 3, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 4, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 5, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 6, {Value()}));
	REQUIRE(CHECK_COLUMN(result, 7, {Value()}));
}

TEST_CASE("Test built in aggregate operator usage", "[aggregate]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	// test incorrect usage of the COUNT aggregate
	REQUIRE_FAIL(con.Query("SELECT COUNT(1, 2, 3)"));
	REQUIRE_FAIL(con.Query("SELECT COUNT(COUNT(1))"));

	// test incorrect usage of STDDEV_SAMP aggregate
	REQUIRE_FAIL(con.Query("SELECT STDDEV_SAMP()"));
	REQUIRE_FAIL(con.Query("SELECT STDDEV_SAMP(1, 2, 3)"));
	REQUIRE_FAIL(con.Query("SELECT STDDEV_SAMP(STDDEV_SAMP(1))"));

	// test incorrect usage of SUM aggregate
	REQUIRE_FAIL(con.Query("SELECT SUM()"));
	REQUIRE_FAIL(con.Query("SELECT SUM(1, 2, 3)"));
	REQUIRE_FAIL(con.Query("SELECT SUM(SUM(1))"));

	// test incorrect usage of FIRST aggregate
	REQUIRE_FAIL(con.Query("SELECT FIRST()"));
	REQUIRE_FAIL(con.Query("SELECT FIRST(1, 2, 3)"));
	REQUIRE_FAIL(con.Query("SELECT FIRST(FIRST(1))"));

	// test incorrect usage of MAX aggregate
	REQUIRE_FAIL(con.Query("SELECT MAX()"));
	REQUIRE_FAIL(con.Query("SELECT MAX(1, 2, 3)"));
	REQUIRE_FAIL(con.Query("SELECT MAX(MAX(1))"));

	// test incorrect usage of MIN aggregate
	REQUIRE_FAIL(con.Query("SELECT MIN()"));
	REQUIRE_FAIL(con.Query("SELECT MIN(1, 2, 3)"));
	REQUIRE_FAIL(con.Query("SELECT MIN(MIN(1))"));
}

TEST_CASE("Test GROUP BY on expression", "[aggregate]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integer(i INTEGER, j INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integer VALUES (3, 4), (3, 5), (3, 7);"));
	// group by on expression
	result = con.Query("SELECT j * 2 FROM integer GROUP BY j * 2 ORDER BY j * 2;");
	REQUIRE(CHECK_COLUMN(result, 0, {8, 10, 14}));
	// verify that adding or removing the table name does not impact the validity of the query
	result = con.Query("SELECT integer.j * 2 FROM integer GROUP BY j * 2 ORDER BY j * 2;");
	REQUIRE(CHECK_COLUMN(result, 0, {8, 10, 14}));
	result = con.Query("SELECT j * 2 FROM integer GROUP BY integer.j * 2 ORDER BY j * 2;");
	REQUIRE(CHECK_COLUMN(result, 0, {8, 10, 14}));
	result = con.Query("SELECT j * 2 FROM integer GROUP BY j * 2 ORDER BY integer.j * 2;");
	REQUIRE(CHECK_COLUMN(result, 0, {8, 10, 14}));
	result = con.Query("SELECT integer.j * 2 FROM integer GROUP BY j * 2 ORDER BY integer.j * 2;");
	REQUIRE(CHECK_COLUMN(result, 0, {8, 10, 14}));
	result = con.Query("SELECT j * 2 FROM integer GROUP BY integer.j * 2 ORDER BY integer.j * 2;");
	REQUIRE(CHECK_COLUMN(result, 0, {8, 10, 14}));
	result = con.Query("SELECT integer.j * 2 FROM integer GROUP BY integer.j * 2 ORDER BY j * 2;");
	REQUIRE(CHECK_COLUMN(result, 0, {8, 10, 14}));
	result = con.Query("SELECT integer.j * 2 FROM integer GROUP BY integer.j * 2 ORDER BY integer.j * 2;");
	REQUIRE(CHECK_COLUMN(result, 0, {8, 10, 14}));
	result = con.Query("SELECT j * 2 AS i FROM integer GROUP BY j * 2 ORDER BY i;");
	REQUIRE(CHECK_COLUMN(result, 0, {8, 10, 14}));
}

TEST_CASE("Test GROUP BY with many groups", "[aggregate][.]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER, j INTEGER);"));
	Appender appender(con, "integers");
	for (idx_t i = 0; i < 10000; i++) {
		appender.AppendRow((int)i, (int)1);
		appender.AppendRow((int)i, (int)2);
	}
	appender.Close();
	result = con.Query("SELECT SUM(i), SUM(sums) FROM (SELECT i, SUM(j) AS sums FROM integers GROUP BY i) tbl1");
	REQUIRE(CHECK_COLUMN(result, 0, {49995000}));
	REQUIRE(CHECK_COLUMN(result, 1, {30000}));
}

TEST_CASE("Test FIRST with non-inlined strings", "[aggregate]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE tbl(a INTEGER, b VARCHAR)"));
	REQUIRE_NO_FAIL(
	    con.Query("INSERT INTO tbl VALUES (1, NULL), (2, 'thisisalongstring'), (3, 'thisisalsoalongstring')"));

	// non-grouped aggregate
	result = con.Query("SELECT FIRST(b) FROM tbl WHERE a=2");
	REQUIRE(CHECK_COLUMN(result, 0, {"thisisalongstring"}));
	result = con.Query("SELECT FIRST(b) FROM tbl WHERE a=1");
	REQUIRE(CHECK_COLUMN(result, 0, {Value()}));

	// grouped aggregate
	result = con.Query("SELECT a, FIRST(b) FROM tbl GROUP BY a ORDER BY a");
	REQUIRE(CHECK_COLUMN(result, 0, {1, 2, 3}));
	REQUIRE(CHECK_COLUMN(result, 1, {Value(), "thisisalongstring", "thisisalsoalongstring"}));
}
