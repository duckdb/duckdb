#include "catch.hpp"
#include "duckdb/common/file_system.hpp"
#include "dbgen.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test not equals join", "[join]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("create table a (i integer)"));
	REQUIRE_NO_FAIL(con.Query(
	    "insert into a values "
	    "('28579'),('16098'),('25281'),('28877'),('18048'),('26820'),('26971'),('22812'),('11757'),('21851'),('27752'),"
	    "('28354'),('29843'),('28828'),('16668'),('20534'),('28222'),('24244'),('28877'),('20150'),('23451'),('23683'),"
	    "('20419'),('28048'),('24244'),('28605'),('25752'),('24466'),('26557'),('16098'),('29454'),('24854'),('13298'),"
	    "('29584'),('13394'),('24843'),('22477'),('14593'),('24244'),('28722'),('25124'),('16668'),('26787'),('28877'),"
	    "('27752'),('28482'),('24408'),('25752'),('24136'),('28222'),('17683'),('24244'),('19275'),('21087'),('26594'),"
	    "('22293'),('25281'),('12898'),('23451'),('12898'),('21757'),('20965'),('25709'),('26614'),('10399'),('28773'),"
	    "('11933'),('29584'),('29003'),('26871'),('17746'),('24092'),('26192'),('19310'),('10965'),('29275'),('20191'),"
	    "('29101'),('28059'),('29584'),('20399'),('24338'),('26192'),('25124'),('28605'),('13003'),('16668'),('23511'),"
	    "('26534'),('24107')"));

	REQUIRE_NO_FAIL(con.Query("create table b (j integer)"));
	REQUIRE_NO_FAIL(con.Query("insert into b values "
	                          "('31904'),('31904'),('31904'),('31904'),('35709'),('31904'),('31904'),('35709'),('31904'"
	                          "),('31904'),('31904'),('31904')"));

	result = con.Query("select count(*) from a,b where i <> j");
	REQUIRE(CHECK_COLUMN(result, 0, {1080}));
}

TEST_CASE("Test less than join", "[join][.]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	REQUIRE_NO_FAIL(con.Query("create table a (i integer)"));
	for (idx_t i = 0; i < 2000; i++) {
		REQUIRE_NO_FAIL(con.Query("insert into a values ($1)", (int32_t)i + 1));
	}

	result = con.Query("select count(*) from a, (SELECT 2000 AS j) b where i < j");
	REQUIRE(CHECK_COLUMN(result, 0, {1999}));

	result = con.Query("select count(*) from a, (SELECT 2000 AS j) b where i <= j");
	REQUIRE(CHECK_COLUMN(result, 0, {2000}));

	result = con.Query("select count(*) from a, (SELECT 1 AS j) b where i > j");
	REQUIRE(CHECK_COLUMN(result, 0, {1999}));

	result = con.Query("select count(*) from a, (SELECT 1 AS j) b where i >= j");
	REQUIRE(CHECK_COLUMN(result, 0, {2000}));
}

TEST_CASE("Test joins with different types", "[join]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// numeric types
	vector<string> numeric_types = {"tinyint", "smallint", "integer", "bigint", "real", "double"};
	for (auto &type : numeric_types) {
		REQUIRE_NO_FAIL(con.Query("begin transaction"));
		REQUIRE_NO_FAIL(con.Query("create table a (i " + type + ")"));
		for (idx_t i = 0; i < 100; i++) {
			REQUIRE_NO_FAIL(con.Query("insert into a values ($1)", (int32_t)i + 1));
		}
		// range joins
		result = con.Query("select count(*), sum(i) from a, (SELECT 100::" + type + " AS j) b where i < j");
		REQUIRE(CHECK_COLUMN(result, 0, {99}));
		REQUIRE(CHECK_COLUMN(result, 1, {4950}));
		result = con.Query("select count(*) from a, (SELECT 100::" + type + " AS j) b where i <= j");
		REQUIRE(CHECK_COLUMN(result, 0, {100}));
		result = con.Query("select count(*) from a, (SELECT 1::" + type + " AS j) b where i > j");
		REQUIRE(CHECK_COLUMN(result, 0, {99}));
		result = con.Query("select count(*) from a, (SELECT 1::" + type + " AS j) b where i >= j");
		REQUIRE(CHECK_COLUMN(result, 0, {100}));
		// inequality join
		result = con.Query("select count(*) from a, (SELECT 1::" + type + " AS j) b where i <> j");
		REQUIRE(CHECK_COLUMN(result, 0, {99}));
		// equality join
		result = con.Query("select count(*) from a, (SELECT 1::" + type + " AS j) b where i = j");
		REQUIRE(CHECK_COLUMN(result, 0, {1}));
		// no results on one side
		result = con.Query("select count(*) from a, (SELECT 1::" + type + " AS j) b where i > j AND i>1000");
		REQUIRE(CHECK_COLUMN(result, 0, {0}));
		result = con.Query("select count(*) from a, (SELECT 1::" + type + " AS j) b where i <> j AND i>1000");
		REQUIRE(CHECK_COLUMN(result, 0, {0}));
		result = con.Query("select count(*) from a, (SELECT 1::" + type + " AS j) b where i = j AND i>1000");
		REQUIRE(CHECK_COLUMN(result, 0, {0}));

		REQUIRE_NO_FAIL(con.Query("rollback"));
	}
	// strings
	REQUIRE_NO_FAIL(con.Query("begin transaction"));
	REQUIRE_NO_FAIL(con.Query("create table a (i VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("insert into a values ('a'), ('b'), ('c'), ('d'), ('e'), ('f')"));

	// range joins
	result = con.Query("select count(*) from a, (SELECT 'f' AS j) b where i < j");
	REQUIRE(CHECK_COLUMN(result, 0, {5}));
	result = con.Query("select count(*) from a, (SELECT 'f' AS j) b where i <= j");
	REQUIRE(CHECK_COLUMN(result, 0, {6}));
	result = con.Query("select count(*) from a, (SELECT 'a' AS j) b where i > j");
	REQUIRE(CHECK_COLUMN(result, 0, {5}));
	result = con.Query("select count(*) from a, (SELECT 'a' AS j) b where i >= j");
	REQUIRE(CHECK_COLUMN(result, 0, {6}));
	result = con.Query("select count(*) from a, (SELECT 'a' AS j) b where i <> j");
	REQUIRE(CHECK_COLUMN(result, 0, {5}));
	result = con.Query("select count(*) from a, (SELECT 'a' AS j) b where i = j");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));

	REQUIRE_NO_FAIL(con.Query("rollback"));
}

TEST_CASE("Test mark join with different types", "[join]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// numeric types
	vector<string> numeric_types = {"tinyint", "smallint", "integer", "bigint", "real", "double"};
	for (auto &type : numeric_types) {
		REQUIRE_NO_FAIL(con.Query("begin transaction"));
		REQUIRE_NO_FAIL(con.Query("create table a (i " + type + ")"));
		std::vector<int32_t> values;
		for (idx_t i = 0; i < 100; i++) {
			values.push_back(i + 1);
		}
		std::random_shuffle(values.begin(), values.end());
		for (idx_t i = 0; i < values.size(); i++) {
			REQUIRE_NO_FAIL(con.Query("insert into a values ($1)", values[i]));
		}

		// range joins
		result = con.Query("select count(*) from a WHERE i > ANY((SELECT 1::" + type + "))");
		REQUIRE(CHECK_COLUMN(result, 0, {99}));
		result = con.Query("select count(*) from a WHERE i >= ANY((SELECT 1::" + type + "))");
		REQUIRE(CHECK_COLUMN(result, 0, {100}));
		result = con.Query("select count(*) from a WHERE i < ANY((SELECT 100::" + type + "))");
		REQUIRE(CHECK_COLUMN(result, 0, {99}));
		result = con.Query("select count(*) from a WHERE i <= ANY((SELECT 100::" + type + "))");
		REQUIRE(CHECK_COLUMN(result, 0, {100}));
		result = con.Query("select count(*) from a WHERE i = ANY((SELECT 1::" + type + "))");
		REQUIRE(CHECK_COLUMN(result, 0, {1}));
		result = con.Query("select count(*) from a WHERE i <> ANY((SELECT 1::" + type + "))");
		REQUIRE(CHECK_COLUMN(result, 0, {99}));

		// now with a filter
		result = con.Query("select count(*) from (select * from a where i % 2 = 0) a WHERE i > ANY((SELECT 2::" + type +
		                   "))");
		REQUIRE(CHECK_COLUMN(result, 0, {49}));
		result = con.Query(
		    "select count(*) from (select * from a where i % 2 = 0) a WHERE i >= ANY((SELECT 2::" + type + "))");
		REQUIRE(CHECK_COLUMN(result, 0, {50}));
		result = con.Query(
		    "select count(*) from (select * from a where i % 2 = 0) a WHERE i < ANY((SELECT 100::" + type + "))");
		REQUIRE(CHECK_COLUMN(result, 0, {49}));
		result = con.Query(
		    "select count(*) from (select * from a where i % 2 = 0) a WHERE i <= ANY((SELECT 100::" + type + "))");
		REQUIRE(CHECK_COLUMN(result, 0, {50}));
		result = con.Query("select * from (select * from a where i % 2 = 0) a WHERE i = ANY((SELECT 2::" + type + "))");
		REQUIRE(CHECK_COLUMN(result, 0, {2}));
		result = con.Query(
		    "select count(*) from (select * from a where i % 2 = 0) a WHERE i <> ANY((SELECT 2::" + type + "))");
		REQUIRE(CHECK_COLUMN(result, 0, {49}));

		// now select the actual values, instead of only the count
		result = con.Query("select * from (select * from a where i % 2 = 0) a WHERE i <= ANY((SELECT 10::" + type +
		                   ")) ORDER BY 1");
		REQUIRE(CHECK_COLUMN(result, 0, {2, 4, 6, 8, 10}));
		result = con.Query("select * from (select * from a where i % 2 = 0) a WHERE i >= ANY((SELECT 90::" + type +
		                   ")) ORDER BY 1");
		REQUIRE(CHECK_COLUMN(result, 0, {90, 92, 94, 96, 98, 100}));
		result = con.Query("select * from (select * from a where i > 90) a WHERE i <> ANY((SELECT 96::" + type +
		                   ")) ORDER BY 1");
		REQUIRE(CHECK_COLUMN(result, 0, {91, 92, 93, 94, 95, 97, 98, 99, 100}));

		REQUIRE_NO_FAIL(con.Query("rollback"));
	}
	// strings
	REQUIRE_NO_FAIL(con.Query("begin transaction"));
	REQUIRE_NO_FAIL(con.Query("create table a (i VARCHAR)"));
	REQUIRE_NO_FAIL(con.Query("insert into a values ('a'), ('b'), ('c'), ('d'), ('e'), ('f')"));

	// range joins
	result = con.Query("select count(*) from a WHERE i < ANY((SELECT 'f'))");
	REQUIRE(CHECK_COLUMN(result, 0, {5}));
	result = con.Query("select count(*) from a WHERE i <= ANY((SELECT 'f' AS j))");
	REQUIRE(CHECK_COLUMN(result, 0, {6}));
	result = con.Query("select count(*) from a WHERE i > ANY((SELECT 'a'))");
	REQUIRE(CHECK_COLUMN(result, 0, {5}));
	result = con.Query("select count(*) from a WHERE i >= ANY((SELECT 'a'))");
	REQUIRE(CHECK_COLUMN(result, 0, {6}));
	result = con.Query("select count(*) from a WHERE i <> ANY((SELECT 'a'))");
	REQUIRE(CHECK_COLUMN(result, 0, {5}));
	result = con.Query("select count(*) from a WHERE i = ANY((SELECT 'a'))");
	REQUIRE(CHECK_COLUMN(result, 0, {1}));

	REQUIRE_NO_FAIL(con.Query("rollback"));
}
