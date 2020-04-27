#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test trigonometric function", "[function]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE numbers(n DOUBLE)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO numbers VALUES (-42),(-1),(0), (1), (42), (NULL)"));

	result = con.Query("SELECT cast(SIN(n::tinyint)*1000 as bigint) FROM numbers ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 916, -841, 0, 841, -916}));
	result = con.Query("SELECT cast(SIN(n::smallint)*1000 as bigint) FROM numbers ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 916, -841, 0, 841, -916}));
	result = con.Query("SELECT cast(SIN(n::integer)*1000 as bigint) FROM numbers ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 916, -841, 0, 841, -916}));
	result = con.Query("SELECT cast(SIN(n::bigint)*1000 as bigint) FROM numbers ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 916, -841, 0, 841, -916}));
	result = con.Query("SELECT cast(SIN(n::float)*1000 as bigint) FROM numbers ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 916, -841, 0, 841, -916}));
	result = con.Query("SELECT cast(SIN(n::double)*1000 as bigint) FROM numbers ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), 916, -841, 0, 841, -916}));

	result = con.Query("SELECT cast(COS(n::tinyint)*1000 as bigint) FROM numbers ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), -399, 540, 1000, 540, -399}));
	result = con.Query("SELECT cast(COS(n::smallint)*1000 as bigint) FROM numbers ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), -399, 540, 1000, 540, -399}));
	result = con.Query("SELECT cast(COS(n::integer)*1000 as bigint) FROM numbers ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), -399, 540, 1000, 540, -399}));
	result = con.Query("SELECT cast(COS(n::bigint)*1000 as bigint) FROM numbers ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), -399, 540, 1000, 540, -399}));
	result = con.Query("SELECT cast(COS(n::float)*1000 as bigint) FROM numbers ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), -399, 540, 1000, 540, -399}));
	result = con.Query("SELECT cast(COS(n::double)*1000 as bigint) FROM numbers ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), -399, 540, 1000, 540, -399}));

	result = con.Query("SELECT cast(TAN(n::tinyint)*1000 as bigint) FROM numbers ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), -2291, -1557, 0, 1557, 2291}));
	result = con.Query("SELECT cast(TAN(n::smallint)*1000 as bigint) FROM numbers ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), -2291, -1557, 0, 1557, 2291}));
	result = con.Query("SELECT cast(TAN(n::integer)*1000 as bigint) FROM numbers ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), -2291, -1557, 0, 1557, 2291}));
	result = con.Query("SELECT cast(TAN(n::bigint)*1000 as bigint) FROM numbers ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), -2291, -1557, 0, 1557, 2291}));
	result = con.Query("SELECT cast(TAN(n::float)*1000 as bigint) FROM numbers ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), -2291, -1557, 0, 1557, 2291}));
	result = con.Query("SELECT cast(TAN(n::double)*1000 as bigint) FROM numbers ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), -2291, -1557, 0, 1557, 2291}));

	result = con.Query("SELECT cast(ATAN(n::tinyint)*1000 as bigint) FROM numbers ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), -1546, -785, 0, 785, 1546}));
	result = con.Query("SELECT cast(ATAN(n::smallint)*1000 as bigint) FROM numbers ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), -1546, -785, 0, 785, 1546}));
	result = con.Query("SELECT cast(ATAN(n::integer)*1000 as bigint) FROM numbers ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), -1546, -785, 0, 785, 1546}));
	result = con.Query("SELECT cast(ATAN(n::bigint)*1000 as bigint) FROM numbers ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), -1546, -785, 0, 785, 1546}));
	result = con.Query("SELECT cast(ATAN(n::float)*1000 as bigint) FROM numbers ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), -1546, -785, 0, 785, 1546}));
	result = con.Query("SELECT cast(ATAN(n::double)*1000 as bigint) FROM numbers ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), -1546, -785, 0, 785, 1546}));

	result =
	    con.Query("SELECT cast(ASIN(n::tinyint)*1000 as bigint) FROM numbers  WHERE n between -1 and 1 ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {-1570, 0, 1570}));
	result =
	    con.Query("SELECT cast(ASIN(n::smallint)*1000 as bigint) FROM numbers  WHERE n between -1 and 1 ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {-1570, 0, 1570}));
	result =
	    con.Query("SELECT cast(ASIN(n::integer)*1000 as bigint) FROM numbers  WHERE n between -1 and 1 ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {-1570, 0, 1570}));
	result = con.Query("SELECT cast(ASIN(n::bigint)*1000 as bigint) FROM numbers  WHERE n between -1 and 1 ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {-1570, 0, 1570}));
	result = con.Query("SELECT cast(ASIN(n::float)*1000 as bigint) FROM numbers  WHERE n between -1 and 1 ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {-1570, 0, 1570}));
	result = con.Query("SELECT cast(ASIN(n::double)*1000 as bigint) FROM numbers  WHERE n between -1 and 1 ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {-1570, 0, 1570}));

	result =
	    con.Query("SELECT cast(ACOS(n::tinyint)*1000 as bigint) FROM numbers  WHERE n between -1 and 1 ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {3141, 1570, 0}));
	result =
	    con.Query("SELECT cast(ACOS(n::smallint)*1000 as bigint) FROM numbers  WHERE n between -1 and 1 ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {3141, 1570, 0}));
	result =
	    con.Query("SELECT cast(ACOS(n::integer)*1000 as bigint) FROM numbers  WHERE n between -1 and 1 ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {3141, 1570, 0}));
	result = con.Query("SELECT cast(ACOS(n::bigint)*1000 as bigint) FROM numbers  WHERE n between -1 and 1 ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {3141, 1570, 0}));
	result = con.Query("SELECT cast(ACOS(n::float)*1000 as bigint) FROM numbers  WHERE n between -1 and 1 ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {3141, 1570, 0}));
	result = con.Query("SELECT cast(ACOS(n::double)*1000 as bigint) FROM numbers  WHERE n between -1 and 1 ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {3141, 1570, 0}));

	REQUIRE_FAIL(con.Query("SELECT cast(ASIN(n)*1000 as bigint) FROM numbers ORDER BY n"));
	// REQUIRE_FAIL(con.Query("SELECT cast(ACOS(n)*1000 as bigint) FROM numbers ORDER BY n"));

	result =
	    con.Query("SELECT cast(COT(n::tinyint)*1000 as bigint) FROM numbers  WHERE n > 0.1 OR N < -0.1 ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {-436, -642, 642, 436}));
	result =
	    con.Query("SELECT cast(COT(n::smallint)*1000 as bigint) FROM numbers  WHERE n > 0.1 OR N < -0.1 ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {-436, -642, 642, 436}));
	result =
	    con.Query("SELECT cast(COT(n::integer)*1000 as bigint) FROM numbers  WHERE n > 0.1 OR N < -0.1 ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {-436, -642, 642, 436}));
	result = con.Query("SELECT cast(COT(n::bigint)*1000 as bigint) FROM numbers  WHERE n > 0.1 OR N < -0.1 ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {-436, -642, 642, 436}));
	result = con.Query("SELECT cast(COT(n::float)*1000 as bigint) FROM numbers  WHERE n > 0.1 OR N < -0.1 ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {-436, -642, 642, 436}));
	result = con.Query("SELECT cast(COT(n::double)*1000 as bigint) FROM numbers  WHERE n > 0.1 OR N < -0.1 ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {-436, -642, 642, 436}));

	result = con.Query("SELECT cast(ATAN2(n::tinyint, 42)*1000 as bigint) FROM numbers ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), -785, -23, 0, 23, 785}));
	result = con.Query("SELECT cast(ATAN2(n::smallint, 42)*1000 as bigint) FROM numbers ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), -785, -23, 0, 23, 785}));
	result = con.Query("SELECT cast(ATAN2(n::integer, 42)*1000 as bigint) FROM numbers ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), -785, -23, 0, 23, 785}));
	result = con.Query("SELECT cast(ATAN2(n::bigint, 42)*1000 as bigint) FROM numbers ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), -785, -23, 0, 23, 785}));
	result = con.Query("SELECT cast(ATAN2(n::float, 42)*1000 as bigint) FROM numbers ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), -785, -23, 0, 23, 785}));
	result = con.Query("SELECT cast(ATAN2(n::double, 42)*1000 as bigint) FROM numbers ORDER BY n");
	REQUIRE(CHECK_COLUMN(result, 0, {Value(), -785, -23, 0, 23, 785}));
}
