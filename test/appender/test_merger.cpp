#include "catch.hpp"
#include "duckdb/main/appender.hpp"
#include "test_helpers.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"

#include <vector>

using namespace duckdb;
using namespace std;

TEST_CASE("Basic merger tests", "[merger]") {
	duckdb::unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER PRIMARY KEY, j INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1, 16)"));

	result = con.Query("SELECT j FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {16}));

	Merger merger(con, "integers");
	merger.BeginRow();
	merger.Append<int32_t>(1);
	merger.Append<int32_t>(18);	
	merger.EndRow();

	merger.Flush();

	result = con.Query("SELECT j FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, {18}));
}

TEST_CASE("Test multiple AppendRow", "[merger]") {
	duckdb::unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER PRIMARY KEY, j INTEGER)"));

	// append a bunch of values
	{
		Appender appender(con, "integers");
		for (size_t i = 0; i < 2000; i++) {
		  appender.AppendRow((int32_t)(i), (int32_t)(i));
		}
		appender.Close();
	}

	// merge rows
	// change even key value
	duckdb::vector<duckdb::Value> values;
	{
		Merger merger(con, "integers");
		for (size_t i = 0; i < 2000; i++) {
		  if ((i % 2 ) == 0) {
		    merger.AppendRow( (int32_t)(i),  (int32_t)(2*i));
		    values.push_back(duckdb::Value((int32_t)(2*i)));
		  }
		  else {
		    values.push_back(duckdb::Value((int32_t)i));
		  }
		}
		merger.Close();
	}
	
	result = con.Query("SELECT j FROM integers");
	REQUIRE(CHECK_COLUMN(result, 0, values));

		
}
