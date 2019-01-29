#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test prepared statements", "[prepared]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);

	DuckDBConnection con(db);

	auto prep0 = con.PrepareStatement("SELECT cast(42 as integer)");
	result = prep0->Execute(con);
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
	{
		auto prep = con.PrepareStatement("SELECT cast($1 as integer)");
		prep->Bind(1, Value::INTEGER(42));
		result = prep->Execute(con);
		REQUIRE(CHECK_COLUMN(result, 0, {42}));

		prep->Bind(1, Value::INTEGER(84));
		result = prep->Execute(con);
		REQUIRE(CHECK_COLUMN(result, 0, {84}));
	}
	{
		auto prep = con.PrepareStatement("SELECT cast($1 as integer), cast($2 as string)");
		prep->Bind(1, Value::INTEGER(42));
		prep->Bind(2, Value("DPFKG"));

		result = prep->Execute(con);
		REQUIRE(CHECK_COLUMN(result, 0, {42}));
		REQUIRE(CHECK_COLUMN(result, 1, {"DPFKG"}));
	}
	{
		auto prep = con.PrepareStatement("SELECT cast($42 as integer)");

		REQUIRE_THROWS(prep->Bind(0, Value::INTEGER(42)));
		REQUIRE_THROWS(prep->Bind(1, Value::INTEGER(42)));

		prep->Bind(42, Value::INTEGER(42));

		result = prep->Execute(con);
		REQUIRE(CHECK_COLUMN(result, 0, {42}));
	}
	{
		auto prep = con.PrepareStatement("SELECT cast($1 as tinyint)");
		REQUIRE_THROWS(prep->Bind(1, Value::INTEGER(10000)));
		REQUIRE_NOTHROW(prep->Bind(1, Value::INTEGER(42)));
		result = prep->Execute(con);
		REQUIRE(CHECK_COLUMN(result, 0, {42}));
	}

	// TODO WAL pushing of prepared statements
	{
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE a (i TINYINT)"));
		REQUIRE_NO_FAIL(con.Query("INSERT INTO a VALUES (42)"));

		auto prep = con.PrepareStatement("SELECT * FROM a WHERE i=?");
		REQUIRE_THROWS(prep->Bind(0, Value::INTEGER(10000)));
		REQUIRE_NOTHROW(prep->Bind(0, Value::INTEGER(42)));
		result = prep->Execute(con);
		REQUIRE(CHECK_COLUMN(result, 0, {42}));

		prep->Bind(0, Value::INTEGER(84));
		result = prep->Execute(con);
		REQUIRE(CHECK_COLUMN(result, 0, {}));
	}
}
