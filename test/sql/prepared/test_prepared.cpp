#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Inline PREPARE", "[prepared]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);

	DuckDBConnection con(db);

	REQUIRE_NO_FAIL(con.Query("PREPARE s1 AS SELECT 42"));
}

TEST_CASE("Test prepared statements for SELECT", "[prepared]") {
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

		// now we drop the table and execute should fail AND NOT CRASH
		REQUIRE_NO_FAIL(con.Query("DROP TABLE a"));
		// FIXME REQUIRE_THROWS(result = prep->Execute(con));
	}
}

TEST_CASE("Test prepared statements for INSERT", "[prepared]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);

	DuckDBConnection con(db);
	{
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE b (i TINYINT)"));
		auto prep = con.PrepareStatement("INSERT INTO b VALUES (cast($1 as tinyint)), ($2 + 1), ($3)");
		REQUIRE_NOTHROW(prep->Bind(1, Value::INTEGER(42)));
		REQUIRE_NOTHROW(prep->Bind(2, Value::INTEGER(41)));
		REQUIRE_NOTHROW(prep->Bind(3, Value::INTEGER(42)));
		REQUIRE_THROWS(prep->Bind(3, Value::INTEGER(10000)));

		result = prep->Execute(con);

		result = con.Query("SELECT * FROM b");
		REQUIRE(CHECK_COLUMN(result, 0, {42, 42, 42}));
	}

	{
		REQUIRE_NO_FAIL(con.Query("CREATE TABLE c (i INTEGER)"));
		auto prep = con.PrepareStatement("INSERT INTO c VALUES ($1)");

		for (size_t i = 0; i < 1000; i++) {
			REQUIRE_NOTHROW(prep->Bind(1, Value::INTEGER(i))->Execute(con));
		}

		result = con.Query("SELECT COUNT(*), MIN(i), MAX(i) FROM c");
		REQUIRE(CHECK_COLUMN(result, 0, {1000}));
		REQUIRE(CHECK_COLUMN(result, 1, {0}));
		REQUIRE(CHECK_COLUMN(result, 2, {999}));

		// now we drop the table and execute should fail
		REQUIRE_NO_FAIL(con.Query("DROP TABLE c"));
		// FIXME REQUIRE_THROWS(result = prep->Execute(con));
	}

	// TODO insert/update/delete
	// TODO WAL pushing of prepared statements
}
