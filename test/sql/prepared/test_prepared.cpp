#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Inline PREPARE for SELECT", "[prepared]") {
	unique_ptr<DuckDBResult> result;
	DuckDB db(nullptr);

	DuckDBConnection con(db);

	REQUIRE_NO_FAIL(con.Query("PREPARE s1 AS SELECT CAST($1 AS INTEGER), CAST($2 AS STRING)"));
	result = con.Query("EXECUTE s1(42, 'dpfkg')");
	REQUIRE(result->GetSuccess());
	REQUIRE(CHECK_COLUMN(result, 0, {42}));
	REQUIRE(CHECK_COLUMN(result, 1, {"dpfkg"}));

	result = con.Query("EXECUTE s1(43, 'asdf')");
	REQUIRE(result->GetSuccess());
	REQUIRE(CHECK_COLUMN(result, 0, {43}));
	REQUIRE(CHECK_COLUMN(result, 1, {"asdf"}));

	// not enough params
	REQUIRE_FAIL(con.Query("EXECUTE s1(43)"));
	// too many
	REQUIRE_FAIL(con.Query("EXECUTE s1(43, 'asdf', 42)"));
	// wrong non-castable types
	REQUIRE_FAIL(con.Query("EXECUTE s1('asdf', 'asdf')"));

	REQUIRE_NO_FAIL(con.Query("DEALLOCATE s1"));

	// we can deallocate non-existing statements
	REQUIRE_NO_FAIL(con.Query("DEALLOCATE s2"));

	// now its gone
	REQUIRE_FAIL(con.Query("EXECUTE s1(42, 'dpfkg')"));

	REQUIRE_NO_FAIL(con.Query("CREATE TABLE a (i TINYINT)"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO a VALUES (42)"));
	REQUIRE_NO_FAIL(con.Query("PREPARE s3 AS SELECT * FROM a WHERE i=$1"));

	REQUIRE_FAIL(con.Query("EXECUTE s3(10000)"));

	result = con.Query("EXECUTE s3(42)");
	REQUIRE(result->GetSuccess());
	REQUIRE(CHECK_COLUMN(result, 0, {42}));

	result = con.Query("EXECUTE s3(84)");
	REQUIRE(result->GetSuccess());
	REQUIRE(CHECK_COLUMN(result, 0, {}));

	REQUIRE_NO_FAIL(con.Query("DEALLOCATE s3"));
}

// TODO NULL
// TODO not-set param throws err
/*

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
}*/
