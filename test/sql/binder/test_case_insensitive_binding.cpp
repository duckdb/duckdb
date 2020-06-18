#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test case insensitive binding of columns", "[binder]") {
	unique_ptr<QueryResult> result;
	DuckDB db(nullptr);
	Connection con(db);
	con.EnableQueryVerification();

	// we can bind case insensitive column names
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test (\"HeLlO\" INTEGER)"));

	// lowercase names are aliased
	REQUIRE_NO_FAIL(con.Query("SELECT HeLlO FROM test"));
	REQUIRE_NO_FAIL(con.Query("SELECT hello FROM test"));
	REQUIRE_NO_FAIL(con.Query("SELECT \"HeLlO\" FROM test"));
	// specifying a different, non-lower, case does fail!
	REQUIRE_FAIL(con.Query("SELECT \"HELLO\" FROM test"));
	REQUIRE_FAIL(con.Query("SELECT \"HELLo\" FROM test"));

	REQUIRE_NO_FAIL(con.Query("SELECT test.HeLlO FROM test"));
	REQUIRE_NO_FAIL(con.Query("SELECT test.hello FROM test"));
	REQUIRE_NO_FAIL(con.Query("SELECT test.\"HeLlO\" FROM test"));
	REQUIRE_FAIL(con.Query("SELECT test.\"HELLO\" FROM test"));
	REQUIRE_FAIL(con.Query("SELECT test.\"HELLo\" FROM test"));

	REQUIRE_NO_FAIL(con.Query("UPDATE test SET hello=3"));
	REQUIRE_NO_FAIL(con.Query("UPDATE test SET HeLlO=3"));

	// but ONLY if there are no conflicts!
	// if the reference is ambiguous (e.g. hello -> HeLlO, HELLO) the name must match exactly
	REQUIRE_NO_FAIL(con.Query("DROP TABLE test"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test(\"HeLlO\" INTEGER, \"HELLO\" INTEGER)"));

	REQUIRE_FAIL(con.Query("SELECT HeLlO FROM test"));
	REQUIRE_FAIL(con.Query("SELECT hello FROM test"));
	REQUIRE_NO_FAIL(con.Query("SELECT \"HeLlO\" FROM test"));
	REQUIRE_NO_FAIL(con.Query("SELECT \"HELLO\" FROM test"));
	REQUIRE_FAIL(con.Query("SELECT \"HELLo\" FROM test"));
	REQUIRE_FAIL(con.Query("UPDATE test SET hello = 3"));
	REQUIRE_NO_FAIL(con.Query("UPDATE test SET \"HeLlO\" = 3"));
	REQUIRE_NO_FAIL(con.Query("UPDATE test SET \"HELLO\" = 3"));

	REQUIRE_FAIL(con.Query("SELECT test.HeLlO FROM test"));
	REQUIRE_FAIL(con.Query("SELECT test.hello FROM test"));
	REQUIRE_NO_FAIL(con.Query("SELECT test.\"HeLlO\" FROM test"));
	REQUIRE_NO_FAIL(con.Query("SELECT test.\"HELLO\" FROM test"));
	REQUIRE_FAIL(con.Query("SELECT test.\"HELLo\" FROM test"));

	// conflicts can also come from different sources!
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test1(\"HeLlO\" INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE test2(\"HELLO\" INTEGER)"));

	REQUIRE_FAIL(con.Query("SELECT HeLlO FROM test1, test2"));
	REQUIRE_FAIL(con.Query("SELECT hello FROM test1, test2"));
	REQUIRE_NO_FAIL(con.Query("SELECT \"HeLlO\" FROM test1, test2"));
	REQUIRE_NO_FAIL(con.Query("SELECT \"HELLO\" FROM test1, test2"));
	REQUIRE_FAIL(con.Query("SELECT \"HELLo\" FROM test1, test2"));

	// in this case we can eliminate the conflict by specifically selecting the source
	REQUIRE_NO_FAIL(con.Query("SELECT test1.HeLlO FROM test1, test2"));
	REQUIRE_NO_FAIL(con.Query("SELECT test1.hello FROM test1, test2"));
	REQUIRE_NO_FAIL(con.Query("SELECT test1.\"HeLlO\" FROM test1, test2"));
	REQUIRE_FAIL(con.Query("SELECT test1.\"HELLO\" FROM test1, test2"));
	REQUIRE_FAIL(con.Query("SELECT test1.\"HELLo\" FROM test1, test2"));

	REQUIRE_NO_FAIL(con.Query("SELECT test2.HeLlO FROM test1, test2"));
	REQUIRE_NO_FAIL(con.Query("SELECT test2.hello FROM test1, test2"));
	REQUIRE_FAIL(con.Query("SELECT test2.\"HeLlO\" FROM test1, test2"));
	REQUIRE_NO_FAIL(con.Query("SELECT test2.\"HELLO\" FROM test1, test2"));
	REQUIRE_FAIL(con.Query("SELECT test2.\"HELLo\" FROM test1, test2"));

	REQUIRE_NO_FAIL(con.Query("SELECT * FROM test1 JOIN test2 USING (hello)"));

	REQUIRE_NO_FAIL(con.Query("SELECT hello FROM (SELECT 42) tbl(\"HeLlO\")"));
}
