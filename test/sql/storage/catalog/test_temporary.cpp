#include "catch.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test persistent temporary structures", "[catalog]") {
	unique_ptr<QueryResult> result;

	// verify that temporary tables are not written to disk
	FileSystem fs;
	string db_folder = TestCreatePath("temptbls");

	{
		DuckDB db_p(db_folder);
		Connection con_p(db_p);
		REQUIRE_NO_FAIL(con_p.Query("CREATE TEMPORARY TABLE temp.a (i INTEGER)"));
		REQUIRE_NO_FAIL(con_p.Query("INSERT INTO a VALUES (42)"));
		REQUIRE_NO_FAIL(con_p.Query("DELETE FROM a"));
		REQUIRE_NO_FAIL(con_p.Query("DELETE FROM temp.a"));
		REQUIRE_FAIL(con_p.Query("DELETE FROM asdf.a"));

		REQUIRE_NO_FAIL(con_p.Query("CREATE TEMPORARY SEQUENCE seq"));
		REQUIRE_NO_FAIL(con_p.Query("CREATE TEMPORARY SEQUENCE seq2"));
		REQUIRE_NO_FAIL(con_p.Query("DROP SEQUENCE seq2"));

		REQUIRE_NO_FAIL(con_p.Query("CREATE TEMPORARY VIEW v1 AS SELECT 42"));
		REQUIRE_NO_FAIL(con_p.Query("CREATE TEMPORARY VIEW v2 AS SELECT 42"));
		REQUIRE_NO_FAIL(con_p.Query("DROP VIEW v2"));

		REQUIRE_NO_FAIL(con_p.Query("INSERT INTO temp.a VALUES (43)"));

		REQUIRE_NO_FAIL(con_p.Query("UPDATE temp.a SET i = 44"));
		REQUIRE_NO_FAIL(con_p.Query("UPDATE a SET i = 45"));

		REQUIRE_NO_FAIL(con_p.Query("ALTER TABLE a RENAME COLUMN i TO k"));

		result = con_p.Query("SELECT COUNT(k) from a");
		REQUIRE(CHECK_COLUMN(result, 0, {1}));
	}

	{
		DuckDB db_p(db_folder);
		Connection con_p(db_p);
		REQUIRE_FAIL(con_p.Query("SELECT * FROM a"));
		REQUIRE_NO_FAIL(con_p.Query("CREATE TEMPORARY TABLE a (i INTEGER)"));
		REQUIRE_NO_FAIL(con_p.Query("CREATE TEMPORARY SEQUENCE seq"));
		REQUIRE_NO_FAIL(con_p.Query("CREATE TEMPORARY VIEW v1 AS SELECT 42"));

		result = con_p.Query("SELECT COUNT(*) from a");
		REQUIRE(CHECK_COLUMN(result, 0, {0}));
	}
}
