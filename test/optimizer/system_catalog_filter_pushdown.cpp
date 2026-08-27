#include "catch.hpp"
#include "duckdb/common/string_util.hpp"
#include "test_helpers.hpp"

namespace duckdb {

static string ExplainOptimized(Connection &con, const string &query) {
	REQUIRE_NO_FAIL(con.Query("PRAGMA explain_output='optimized_only'"));
	auto result = con.Query("EXPLAIN " + query);
	REQUIRE_NO_FAIL(*result);
	string plan;
	for (idx_t row = 0; row < result->RowCount(); row++) {
		for (idx_t column = 0; column < result->ColumnCount(); column++) {
			plan += result->GetValue(column, row).ToString();
		}
	}
	return plan;
}

static int64_t QueryCount(Connection &con, const string &query) {
	auto result = con.Query(query);
	REQUIRE_NO_FAIL(*result);
	return result->GetValue(0, 0).GetValue<int64_t>();
}

TEST_CASE("Metadata functions push a literal database name into the catalog scan", "[optimizer][filter_pushdown]") {
	DuckDB db(nullptr);
	Connection con(db);

	REQUIRE_NO_FAIL(con.Query("ATTACH ':memory:' AS db1"));
	REQUIRE_NO_FAIL(con.Query("ATTACH ':memory:' AS db2"));
	REQUIRE_NO_FAIL(con.Query("CREATE SCHEMA db1.s1"));
	REQUIRE_NO_FAIL(con.Query("CREATE SCHEMA db2.s2"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE db1.s1.t1(i INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE db2.s2.t2(j INTEGER)"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW db1.s1.v1 AS SELECT 1"));
	REQUIRE_NO_FAIL(con.Query("CREATE VIEW db2.s2.v2 AS SELECT 2"));

	const char *functions[] = {"duckdb_schemas", "duckdb_tables", "duckdb_views", "duckdb_columns"};
	for (auto function : functions) {
		auto plan =
		    ExplainOptimized(con, StringUtil::Format("SELECT * FROM %s() WHERE database_name = 'db1'", function));
		REQUIRE(StringUtil::Contains(plan, "Catalog: db1"));
		REQUIRE_FALSE(StringUtil::Contains(plan, "database_name = 'db1'"));
		auto empty_name_query = StringUtil::Format("SELECT count(*) FROM %s() WHERE database_name = ''", function);
		REQUIRE(QueryCount(con, empty_name_query) == 0);
	}

	auto reversed = ExplainOptimized(con, "SELECT * FROM duckdb_schemas() WHERE 'db1' = database_name");
	REQUIRE(StringUtil::Contains(reversed, "Catalog: db1"));

	auto unsupported = ExplainOptimized(con, "SELECT * FROM duckdb_schemas() WHERE lower(database_name) = 'db1'");
	REQUIRE_FALSE(StringUtil::Contains(unsupported, "Catalog: db1"));
	REQUIRE(StringUtil::Contains(unsupported, "database_name"));

	REQUIRE(QueryCount(con, "SELECT count(*) FROM duckdb_schemas() WHERE schema_name IN ('s1', 's2')") == 2);
	REQUIRE(QueryCount(con, "SELECT count(*) FROM duckdb_schemas() WHERE database_name = 'db1' "
	                        "AND schema_name = 's1'") == 1);
	REQUIRE(QueryCount(con, "SELECT count(*) FROM duckdb_tables() WHERE database_name = 'db1' "
	                        "AND table_name = 't1'") == 1);
	REQUIRE(QueryCount(con, "SELECT count(*) FROM duckdb_views() WHERE database_name = 'db1' "
	                        "AND view_name = 'v1'") == 1);
	REQUIRE(QueryCount(con, "SELECT count(*) FROM duckdb_columns() WHERE database_name = 'db1' "
	                        "AND table_name = 't1'") == 1);
	REQUIRE(QueryCount(con, "SELECT count(*) FROM duckdb_schemas() WHERE database_name = 'missing'") == 0);
	REQUIRE(QueryCount(con, "SELECT count(*) FROM duckdb_schemas() WHERE database_name = 'DB1'") == 0);
}

} // namespace duckdb
