#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/parser/tableref/column_data_ref.hpp"

using namespace duckdb;

TEST_CASE("Test a persisted view over materialized rows", "[api]") {
	auto db_path = TestCreatePath("persisted_view_over_rows.db");
	{
		DuckDB db(db_path);
		Connection con(db);

		REQUIRE_NO_FAIL(con.Query("create table tbl(a varchar);"));

		auto result = con.Query("insert into tbl values ('test') returning *");
		shared_ptr<ColumnDataCollection> collection = result->TakeCollection();
		auto rows = make_uniq<ColumnDataRef>(std::move(collection), result->GetNames());
		rows->alias = Identifier("vw");
		CreateViewOver(con, std::move(rows), "vw", true, false);

		result = con.Query("SELECT * FROM vw");
		REQUIRE(CHECK_COLUMN(result, 0, {"test"}));
	}
	// reset and query again
	{
		DuckDB db(db_path);
		Connection con(db);
		auto result = con.Query("SELECT * FROM vw");
		REQUIRE(CHECK_COLUMN(result, 0, {"test"}));
	}
}
