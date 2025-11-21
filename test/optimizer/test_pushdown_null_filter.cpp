#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb.hpp"
#include "duckdb/planner/operator/logical_get.hpp"

using namespace duckdb;

static TableFilterType ExtractFirstTableFilterTypeInQuery(duckdb::Connection& con, const std::string& query) {
	const auto plan = con.context->ExtractPlan(query);
	LogicalOperator *op = plan.get();
	// find the logical get:
	while (op->type != LogicalOperatorType::LOGICAL_GET) {
		REQUIRE(op->children.size() > 0);
		op = op->children[0].get();
	}
	REQUIRE(op->type == LogicalOperatorType::LOGICAL_GET);
	const auto &get = op->Cast<LogicalGet>();
	REQUIRE(!get.table_filters.filters.empty());
	return get.table_filters.filters.begin()->second->filter_type;
}

TEST_CASE("NullFilters are pushed down in TableFilters", "[optimizer][.]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE integers(i INTEGER);"));
	REQUIRE_NO_FAIL(con.Query("INSERT INTO integers VALUES (1), (NULL), (3), (NULL);"));
	SECTION("IS NULL filter") {
		REQUIRE(ExtractFirstTableFilterTypeInQuery(con, "SELECT * FROM integers WHERE i IS NULL;") == TableFilterType::IS_NULL);
	}
	SECTION("IS NOT NULL filter") {
		REQUIRE(ExtractFirstTableFilterTypeInQuery(con, "SELECT * FROM integers WHERE i IS NOT NULL;") == TableFilterType::IS_NOT_NULL);
	}
}
