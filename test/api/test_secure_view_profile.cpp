#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/query_profiler.hpp"

using namespace duckdb;

static void RequireSubtreeIsHidden(const QueryProfiler::TreeMap &tree_map, const PhysicalOperator &op) {
	for (auto &child : op.GetChildren()) {
		INFO("operator of the view still in the tree map: " << EnumUtil::ToString(child.get().type));
		REQUIRE(tree_map.find(child) == tree_map.end());
		RequireSubtreeIsHidden(tree_map, child.get());
	}
}

TEST_CASE("Test that the profiler tree map does not expose the operators of a secure view", "[api]") {
	DuckDB db(nullptr);
	Connection con(db);
	REQUIRE_NO_FAIL(con.Query("PRAGMA enable_profiling = 'no_output'"));
	REQUIRE_NO_FAIL(con.Query("SET profiling_coverage = 'ALL'"));
	REQUIRE_NO_FAIL(con.Query("CREATE TABLE hidden_table AS SELECT i, i % 10 AS j FROM range(10000) t(i)"));
	REQUIRE_NO_FAIL(con.Query("CREATE SECURE VIEW secure_view AS SELECT j, count(*) AS c FROM hidden_table "
	                          "WHERE j <> 7 GROUP BY j"));

	// the plan is freed by the last fetch of its result, so keep the result to read the profile of a live plan
	auto result = con.Submit("SELECT * FROM secure_view WHERE c > 0");
	REQUIRE(!result->HasError());

	auto &profiler = QueryProfiler::Get(*con.context);
	// collapsing the view onto its boundary node happens here, and frees the nodes of the operators it hides
	profiler.FinalizeMetrics();

	auto &tree_map = profiler.GetTreeMap();
	optional_ptr<const PhysicalOperator> secure_view;
	for (auto &entry : tree_map) {
		if (entry.first.get().type == PhysicalOperatorType::SECURE_VIEW) {
			secure_view = entry.first.get();
		}
	}
	// the boundary of the view is part of the plan and stays
	REQUIRE(secure_view);
	// what it reads is not: the tree map must not hand out what the collapsed tree hides
	RequireSubtreeIsHidden(tree_map, *secure_view);
}
