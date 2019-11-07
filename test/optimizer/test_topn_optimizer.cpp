#include "catch.hpp"
#include "expression_helper.hpp"
#include "duckdb/planner/operator/logical_top_n.hpp"
#include "duckdb/optimizer/topn_optimizer.hpp"
#include "test_helpers.hpp"

using namespace duckdb;
using namespace std;

TEST_CASE("Test Top N optimization", "[topn]") {
	// LogicalTopN *topn;
	ExpressionHelper helper;
	helper.con.Query("CREATE TABLE integers(i INTEGER, j INTEGER)");
	auto tree = helper.ParseLogicalTree("SELECT i FROM integers ORDER BY i LIMIT 4");

	REQUIRE(tree->type == LogicalOperatorType::LIMIT);
	REQUIRE(tree->children[0]->type == LogicalOperatorType::ORDER_BY);

	TopN topn_optimizer;
	auto plan = topn_optimizer.Optimize(move(tree));

	// ORDER BY + LIMIT is now replaced by TOP N optimization
	REQUIRE(plan->type == LogicalOperatorType::TOP_N);

	// Same as above but with OFFSET
	tree = helper.ParseLogicalTree("SELECT i FROM integers ORDER BY i DESC LIMIT 4 OFFSET 5");

	REQUIRE(tree->type == LogicalOperatorType::LIMIT);
	REQUIRE(tree->children[0]->type == LogicalOperatorType::ORDER_BY);

	plan = topn_optimizer.Optimize(move(tree));
	REQUIRE(plan->type == LogicalOperatorType::TOP_N);

	// Same does not apply when OFFSET is present without LIMIT
	tree = helper.ParseLogicalTree("SELECT i FROM integers ORDER BY i OFFSET 5");

	REQUIRE(tree->type == LogicalOperatorType::LIMIT);
	REQUIRE(tree->children[0]->type == LogicalOperatorType::ORDER_BY);

	plan = topn_optimizer.Optimize(move(tree));
	REQUIRE(plan->type == LogicalOperatorType::LIMIT);
}
