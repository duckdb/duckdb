
#include "catch.hpp"

#include <vector>

#include "optimizer/rewriter.hpp"

#include "common/helper.hpp"
#include "optimizer/logical_rules/rule_list.hpp"
#include "expression_helper.hpp"

#include "duckdb.hpp"

using namespace duckdb;
using namespace std;

bool FindLogicalNode(LogicalOperator* op, LogicalOperatorType type) {
	if (op->type == type) {
		return true;
	}
	for(auto &child : op->children) {
		if (FindLogicalNode(child.get(), type)) {
			return true;
		}
	}
	return false;
}

// Rewrite subquery with correlated equality in WHERE to INNER JOIN
TEST_CASE("Subquery rewriting", "[subquery_rewrite]") {
	DuckDB db(nullptr);
	DuckDBConnection con(db);
	con.Query("BEGIN TRANSACTION");
	con.Query("CREATE TABLE t1 (a INTEGER, b INTEGER)");
	con.Query("INSERT INTO t1 VALUES (1, 10), (2, 20), (3, 30), (3, 42)");

	auto planner = ParseLogicalPlan(con, "SELECT t1.a, t1.b FROM t1 WHERE b = (SELECT MIN(b) "
	                       "FROM t1 ts WHERE t1.a=ts.a)");

	Rewriter rewriter(*planner->context);
	rewriter.rules.push_back(make_unique_base<Rule, SubqueryRewritingRule>());
	auto plan = rewriter.ApplyRules(move(planner->plan));
	// now we expect a subquery
	REQUIRE(FindLogicalNode(plan.get(), LogicalOperatorType::SUBQUERY));
};
