// #include "catch.hpp"
// #include "common/helper.hpp"
// #include "duckdb.hpp"
// #include "expression_helper.hpp"
// // #include "optimizer/logical_rules/list.hpp"
// // #include "optimizer/rewriter.hpp"
// #include "optimizer/subquery_rewriter.hpp"

// #include <vector>

// using namespace duckdb;
// using namespace std;

// bool FindLogicalNode(LogicalOperator *op, LogicalOperatorType type) {
// 	if (op->type == type) {
// 		return true;
// 	}
// 	for (auto &child : op->children) {
// 		if (FindLogicalNode(child.get(), type)) {
// 			return true;
// 		}
// 	}
// 	return false;
// }

// // Rewrite subquery with correlated equality in WHERE to INNER JOIN
// TEST_CASE("Subquery rewriting", "[subquery_rewrite]") {
// 	DuckDB db(nullptr);
// 	DuckDBConnection con(db);
// 	con.Query("BEGIN TRANSACTION");
// 	con.Query("CREATE TABLE t1 (a INTEGER, b INTEGER)");
// 	con.Query("INSERT INTO t1 VALUES (1, 10), (2, 20), (3, 30), (3, 42)");

// 	auto planner = ParseLogicalPlan(con, "SELECT t1.a, t1.b FROM t1 WHERE b = (SELECT MIN(b) "
// 	                                     "FROM t1 ts WHERE t1.a=ts.a)");
// 	SubqueryRewriter rewriter(*planner->context);
// 	auto plan = rewriter.Rewrite(move(planner->plan));
// 	// now we expect the subquery to be flattened
// 	REQUIRE(FindLogicalNode(plan.get(), LogicalOperatorType::SUBQUERY));
// };

// // Rewrite subquery with (NOT) IN clause to semi/anti join
// TEST_CASE("(NOT) IN clause rewriting", "[subquery_rewrite]") {
// 	DuckDB db(nullptr);
// 	DuckDBConnection con(db);
// 	con.Query("BEGIN TRANSACTION");
// 	con.Query("CREATE TABLE t1 (a INTEGER, b INTEGER)");
// 	con.Query("INSERT INTO t1 VALUES (1, 10), (2, 20), (3, 30), (3, 42)");

// 	// anti join
// 	{
// 		auto planner = ParseLogicalPlan(con, "SELECT t1.a, t1.b FROM t1 WHERE b NOT IN (SELECT b "
// 		                                     "FROM t1 WHERE t1.a < 20)");

// 		SubqueryRewriter rewriter(*planner->context);
// 		auto plan = rewriter.Rewrite(move(planner->plan));
// 		// now we expect the subquery to be flattened
// 		REQUIRE(FindLogicalNode(plan.get(), LogicalOperatorType::SUBQUERY));
// 	}
// 	// semi join
// 	{
// 		auto planner = ParseLogicalPlan(con, "SELECT t1.a, t1.b FROM t1 WHERE b IN (SELECT b "
// 		                                     "FROM t1 WHERE t1.a < 20)");

// 		SubqueryRewriter rewriter(*planner->context);
// 		auto plan = rewriter.Rewrite(move(planner->plan));
// 		// now we expect the subquery to be flattened
// 		REQUIRE(FindLogicalNode(plan.get(), LogicalOperatorType::SUBQUERY));
// 	}
// };
