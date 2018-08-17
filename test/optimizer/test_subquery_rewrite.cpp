
#include "catch.hpp"

#include <vector>

#include "optimizer/rewriter.hpp"
#include "parser/parser.hpp"
#include "planner/planner.hpp"

#include "common/helper.hpp"
#include "optimizer/logical_rules/rule_list.hpp"

#include "duckdb.hpp"

using namespace duckdb;
using namespace std;

// ADD(42, 1) -> 43
TEST_CASE("Subquery rewriting", "[subquery_rewrite]") {
	DuckDB db(nullptr);
	DuckDBConnection con(db);
	con.Query("CREATE TABLE t1 (a INTEGER, b INTEGER)");
	con.Query("INSERT INTO t1 VALUES (1, 10)");
	con.Query("INSERT INTO t1 VALUES (2, 20)");
	con.Query("INSERT INTO t1 VALUES (3, 30)");
	con.Query("INSERT INTO t1 VALUES (3, 42)");

	Parser parser;
	if (!parser.ParseQuery("SELECT t1.a, t1.b FROM t1 WHERE b = (SELECT MIN(b) "
	                       "FROM t1 ts WHERE t1.a=ts.a)")) {
		FAIL(parser.GetErrorMessage());
	}

	Planner planner;
	if (!planner.CreatePlan(db.catalog, move(parser.statements.back()))) {
		FAIL(planner.GetErrorMessage());
	}
	if (!planner.plan) {
		FAIL();
	}

	Rewriter rewriter;
	rewriter.rules.push_back(make_unique_base<Rule, SubqueryRewritingRule>());

	//cout << planner.plan->ToString() + "\n";
	auto plan = rewriter.ApplyRules(move(planner.plan));
	//cout << plan->ToString() + "\n";
};
