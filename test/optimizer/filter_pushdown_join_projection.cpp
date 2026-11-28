#include "catch.hpp"

#include "duckdb.hpp"
#include "duckdb/optimizer/filter_pushdown.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/logical_plan_statement.hpp"
#include "duckdb/planner/operator/logical_any_join.hpp"
#include "duckdb/planner/planner.hpp"

using namespace duckdb;

TEST_CASE("Inner join pushdown preserves projected columns when the join disappears", "[optimizer][filter_pushdown]") {
	string predicate;
	SECTION("One-sided predicate pushed below a cross product") {
		predicate = "l.i > 0";
	}
	SECTION("IN predicate deferred above a cross product") {
		predicate = "(l.i + r.i) IN (0, 2, 4, 6, 8, 10, 12, 14)";
	}

	DuckDB db;
	Connection con(db);
	con.BeginTransaction();
	Parser parser(*con.context);
	parser.ParseQuery("SELECT l.i, l.s, r.i, " + predicate +
	                  " FROM (VALUES (1, 'left')) l(i,s) CROSS JOIN (VALUES (3)) r(i)");
	Planner planner(*con.context);
	planner.CreatePlan(std::move(parser.statements[0]));
	auto &project = *planner.plan;
	REQUIRE(project.type == LogicalOperatorType::LOGICAL_PROJECTION);
	REQUIRE(project.children[0]->type == LogicalOperatorType::LOGICAL_CROSS_PRODUCT);
	auto join = make_uniq<LogicalAnyJoin>(JoinType::INNER);
	join->condition = std::move(project.expressions[3]);
	join->children = std::move(project.children[0]->children);
	join->left_projection_map = {ProjectionIndex(1), ProjectionIndex(0)};
	join->ResolveOperatorTypes();
	const auto bindings = join->GetColumnBindings();
	const auto types = join->types;

	Optimizer optimizer(*planner.binder, *con.context);
	FilterPushdown pushdown(optimizer);
	auto rewritten = pushdown.Rewrite(std::move(join));
	rewritten->ResolveOperatorTypes();
	CAPTURE(rewritten->GetName(), LogicalOperator::ColumnBindingsToString(rewritten->GetColumnBindings()),
	        LogicalOperator::ColumnBindingsToString(bindings));
	CHECK(rewritten->GetColumnBindings() == bindings);
	REQUIRE(rewritten->types == types);
	con.Query("SET enable_optimizer = false")->ThrowIfError();
	auto result = con.Query(make_uniq<LogicalPlanStatement>(std::move(rewritten)));
	result->ThrowIfError();
	REQUIRE(result->RowCount() == 1);
	auto row = result->Fetch();
	REQUIRE(row->GetValue(0, 0) == Value("left"));
	REQUIRE(row->GetValue(1, 0) == Value::INTEGER(1));
	REQUIRE(row->GetValue(2, 0) == Value::INTEGER(3));
}
