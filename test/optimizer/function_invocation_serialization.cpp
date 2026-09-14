#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/logical_plan_statement.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/planner/logical_operator.hpp"

using namespace duckdb;

static unique_ptr<LogicalOperator> PlanAndOptimize(Connection &connection, const string &sql) {
	Parser parser(connection.context->GetParserOptions());
	parser.ParseQuery(sql);
	Planner planner(*connection.context);
	planner.CreatePlan(std::move(parser.statements[0]));
	planner.plan->ResolveOperatorTypes();
	Optimizer optimizer(*planner.binder, *connection.context);
	auto plan = optimizer.Optimize(std::move(planner.plan));
	plan->ResolveOperatorTypes();
	return plan;
}

TEST_CASE("List ordering retains its bound collation across plan serialization",
          "[serialization][function_invocation]") {
	DuckDB db(nullptr);
	Connection connection(db);
	REQUIRE_NO_FAIL(connection.Query("SET threads=1; SET default_collation='nocase'"));
	connection.BeginTransaction();
	auto plan = PlanAndOptimize(connection, "SELECT list_sort(x) FROM (VALUES (['b','B']::VARCHAR[]))t(x)");
	REQUIRE_NO_FAIL(connection.Query("SET default_collation=''"));
	plan = plan->Copy(*connection.context);
	plan->ResolveOperatorTypes();
	REQUIRE_NO_FAIL(connection.Query("SET debug_disable_optimizer=true"));
	auto direct = connection.Query(make_uniq<LogicalPlanStatement>(std::move(plan)));
	REQUIRE_NO_FAIL(*direct);
	REQUIRE(Value::NotDistinctFrom(direct->GetValue(0, 0), Value::LIST({Value("b"), Value("B")})));
	connection.Rollback();
}
