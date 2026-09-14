#include "catch.hpp"
#include "test_helpers.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/statement/logical_plan_statement.hpp"
#include "duckdb/planner/planner.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_secure_view.hpp"
#include "duckdb/planner/expression_iterator.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"

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

static optional_ptr<LogicalSecureView> FindSecureView(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_SECURE_VIEW) {
		return op.Cast<LogicalSecureView>();
	}
	for (auto &child : op.children) {
		auto view = FindSecureView(*child);
		if (view) {
			return view;
		}
	}
	return nullptr;
}

TEST_CASE("Secure-view caller predicates retain source positions across pruning and serialization",
          "[serialization][secure_view]") {
	DuckDB db(nullptr);
	Connection connection(db);
	REQUIRE_NO_FAIL(connection.Query("CREATE TABLE filter_input(i INTEGER, p VARCHAR); "
	                                 "INSERT INTO filter_input VALUES (1,'a'),(2,'b'),(NULL,'n'),(1,'a'); "
	                                 "CREATE SECURE VIEW filter_view AS SELECT * FROM filter_input"));
	connection.BeginTransaction();
	auto plan = PlanAndOptimize(connection, "SELECT p FROM filter_view WHERE i=1 ORDER BY p");
	for (idx_t copy = 0; copy < 2; copy++) {
		auto view = FindSecureView(*plan);
		REQUIRE(view);
		REQUIRE(view->source_filters.size() == 1);
		REQUIRE(view->source_filters.size() == view->pushed_filters.size());
		REQUIRE(view->source_filters[0]);
		idx_t references = 0;
		ExpressionIterator::VisitExpression<BoundColumnRefExpression>(
		    *view->source_filters[0], [&](const BoundColumnRefExpression &ref) {
			    REQUIRE(ref.Depth() == 0);
			    REQUIRE(ref.Binding() == ColumnBinding(TableIndex(0), ProjectionIndex(0)));
			    references++;
		    });
		REQUIRE(references == 1);
		plan = plan->Copy(*connection.context);
		plan->ResolveOperatorTypes();
	}
	REQUIRE_NO_FAIL(connection.Query("SET debug_disable_optimizer=true"));
	auto result = connection.Query(make_uniq<LogicalPlanStatement>(std::move(plan)));
	REQUIRE_NO_FAIL(*result);
	REQUIRE(CHECK_COLUMN(result, 0, {"a", "a"}));
	connection.Rollback();
}
