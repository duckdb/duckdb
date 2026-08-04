#include "catch.hpp"

#include "duckdb/main/connection.hpp"
#include "duckdb/optimizer/optimizer.hpp"
#include "duckdb/optimizer/projection_pullup.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_dummy_scan.hpp"
#include "duckdb/planner/operator/logical_materialized_cte.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "test_helpers.hpp"

using namespace duckdb;

static optional_ptr<LogicalMaterializedCTE> FindMaterializedCTE(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_MATERIALIZED_CTE) {
		return op.Cast<LogicalMaterializedCTE>();
	}
	for (auto &child : op.children) {
		auto result = FindMaterializedCTE(*child);
		if (result) {
			return result;
		}
	}
	return nullptr;
}

TEST_CASE("Projection pullup revisits the subtree left behind by a moved projection",
          "[optimizer][projection_pullup]") {
	DuckDB db(nullptr);
	Connection con(db);
	auto binder = Binder::CreateBinder(*con.context);
	Optimizer optimizer(*binder, *con.context);

	auto cte = make_uniq<LogicalMaterializedCTE>(
	    Identifier("cte"), TableIndex(2), 1, make_uniq<LogicalDummyScan>(TableIndex(0)),
	    make_uniq<LogicalDummyScan>(TableIndex(1)), CTEMaterialize::CTE_MATERIALIZE_ALWAYS);
	auto nested_join = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
	nested_join->children.push_back(std::move(cte));
	nested_join->children.push_back(make_uniq<LogicalDummyScan>(TableIndex(3)));

	vector<unique_ptr<Expression>> expressions;
	expressions.push_back(make_uniq<BoundConstantExpression>(Value::INTEGER(42)));
	auto projection = make_uniq<LogicalProjection>(TableIndex(4), std::move(expressions));
	projection->children.push_back(std::move(nested_join));

	auto root = make_uniq<LogicalComparisonJoin>(JoinType::INNER);
	root->children.push_back(std::move(projection));
	root->children.push_back(make_uniq<LogicalDummyScan>(TableIndex(5)));
	unique_ptr<LogicalOperator> plan = std::move(root);

	ProjectionPullup projection_pullup(optimizer, plan);
	projection_pullup.Optimize(plan);

	auto materialized_cte = FindMaterializedCTE(*plan);
	REQUIRE(materialized_cte);
	REQUIRE(materialized_cte->children[0]->type == LogicalOperatorType::LOGICAL_PROJECTION);
}
