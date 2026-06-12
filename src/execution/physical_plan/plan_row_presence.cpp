#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_row_presence.hpp"

namespace duckdb {

PhysicalOperator &PhysicalPlanGenerator::CreatePlan(LogicalRowPresence &op) {
	D_ASSERT(op.children.size() == 1);
	auto &plan = CreatePlan(*op.children[0]);

	// emit as a projection of all child columns plus a constant true presence column
	auto &child_types = plan.GetTypes();
	vector<unique_ptr<Expression>> expressions;
	expressions.reserve(child_types.size() + 1);
	for (idx_t i = 0; i < child_types.size(); i++) {
		expressions.push_back(make_uniq<BoundReferenceExpression>(child_types[i], i));
	}
	expressions.push_back(make_uniq<BoundConstantExpression>(Value::BOOLEAN(true)));

	auto &projection = Make<PhysicalProjection>(op.types, std::move(expressions), op.estimated_cardinality);
	projection.children.push_back(plan);
	return projection;
}

} // namespace duckdb
