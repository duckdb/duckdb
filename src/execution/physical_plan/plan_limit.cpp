#include "duckdb/execution/operator/helper/physical_limit.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/planner/operator/logical_limit.hpp"

namespace duckdb {

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalLimit &op) {
	D_ASSERT(op.children.size() == 1);

	auto plan = CreatePlan(*op.children[0]);

	if (op.limit_expression) {
		vector<unique_ptr<Expression>> expressions;
		vector<LogicalType> types;
//		auto &limit_exp = op.limit_expression;
		auto ref = make_unique<BoundReferenceExpression>(op.limit_expression->return_type, expressions.size());
		types.push_back(op.limit_expression->return_type);
		expressions.push_back(move(op.limit_expression));
		op.limit_expression = move(ref);
		auto projection = make_unique<PhysicalProjection>(move(types), move(expressions));
		projection->children.push_back(move(plan));
		auto limit = make_unique<PhysicalLimit>(op.types, op.limit, op.offset);
		limit->children.push_back(move(projection));
		return move(limit);
	} else {
		auto limit = make_unique<PhysicalLimit>(op.types, op.limit, op.offset);
		limit->children.push_back(move(plan));
		return move(limit);
	}
}


} // namespace duckdb
