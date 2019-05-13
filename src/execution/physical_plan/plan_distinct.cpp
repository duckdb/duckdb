#include "execution/operator/aggregate/physical_hash_aggregate.hpp"
#include "execution/physical_plan_generator.hpp"
#include "planner/expression/bound_reference_expression.hpp"
#include "planner/operator/logical_distinct.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreateDistinct(unique_ptr<PhysicalOperator> child) {
	assert(child);
	// create a PhysicalHashAggregate that groups by the input columns
	auto &types = child->GetTypes();
	vector<unique_ptr<Expression>> groups, expressions;
	for (index_t i = 0; i < types.size(); i++) {
		groups.push_back(make_unique<BoundReferenceExpression>(types[i], i));
	}
	auto groupby =
	    make_unique<PhysicalHashAggregate>(types, move(expressions), move(groups), PhysicalOperatorType::DISTINCT);
	groupby->children.push_back(move(child));
	return move(groupby);
}

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalDistinct &op) {
	assert(op.children.size() == 1);
	auto plan = CreatePlan(*op.children[0]);
	return CreateDistinct(move(plan));
}
