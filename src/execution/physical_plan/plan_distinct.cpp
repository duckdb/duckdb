#include "execution/operator/aggregate/physical_hash_aggregate.hpp"
#include "execution/operator/projection/physical_projection.hpp"
#include "execution/physical_plan_generator.hpp"
#include "planner/expression/bound_aggregate_expression.hpp"
#include "planner/expression/bound_columnref_expression.hpp"
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

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreateDistinctOn(unique_ptr<PhysicalOperator> child,
                                                                     vector<unique_ptr<Expression>> distinct_targets) {
	assert(child);
	assert(distinct_targets.size() > 0);

	auto &types = child->GetTypes();
	auto &projection = reinterpret_cast<PhysicalProjection &>(*child);

	vector<unique_ptr<Expression>> groups;
	vector<unique_ptr<Expression>> expressions;
	vector<unique_ptr<Expression>> projections;
	vector<TypeId> target_types;
	// creates one group per distinct_target
	for (index_t i = 0; i < distinct_targets.size(); i++) {
		auto &colref = (BoundColumnRefExpression &)*distinct_targets[i];
		auto col_index = colref.binding.column_index;
		groups.push_back(make_unique<BoundReferenceExpression>(types[col_index], col_index));
	}
	// creates one aggregate per column in the select_list
	for (index_t i = 0; i < projection.select_list.size(); ++i) {
		// first we create an aggregate that returns the FIRST element
		auto bound = make_unique<BoundReferenceExpression>(types[i], i);
		auto first_aggregate =
		    make_unique<BoundAggregateExpression>(types[i], ExpressionType::AGGREGATE_FIRST, move(bound));
		// and push it to the list of expressions
		expressions.push_back(move(first_aggregate));
		projections.push_back(make_unique<BoundReferenceExpression>(types[i], i));
	}

	auto groupby =
	    make_unique<PhysicalHashAggregate>(types, move(expressions), move(groups), PhysicalOperatorType::DISTINCT_ON);
	groupby->children.push_back(move(child));

	auto final_projection = make_unique<PhysicalProjection>(types, move(projections));
	final_projection->children.push_back(move(groupby));

	return move(final_projection);
}

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreatePlan(LogicalDistinct &op) {
	assert(op.children.size() == 1);
	auto plan = CreatePlan(*op.children[0]);
	if (op.distinct_targets.size() > 0) {
		return CreateDistinctOn(move(plan), move(op.distinct_targets));
	} else {
		return CreateDistinct(move(plan));
	}
}
