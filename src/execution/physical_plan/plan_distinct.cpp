#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"
#include "duckdb/execution/operator/projection/physical_projection.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/function/aggregate/distributive_functions.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/operator/logical_distinct.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<PhysicalOperator> PhysicalPlanGenerator::CreateDistinct(unique_ptr<PhysicalOperator> child) {
	assert(child);
	// create a PhysicalHashAggregate that groups by the input columns
	auto &types = child->GetTypes();
	vector<unique_ptr<Expression>> groups, expressions;
	for (idx_t i = 0; i < types.size(); i++) {
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
	vector<unique_ptr<Expression>> groups, aggregates, projections;
	// creates one group per distinct_target
	for (auto &target : distinct_targets) {
		groups.push_back(move(target));
	}
	// we need to create one aggregate per column in the select_list
	for (idx_t i = 0; i < types.size(); ++i) {
		// first we create an aggregate that returns the FIRST element
		auto bound = make_unique<BoundReferenceExpression>(types[i], i);
		auto first_aggregate = make_unique<BoundAggregateExpression>(
		    types[i], FirstFun::GetFunction(SQLTypeFromInternalType(types[i])), false);
		first_aggregate->children.push_back(move(bound));
		// and push it to the list of aggregates
		aggregates.push_back(move(first_aggregate));
		projections.push_back(make_unique<BoundReferenceExpression>(types[i], i));
	}

	// we add a physical hash aggregation in the plan to select the distinct groups
	auto groupby =
	    make_unique<PhysicalHashAggregate>(types, move(aggregates), move(groups), PhysicalOperatorType::DISTINCT);
	groupby->children.push_back(move(child));

	// we add a physical projection on top of the aggregation to project all members in the select list
	auto aggr_projection = make_unique<PhysicalProjection>(types, move(projections));
	aggr_projection->children.push_back(move(groupby));

	return move(aggr_projection);
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
