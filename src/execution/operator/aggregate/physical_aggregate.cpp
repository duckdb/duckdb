#include "execution/operator/aggregate/physical_aggregate.hpp"

#include "execution/expression_executor.hpp"
#include "parser/expression/aggregate_expression.hpp"
#include "parser/expression/constant_expression.hpp"

using namespace duckdb;
using namespace std;

PhysicalAggregate::PhysicalAggregate(vector<TypeId> types, vector<unique_ptr<Expression>> select_list,
                                     PhysicalOperatorType type)
    : PhysicalOperator(type, types), select_list(std::move(select_list)) {
	Initialize();
}

PhysicalAggregate::PhysicalAggregate(vector<TypeId> types, vector<unique_ptr<Expression>> select_list,
                                     vector<unique_ptr<Expression>> groups, PhysicalOperatorType type)
    : PhysicalOperator(type, types), select_list(std::move(select_list)), groups(std::move(groups)) {
	Initialize();
}

void PhysicalAggregate::Initialize() {
	// get a list of all aggregates to be computed
	// fake a single group with a constant value for aggregation without groups
	if (groups.size() == 0) {
		unique_ptr<Expression> ce = make_unique<ConstantExpression>(Value::TINYINT(42));
		groups.push_back(move(ce));
		is_implicit_aggr = true;
	} else {
		is_implicit_aggr = false;
	}
	for (auto &expr : select_list) {
		assert(expr->GetExpressionClass() == ExpressionClass::AGGREGATE);
		aggregates.push_back((AggregateExpression *)expr.get());
	}
	for (size_t i = 0; i < aggregates.size(); i++) {
		aggregates[i]->index = i;
	}
}

PhysicalAggregateOperatorState::PhysicalAggregateOperatorState(PhysicalAggregate *parent, PhysicalOperator *child,
                                                               ExpressionExecutor *parent_executor)
    : PhysicalOperatorState(child, parent_executor) {
	vector<TypeId> group_types, aggregate_types;
	for (auto &expr : parent->groups) {
		group_types.push_back(expr->return_type);
	}
	group_chunk.Initialize(group_types);
	for (auto &expr : parent->aggregates) {
		aggregate_types.push_back(expr->return_type);
	}
	aggregate_chunk.Initialize(aggregate_types);
}
