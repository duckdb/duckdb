
#include "execution/operator/physical_aggregate.hpp"
#include "execution/expression_executor.hpp"

#include "parser/expression/aggregate_expression.hpp"

using namespace duckdb;
using namespace std;

PhysicalAggregate::PhysicalAggregate(
    std::vector<std::unique_ptr<Expression>> select_list,
    PhysicalOperatorType type)
    : PhysicalOperator(type), select_list(std::move(select_list)) {
	Initialize();
}

PhysicalAggregate::PhysicalAggregate(
    std::vector<std::unique_ptr<Expression>> select_list,
    std::vector<std::unique_ptr<Expression>> groups, PhysicalOperatorType type)
    : PhysicalOperator(type), select_list(std::move(select_list)),
      groups(std::move(groups)) {
	Initialize();
}

vector<string> PhysicalAggregate::GetNames() {
	vector<string> names;
	for (auto &exp : select_list) {
		names.push_back(exp->GetName());
	}
	return names;
}

vector<TypeId> PhysicalAggregate::GetTypes() {
	// get the chunk types from the projection list
	vector<TypeId> types;
	for (auto &expr : select_list) {
		types.push_back(expr->return_type);
	}
	return types;
}

void PhysicalAggregate::Initialize() {
	// get a list of all aggregates to be computed
	for (auto &expr : select_list) {
		expr->GetAggregates(aggregates);
	}
	for (size_t i = 0; i < aggregates.size(); i++) {
		aggregates[i]->index = i;
	}
}

PhysicalAggregateOperatorState::PhysicalAggregateOperatorState(
    PhysicalAggregate *parent, PhysicalOperator *child,
    ExpressionExecutor *parent_executor)
    : PhysicalOperatorState(child, parent_executor) {
	if (parent->groups.size() > 0) {
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
}
