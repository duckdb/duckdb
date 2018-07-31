
#include "execution/operator/physical_aggregate.hpp"
#include "execution/expression_executor.hpp"

#include "parser/expression/aggregate_expression.hpp"

using namespace duckdb;
using namespace std;

PhysicalAggregate::PhysicalAggregate(
    std::vector<std::unique_ptr<AbstractExpression>> select_list,
    PhysicalOperatorType type)
    : PhysicalOperator(type), select_list(std::move(select_list)) {
	Initialize();
}

PhysicalAggregate::PhysicalAggregate(
    std::vector<std::unique_ptr<AbstractExpression>> select_list,
    std::vector<std::unique_ptr<AbstractExpression>> groups,
    PhysicalOperatorType type)
    : PhysicalOperator(type), select_list(std::move(select_list)),
      groups(std::move(groups)) {
	Initialize();
}

void PhysicalAggregate::InitializeChunk(DataChunk &chunk) {
	// get the chunk types from the projection list
	vector<TypeId> types;
	for (auto &expr : select_list) {
		types.push_back(expr->return_type);
	}
	chunk.Initialize(types);
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
    PhysicalAggregate *parent, PhysicalOperator *child)
    : PhysicalOperatorState(child) {
	if (parent->groups.size() == 0) {
		aggregates.resize(parent->aggregates.size());
		for (size_t i = 0; i < parent->aggregates.size(); i++) {
			aggregates[i] =
			    Value::NumericValue(parent->aggregates[i]->return_type, 0);
		}
	} else {
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