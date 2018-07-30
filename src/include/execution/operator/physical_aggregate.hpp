

#pragma once

#include "execution/physical_operator.hpp"

namespace duckdb {

class PhysicalAggregate : public PhysicalOperator {
  public:
	PhysicalAggregate(
	    std::vector<std::unique_ptr<AbstractExpression>> select_list,
	    PhysicalOperatorType type = PhysicalOperatorType::BASE_GROUP_BY);
	PhysicalAggregate(
	    std::vector<std::unique_ptr<AbstractExpression>> select_list,
	    std::vector<std::unique_ptr<AbstractExpression>> groups,
	    PhysicalOperatorType type = PhysicalOperatorType::BASE_GROUP_BY);

	void Initialize();

	void InitializeChunk(DataChunk &chunk) override;

	std::vector<std::unique_ptr<AbstractExpression>> select_list;
	std::vector<std::unique_ptr<AbstractExpression>> groups;
	std::vector<AggregateExpression*> aggregates;
};

class PhysicalAggregateOperatorState : public PhysicalOperatorState {
  public:
	PhysicalAggregateOperatorState(PhysicalAggregate *parent, PhysicalOperator *child = nullptr);

	bool finished;
	std::vector<Value> aggregates;
	DataChunk group_chunk;
	DataChunk aggregate_chunk;
};

}
