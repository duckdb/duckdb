

#pragma once

#include "execution/physical_operator.hpp"

#include "storage/data_table.hpp"

namespace duckdb {

class PhysicalHashAggregate : public PhysicalAggregate {
  public:
	PhysicalHashAggregate(
	    std::vector<std::unique_ptr<AbstractExpression>> expressions,
	    std::vector<std::unique_ptr<AbstractExpression>> groups)
	    : PhysicalAggregate(PhysicalOperatorType::HASH_GROUP_BY,
	                        std::move(expressions), std::move(groups)) {}

	void InitializeChunk(DataChunk &chunk) override;
	void GetChunk(DataChunk &chunk, PhysicalOperatorState *state) override;

	std::unique_ptr<PhysicalOperatorState> GetOperatorState() override;
};

class PhysicalHashAggregateOperatorState : public PhysicalOperatorState {
  public:
	PhysicalHashAggregateOperatorState(PhysicalOperator *child)
	    : PhysicalOperatorState(child) {}

	DataChunk group_chunk;
	// std::unordered_map<size_t, size_t> group_mapping;
};
}
