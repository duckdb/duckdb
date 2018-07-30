

#pragma once

#include "execution/aggregate_hashtable.hpp"
#include "execution/operator/physical_aggregate.hpp"

#include "storage/data_table.hpp"

namespace duckdb {

class PhysicalHashAggregate : public PhysicalAggregate {
  public:
	PhysicalHashAggregate(
	    std::vector<std::unique_ptr<AbstractExpression>> expressions);
	PhysicalHashAggregate(
	    std::vector<std::unique_ptr<AbstractExpression>> expressions,
	    std::vector<std::unique_ptr<AbstractExpression>> groups);

	void Initialize();

	void InitializeChunk(DataChunk &chunk) override;
	void GetChunk(DataChunk &chunk, PhysicalOperatorState *state) override;

	std::unique_ptr<PhysicalOperatorState> GetOperatorState() override;
};

class PhysicalHashAggregateOperatorState
    : public PhysicalAggregateOperatorState {
  public:
	PhysicalHashAggregateOperatorState(PhysicalAggregate *parent,
	                                   PhysicalOperator *child)
	    : PhysicalAggregateOperatorState(parent, child), ht_scan_position(0) {}

	size_t ht_scan_position;
	std::unique_ptr<SuperLargeHashTable> ht;
	DataChunk group_chunk;
	DataChunk payload_chunk;
};
} // namespace duckdb
