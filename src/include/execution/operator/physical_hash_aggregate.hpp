//===----------------------------------------------------------------------===// 
// 
//                         DuckDB 
// 
// execution/operator/physical_hash_aggregate.hpp
// 
// 
// 
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/aggregate_hashtable.hpp"
#include "execution/operator/physical_aggregate.hpp"

#include "storage/data_table.hpp"

namespace duckdb {

//! PhysicalHashAggregate is an group-by and aggregate implementation that uses
//! a hash table to perform the grouping
class PhysicalHashAggregate : public PhysicalAggregate {
  public:
	PhysicalHashAggregate(std::vector<std::unique_ptr<Expression>> expressions);
	PhysicalHashAggregate(std::vector<std::unique_ptr<Expression>> expressions,
	                      std::vector<std::unique_ptr<Expression>> groups);

	void Initialize();

	void _GetChunk(ClientContext &context, DataChunk &chunk,
	               PhysicalOperatorState *state) override;

	std::unique_ptr<PhysicalOperatorState>
	GetOperatorState(ExpressionExecutor *parent) override;
};

class PhysicalHashAggregateOperatorState
    : public PhysicalAggregateOperatorState {
  public:
	PhysicalHashAggregateOperatorState(PhysicalAggregate *parent,
	                                   PhysicalOperator *child,
	                                   ExpressionExecutor *parent_executor)
	    : PhysicalAggregateOperatorState(parent, child, parent_executor),
	      ht_scan_position(0) {
	}

	//! The current position to scan the HT for output tuples
	size_t ht_scan_position;
	size_t tuples_scanned;
	//! The HT
	std::unique_ptr<SuperLargeHashTable> ht;
	//! The payload chunk, only used while filling the HT
	DataChunk payload_chunk;

	std::vector<Expression *> payload_expressions;
};
} // namespace duckdb
