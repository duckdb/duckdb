//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/operator/aggregate/physical_hash_aggregate.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "execution/aggregate_hashtable.hpp"
#include "execution/operator/aggregate/physical_aggregate.hpp"
#include "storage/data_table.hpp"

namespace duckdb {

//! PhysicalHashAggregate is an group-by and aggregate implementation that uses
//! a hash table to perform the grouping
class PhysicalHashAggregate : public PhysicalAggregate {
public:
	PhysicalHashAggregate(vector<TypeId> types, vector<unique_ptr<Expression>> expressions,
	                      PhysicalOperatorType type = PhysicalOperatorType::HASH_GROUP_BY);
	PhysicalHashAggregate(vector<TypeId> types, vector<unique_ptr<Expression>> expressions,
	                      vector<unique_ptr<Expression>> groups,
	                      PhysicalOperatorType type = PhysicalOperatorType::HASH_GROUP_BY);

public:
	void GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;

	unique_ptr<PhysicalOperatorState> GetOperatorState() override;
};

class PhysicalHashAggregateOperatorState : public PhysicalAggregateOperatorState {
public:
	PhysicalHashAggregateOperatorState(PhysicalAggregate *parent, PhysicalOperator *child)
	    : PhysicalAggregateOperatorState(parent, child), ht_scan_position(0), tuples_scanned(0) {
	}

	//! The current position to scan the HT for output tuples
	index_t ht_scan_position;
	index_t tuples_scanned;
	//! The HT
	unique_ptr<SuperLargeHashTable> ht;
	//! The payload chunk, only used while filling the HT
	DataChunk payload_chunk;
};
} // namespace duckdb
