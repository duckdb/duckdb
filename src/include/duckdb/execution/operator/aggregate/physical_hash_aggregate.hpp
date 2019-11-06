//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/operator/aggregate/physical_hash_aggregate.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/aggregate_hashtable.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/storage/data_table.hpp"

namespace duckdb {

//! PhysicalHashAggregate is an group-by and aggregate implementation that uses
//! a hash table to perform the grouping
class PhysicalHashAggregate : public PhysicalOperator {
public:
	PhysicalHashAggregate(vector<TypeId> types, vector<unique_ptr<Expression>> expressions,
	                      PhysicalOperatorType type = PhysicalOperatorType::HASH_GROUP_BY);
	PhysicalHashAggregate(vector<TypeId> types, vector<unique_ptr<Expression>> expressions,
	                      vector<unique_ptr<Expression>> groups,
	                      PhysicalOperatorType type = PhysicalOperatorType::HASH_GROUP_BY);

	//! The groups
	vector<unique_ptr<Expression>> groups;
	//! The aggregates that have to be computed
	vector<unique_ptr<Expression>> aggregates;
	bool is_implicit_aggr;

public:
	void GetChunkInternal(ClientContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;

	unique_ptr<PhysicalOperatorState> GetOperatorState() override;
};

class PhysicalHashAggregateOperatorState : public PhysicalOperatorState {
public:
	PhysicalHashAggregateOperatorState(PhysicalHashAggregate *parent, PhysicalOperator *child);

	//! Materialized GROUP BY expression
	DataChunk group_chunk;
	//! Materialized aggregates
	DataChunk aggregate_chunk;
	//! The current position to scan the HT for output tuples
	index_t ht_scan_position;
	index_t tuples_scanned;
	//! The HT
	unique_ptr<SuperLargeHashTable> ht;
	//! The payload chunk, only used while filling the HT
	DataChunk payload_chunk;
};
} // namespace duckdb
