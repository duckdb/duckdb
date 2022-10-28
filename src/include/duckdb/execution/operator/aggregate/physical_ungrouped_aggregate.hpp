//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/aggregate/physical_ungrouped_aggregate.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/execution/operator/aggregate/grouped_aggregate_data.hpp"
#include "duckdb/execution/operator/aggregate/distinct_aggregate_data.hpp"
#include "duckdb/parser/group_by_node.hpp"
#include "duckdb/execution/radix_partitioned_hashtable.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {

//! PhysicalUngroupedAggregate is an aggregate operator that can only perform aggregates (1) without any groups, (2)
//! without any DISTINCT aggregates, and (3) when all aggregates are combineable
class PhysicalUngroupedAggregate : public PhysicalOperator {
public:
	PhysicalUngroupedAggregate(vector<LogicalType> types, vector<unique_ptr<Expression>> expressions,
	                           idx_t estimated_cardinality);

	vector<idx_t> distinct_filter;

	//! The aggregates that have to be computed
	vector<unique_ptr<Expression>> aggregates;
	unique_ptr<DistinctAggregateData> distinct_data;
	unique_ptr<DistinctAggregateCollectionInfo> distinct_collection_info;

public:
	// Source interface
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	void GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
	             LocalSourceState &lstate) const override;

public:
	// Sink interface
	SinkResultType Sink(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate,
	                    DataChunk &input) const override;
	void Combine(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          GlobalSinkState &gstate) const override;

	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;

	string ParamsToString() const override;

	bool IsSink() const override {
		return true;
	}

	bool ParallelSink() const override {
		return true;
	}

private:
	//! Finalize the distinct aggregates
	SinkFinalizeType FinalizeDistinct(Pipeline &pipeline, Event &event, ClientContext &context,
	                                  GlobalSinkState &gstate) const;
	//! Combine the distinct aggregates
	void CombineDistinct(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate) const;
	//! Sink the distinct aggregates
	void SinkDistinct(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate,
	                  DataChunk &input) const;
};

} // namespace duckdb
