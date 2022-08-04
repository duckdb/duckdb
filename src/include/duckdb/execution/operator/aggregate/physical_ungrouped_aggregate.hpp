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
#include "duckdb/parser/group_by_node.hpp"
#include "duckdb/execution/radix_partitioned_hashtable.hpp"

namespace duckdb {

struct DistinctAggregateData {
public:
	DistinctAggregateData() {
	}
	//! The data used by the radix_tables;
	vector<unique_ptr<GroupedAggregateData>> grouped_aggregate_data;
	//! The hashtables for distinct aggregates
	vector<unique_ptr<RadixPartitionedHashTable>> radix_tables;

public:
	void Initialize(vector<unique_ptr<Expression>> &aggregates, const vector<idx_t> &indices) {
		idx_t distinct_aggregates = indices.size();
		radix_tables.resize(distinct_aggregates);
		grouped_aggregate_data.resize(distinct_aggregates);

		//! For every distinct aggregate, create a hashtable
		for (idx_t i = 0; i < indices.size(); i++) {
			auto aggr_idx = indices[i];
			auto &aggr = (BoundAggregateExpression &)*(aggregates[aggr_idx]);

			GroupingSet set;
			for (size_t set_idx = 0; set_idx < aggr.children.size(); set_idx++) {
				set.insert(set_idx);
			}
			grouped_aggregate_data[aggr_idx] = make_unique<GroupedAggregateData>();
			grouped_aggregate_data[aggr_idx]->SetDistinctGroupData(aggregates[aggr_idx]->Copy());
			radix_tables[aggr_idx] = make_unique<RadixPartitionedHashTable>(set, *grouped_aggregate_data[aggr_idx]);
		}
	}
	bool AnyDistinct() const {
		return !radix_tables.empty();
	}
};

//! PhysicalUngroupedAggregate is an aggregate operator that can only perform aggregates (1) without any groups, (2)
//! without any DISTINCT aggregates, and (3) when all aggregates are combineable
class PhysicalUngroupedAggregate : public PhysicalOperator {
public:
	PhysicalUngroupedAggregate(vector<LogicalType> types, vector<unique_ptr<Expression>> expressions,
	                           idx_t estimated_cardinality);

	//! The aggregates that have to be computed
	vector<unique_ptr<Expression>> aggregates;
	//! The data used for the distinct aggregates (if any)
	struct DistinctAggregateData distinct_aggregate_data;

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
};

} // namespace duckdb
