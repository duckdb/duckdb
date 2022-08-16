//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/radix_partitioned_hashtable.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/partitionable_hashtable.hpp"
#include "duckdb/parser/group_by_node.hpp"
#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {
class BufferManager;
class Executor;
class PhysicalHashAggregate;
class Pipeline;
class Task;

class RadixPartitionedHashTable {
public:
	RadixPartitionedHashTable(GroupingSet &grouping_set, const PhysicalHashAggregate &op);

	GroupingSet &grouping_set;
	vector<idx_t> null_groups;
	const PhysicalHashAggregate &op;

	vector<LogicalType> group_types;
	//! how many groups can we have in the operator before we switch to radix partitioning
	idx_t radix_limit;

	//! The GROUPING values that belong to this hash table
	vector<Value> grouping_values;

public:
	//! Sink Interface
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const;

	void Sink(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate, DataChunk &input,
	          DataChunk &aggregate_input_chunk) const;
	void Combine(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate) const;
	bool Finalize(ClientContext &context, GlobalSinkState &gstate_p) const;

	void ScheduleTasks(Executor &executor, const shared_ptr<Event> &event, GlobalSinkState &state,
	                   vector<unique_ptr<Task>> &tasks) const;

	//! Source interface
	idx_t Size(GlobalSinkState &sink_state) const;
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const;
	unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context) const;
	void GetData(ExecutionContext &context, DataChunk &chunk, GlobalSinkState &sink_state, GlobalSourceState &gstate_p,
	             LocalSourceState &lstate_p) const;

	static void SetMultiScan(GlobalSinkState &state);
	bool ForceSingleHT(GlobalSinkState &state) const;
};

} // namespace duckdb
