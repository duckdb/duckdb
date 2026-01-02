#include "duckdb/execution/operator/aggregate/physical_partitioned_aggregate.hpp"
#include "duckdb/execution/operator/aggregate/ungrouped_aggregate_state.hpp"
#include "duckdb/common/types/value_map.hpp"

namespace duckdb {

PhysicalPartitionedAggregate::PhysicalPartitionedAggregate(PhysicalPlan &physical_plan, ClientContext &context,
                                                           vector<LogicalType> types,
                                                           vector<unique_ptr<Expression>> aggregates_p,
                                                           vector<unique_ptr<Expression>> groups_p,
                                                           vector<column_t> partitions_p, idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::PARTITIONED_AGGREGATE, std::move(types),
                       estimated_cardinality),
      partitions(std::move(partitions_p)), groups(std::move(groups_p)), aggregates(std::move(aggregates_p)) {
}

OperatorPartitionInfo PhysicalPartitionedAggregate::RequiredPartitionInfo() const {
	return OperatorPartitionInfo::PartitionColumns(partitions);
}
//===--------------------------------------------------------------------===//
// Global State
//===--------------------------------------------------------------------===//
class PartitionedAggregateLocalSinkState : public LocalSinkState {
public:
	PartitionedAggregateLocalSinkState(const PhysicalPartitionedAggregate &op, const vector<LogicalType> &child_types,
	                                   ExecutionContext &context)
	    : execute_state(context.client, op.aggregates, child_types) {
	}

	//! The current partition
	Value current_partition;
	//! The local aggregate state for the current partition
	unique_ptr<LocalUngroupedAggregateState> state;
	//! The ungrouped aggregate execute state
	UngroupedAggregateExecuteState execute_state;
};

class PartitionedAggregateGlobalSinkState : public GlobalSinkState {
public:
	PartitionedAggregateGlobalSinkState(const PhysicalPartitionedAggregate &op, ClientContext &context)
	    : op(op), aggregate_result(BufferAllocator::Get(context), op.types) {
	}

	mutex lock;
	const PhysicalPartitionedAggregate &op;
	//! The per-partition aggregate states
	value_map_t<unique_ptr<GlobalUngroupedAggregateState>> aggregate_states;
	//! Final aggregate result
	ColumnDataCollection aggregate_result;

	GlobalUngroupedAggregateState &GetOrCreatePartition(ClientContext &context, const Value &partition) {
		lock_guard<mutex> l(lock);
		// find the state that corresponds to this partition and combine
		auto entry = aggregate_states.find(partition);
		if (entry != aggregate_states.end()) {
			return *entry->second;
		}
		// no state yet for this partition - allocate a new one
		auto new_global_state = make_uniq<GlobalUngroupedAggregateState>(BufferAllocator::Get(context), op.aggregates);
		auto &result = *new_global_state;
		aggregate_states.insert(make_pair(partition, std::move(new_global_state)));
		return result;
	}

	void Combine(ClientContext &context, PartitionedAggregateLocalSinkState &lstate) {
		if (!lstate.state) {
			// no aggregate state
			return;
		}
		auto &global_state = GetOrCreatePartition(context, lstate.current_partition);
		global_state.Combine(*lstate.state);
		// clear the local aggregate state
		lstate.state.reset();
	}
};

unique_ptr<GlobalSinkState> PhysicalPartitionedAggregate::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<PartitionedAggregateGlobalSinkState>(*this, context);
}

//===--------------------------------------------------------------------===//
// Local State
//===--------------------------------------------------------------------===//

unique_ptr<LocalSinkState> PhysicalPartitionedAggregate::GetLocalSinkState(ExecutionContext &context) const {
	D_ASSERT(sink_state);
	return make_uniq<PartitionedAggregateLocalSinkState>(*this, children[0].get().GetTypes(), context);
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
SinkResultType PhysicalPartitionedAggregate::Sink(ExecutionContext &context, DataChunk &chunk,
                                                  OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<PartitionedAggregateGlobalSinkState>();
	auto &lstate = input.local_state.Cast<PartitionedAggregateLocalSinkState>();
	if (!lstate.state) {
		// the local state is not yet initialized for this partition
		// initialize the partition
		child_list_t<Value> partition_values;
		for (idx_t partition_idx = 0; partition_idx < groups.size(); partition_idx++) {
			auto column_name = to_string(partition_idx);
			auto &partition = input.local_state.partition_info.partition_data[partition_idx];
			D_ASSERT(Value::NotDistinctFrom(partition.min_val, partition.max_val));
			partition_values.emplace_back(make_pair(std::move(column_name), partition.min_val));
		}
		lstate.current_partition = Value::STRUCT(std::move(partition_values));

		// initialize the state
		auto &global_aggregate_state = gstate.GetOrCreatePartition(context.client, lstate.current_partition);
		lstate.state = make_uniq<LocalUngroupedAggregateState>(global_aggregate_state);
	}

	// perform the aggregation
	lstate.execute_state.Sink(*lstate.state, chunk);
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Next Batch
//===--------------------------------------------------------------------===//
SinkNextBatchType PhysicalPartitionedAggregate::NextBatch(ExecutionContext &context,
                                                          OperatorSinkNextBatchInput &input) const {
	// flush the local state
	auto &gstate = input.global_state.Cast<PartitionedAggregateGlobalSinkState>();
	auto &lstate = input.local_state.Cast<PartitionedAggregateLocalSinkState>();

	// finalize and reset the current state (if any)
	gstate.Combine(context.client, lstate);
	return SinkNextBatchType::READY;
}

//===--------------------------------------------------------------------===//
// Combine
//===--------------------------------------------------------------------===//
SinkCombineResultType PhysicalPartitionedAggregate::Combine(ExecutionContext &context,
                                                            OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<PartitionedAggregateGlobalSinkState>();
	auto &lstate = input.local_state.Cast<PartitionedAggregateLocalSinkState>();
	gstate.Combine(context.client, lstate);
	return SinkCombineResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
SinkFinalizeType PhysicalPartitionedAggregate::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                        OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<PartitionedAggregateGlobalSinkState>();
	ColumnDataAppendState append_state;
	gstate.aggregate_result.InitializeAppend(append_state);
	// finalize each of the partitions and append to a ColumnDataCollection
	DataChunk chunk;
	chunk.Initialize(context, types);
	for (auto &entry : gstate.aggregate_states) {
		chunk.Reset();
		// reference the partitions
		auto &partitions = StructValue::GetChildren(entry.first);
		for (idx_t partition_idx = 0; partition_idx < partitions.size(); partition_idx++) {
			chunk.data[partition_idx].Reference(partitions[partition_idx]);
		}
		// finalize the aggregates
		entry.second->Finalize(chunk, partitions.size());

		// append to the CDC
		gstate.aggregate_result.Append(append_state, chunk);
	}
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class PartitionedAggregateGlobalSourceState : public GlobalSourceState {
public:
	explicit PartitionedAggregateGlobalSourceState(PartitionedAggregateGlobalSinkState &gstate) {
		gstate.aggregate_result.InitializeScan(scan_state);
	}

	ColumnDataScanState scan_state;

	idx_t MaxThreads() override {
		return 1;
	}
};

unique_ptr<GlobalSourceState> PhysicalPartitionedAggregate::GetGlobalSourceState(ClientContext &context) const {
	auto &gstate = sink_state->Cast<PartitionedAggregateGlobalSinkState>();
	return make_uniq<PartitionedAggregateGlobalSourceState>(gstate);
}

SourceResultType PhysicalPartitionedAggregate::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                               OperatorSourceInput &input) const {
	auto &gstate = sink_state->Cast<PartitionedAggregateGlobalSinkState>();
	auto &gsource = input.global_state.Cast<PartitionedAggregateGlobalSourceState>();
	gstate.aggregate_result.Scan(gsource.scan_state, chunk);
	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}

//===--------------------------------------------------------------------===//
// ParamsToString
//===--------------------------------------------------------------------===//
InsertionOrderPreservingMap<string> PhysicalPartitionedAggregate::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	string groups_info;
	for (idx_t i = 0; i < groups.size(); i++) {
		if (i > 0) {
			groups_info += "\n";
		}
		groups_info += groups[i]->GetName();
	}
	result["Groups"] = groups_info;
	string aggregate_info;
	for (idx_t i = 0; i < aggregates.size(); i++) {
		auto &aggregate = aggregates[i]->Cast<BoundAggregateExpression>();
		if (i > 0) {
			aggregate_info += "\n";
		}
		aggregate_info += aggregates[i]->GetName();
		if (aggregate.filter) {
			aggregate_info += " Filter: " + aggregate.filter->GetName();
		}
	}
	result["Aggregates"] = aggregate_info;
	return result;
}

} // namespace duckdb
