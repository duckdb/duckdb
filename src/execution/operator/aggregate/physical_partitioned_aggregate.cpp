#include "duckdb/execution/operator/aggregate/physical_partitioned_aggregate.hpp"
#include "duckdb/execution/operator/aggregate/ungrouped_aggregate_state.hpp"
#include "duckdb/common/types/value_map.hpp"

namespace duckdb {

PhysicalPartitionedAggregate::PhysicalPartitionedAggregate(ClientContext &context, vector<LogicalType> types, vector<unique_ptr<Expression>> aggregates_p,
							 vector<unique_ptr<Expression>> groups_p, vector<column_t> partitions_p, idx_t estimated_cardinality) :
	PhysicalOperator(PhysicalOperatorType::PARTITIONED_AGGREGATE, std::move(types), estimated_cardinality),
	partitions(std::move(partitions_p)), groups(std::move(groups_p)), aggregates(std::move(aggregates_p)) {
}

OperatorPartitionInfo PhysicalPartitionedAggregate::RequiredPartitionInfo() const {
	return OperatorPartitionInfo::PartitionColumns(partitions);
}
//===--------------------------------------------------------------------===//
// Global State
//===--------------------------------------------------------------------===//
class PartitionedAggregateGlobalSinkState : public GlobalSinkState {
public:
	PartitionedAggregateGlobalSinkState(const PhysicalPartitionedAggregate &op, ClientContext &client) {
	}

	//! The per-partition aggregate states
	value_map_t<unique_ptr<GlobalUngroupedAggregateState>> aggregate_states;
};

unique_ptr<GlobalSinkState> PhysicalPartitionedAggregate::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<PartitionedAggregateGlobalSinkState>(*this, context);
}

//===--------------------------------------------------------------------===//
// Local State
//===--------------------------------------------------------------------===//
class PartitionedAggregateLocalSinkState : public LocalSinkState {
public:
	PartitionedAggregateLocalSinkState(const PhysicalPartitionedAggregate &op, const vector<LogicalType> &child_types,
	                                 ExecutionContext &context)
	    : child_executor(context.client), aggregate_input_chunk(), filter_set() {
		auto &allocator = BufferAllocator::Get(context.client);

		vector<LogicalType> payload_types;
		vector<AggregateObject> aggregate_objects;
		for (auto &aggregate : op.aggregates) {
			D_ASSERT(aggregate->GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE);
			auto &aggr = aggregate->Cast<BoundAggregateExpression>();
			// initialize the payload chunk
			for (auto &child : aggr.children) {
				payload_types.push_back(child->return_type);
				child_executor.AddExpression(*child);
			}
			aggregate_objects.emplace_back(&aggr);
		}
		if (!payload_types.empty()) { // for select count(*) from t; there is no payload at all
			aggregate_input_chunk.Initialize(allocator, payload_types);
		}
		filter_set.Initialize(context.client, aggregate_objects, child_types);
	}

	//! The local aggregate state for the current partition
	unique_ptr<LocalUngroupedAggregateState> state;
	//! The executor
	ExpressionExecutor child_executor;
	//! The payload chunk, containing all the Vectors for the aggregates
	DataChunk aggregate_input_chunk;
	//! Aggregate filter data set
	AggregateFilterDataSet filter_set;

public:
	void Reset() {
		aggregate_input_chunk.Reset();
	}
};

unique_ptr<LocalSinkState> PhysicalPartitionedAggregate::GetLocalSinkState(ExecutionContext &context) const {
	D_ASSERT(sink_state);
	return make_uniq<PartitionedAggregateLocalSinkState>(*this, children[0]->GetTypes(), context);
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
SinkResultType PhysicalPartitionedAggregate::Sink(ExecutionContext &context, DataChunk &chunk,
												OperatorSinkInput &input) const {
	throw InternalException("Sink");
}

//===--------------------------------------------------------------------===//
// Combine
//===--------------------------------------------------------------------===//
SinkCombineResultType PhysicalPartitionedAggregate::Combine(ExecutionContext &context,
														  OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<PartitionedAggregateGlobalSinkState>();
	auto &lstate = input.local_state.Cast<PartitionedAggregateLocalSinkState>();
	throw InternalException("Combine");
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
SinkFinalizeType PhysicalPartitionedAggregate::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
													  OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<PartitionedAggregateGlobalSinkState>();
	throw InternalException("Finalize");
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
SourceResultType PhysicalPartitionedAggregate::GetData(ExecutionContext &context, DataChunk &chunk,
													 OperatorSourceInput &input) const {
	throw InternalException("GetData");
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

}

