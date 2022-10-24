#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"

#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/aggregate_hashtable.hpp"
#include "duckdb/execution/partitionable_hashtable.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/parallel/base_pipeline_event.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/execution/operator/aggregate/distinct_aggregate_data.hpp"

namespace duckdb {

PhysicalHashAggregate::PhysicalHashAggregate(ClientContext &context, vector<LogicalType> types,
                                             vector<unique_ptr<Expression>> expressions, idx_t estimated_cardinality)
    : PhysicalHashAggregate(context, move(types), move(expressions), {}, estimated_cardinality) {
}

PhysicalHashAggregate::PhysicalHashAggregate(ClientContext &context, vector<LogicalType> types,
                                             vector<unique_ptr<Expression>> expressions,
                                             vector<unique_ptr<Expression>> groups_p, idx_t estimated_cardinality)
    : PhysicalHashAggregate(context, move(types), move(expressions), move(groups_p), {}, {}, estimated_cardinality) {
}

PhysicalHashAggregate::PhysicalHashAggregate(ClientContext &context, vector<LogicalType> types,
                                             vector<unique_ptr<Expression>> expressions,
                                             vector<unique_ptr<Expression>> groups_p,
                                             vector<GroupingSet> grouping_sets_p,
                                             vector<vector<idx_t>> grouping_functions_p, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::HASH_GROUP_BY, move(types), estimated_cardinality),
      grouping_sets(move(grouping_sets_p)) {
	// get a list of all aggregates to be computed
	const idx_t group_count = groups_p.size();
	if (grouping_sets.empty()) {
		GroupingSet set;
		for (idx_t i = 0; i < group_count; i++) {
			set.insert(i);
		}
		grouping_sets.push_back(move(set));
	}
	grouped_aggregate_data.InitializeGroupby(move(groups_p), move(expressions), move(grouping_functions_p));

	auto &aggregates = grouped_aggregate_data.aggregates;
	// filter_indexes must be pre-built, not lazily instantiated in parallel...
	idx_t aggregate_input_idx = 0;
	for (auto &aggregate : aggregates) {
		auto &aggr = (BoundAggregateExpression &)*aggregate;
		aggregate_input_idx += aggr.children.size();
	}
	for (idx_t i = 0; i < aggregates.size(); i++) {
		auto &aggregate = aggregates[i];
		auto &aggr = (BoundAggregateExpression &)*aggregate;
		if (aggr.filter) {
			auto &bound_ref_expr = (BoundReferenceExpression &)*aggr.filter;
			auto it = filter_indexes.find(aggr.filter.get());
			if (it == filter_indexes.end()) {
				filter_indexes[aggr.filter.get()] = bound_ref_expr.index;
				bound_ref_expr.index = aggregate_input_idx++;
			} else {
				++aggregate_input_idx;
			}
		}
	}

	for (auto &grouping_set : grouping_sets) {
		radix_tables.emplace_back(grouping_set, grouped_aggregate_data);
	}
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class HashAggregateGlobalState : public GlobalSinkState {
public:
	HashAggregateGlobalState(const PhysicalHashAggregate &op, ClientContext &context) {
		auto &allocator = Allocator::Get(context);

		radix_states.reserve(op.radix_tables.size());
		for (auto &rt : op.radix_tables) {
			radix_states.push_back(rt.GetGlobalSinkState(context));
		}
		vector<idx_t> distinct_indices;
		for (idx_t i = 0; i < op.grouped_aggregate_data.aggregates.size(); i++) {
			auto &aggregate = op.grouped_aggregate_data.aggregates[i];
			auto &aggr = (BoundAggregateExpression &)*aggregate;
			if (!aggr.distinct) {
				continue;
			}
			distinct_indices.push_back(i);
		}
		if (distinct_indices.empty()) {
			// None of the aggregates are DISTINCT
			return;
		}
		distinct_data = make_unique<DistinctAggregateData>(allocator, op.grouped_aggregate_data.aggregates,
		                                                   move(distinct_indices), context);
	}

	vector<unique_ptr<GlobalSinkState>> radix_states;
	unique_ptr<DistinctAggregateData> distinct_data;
};

class HashAggregateLocalState : public LocalSinkState {
public:
	HashAggregateLocalState(const PhysicalHashAggregate &op, GlobalSinkState &gstate_p, ExecutionContext &context) {

		auto &gstate = (HashAggregateGlobalState &)gstate_p;

		InitializeDistinctAggregates(gstate, context);

		auto &payload_types = op.grouped_aggregate_data.payload_types;
		if (!payload_types.empty()) {
			aggregate_input_chunk.InitializeEmpty(payload_types);
		}

		radix_states.reserve(op.radix_tables.size());
		for (auto &rt : op.radix_tables) {
			radix_states.push_back(rt.GetLocalSinkState(context));
		}
		// FIXME: missing filter_set??
	}

	DataChunk aggregate_input_chunk;

	vector<unique_ptr<LocalSinkState>> radix_states;
	vector<unique_ptr<LocalSinkState>> distinct_states;

public:
	void InitializeDistinctAggregates(const HashAggregateGlobalState &gstate, ExecutionContext &context) {
		if (!gstate.distinct_data) {
			return;
		}
		auto &data = *gstate.distinct_data;
		auto &distinct_indices = data.Indices();
		D_ASSERT(!distinct_indices.empty());
		D_ASSERT(!data.radix_tables.empty());

		const idx_t aggregate_count = data.radix_tables.size();
		distinct_states.resize(aggregate_count);

		for (auto &idx : distinct_indices) {
			idx_t table_idx = data.table_map[idx];
			if (data.radix_tables[table_idx] == nullptr) {
				// This aggregate has identical input as another aggregate, so no table is created for it
				continue;
			}
			// Initialize the states of the radix tables used for the distinct aggregates
			auto &radix_table = *data.radix_tables[table_idx];
			distinct_states[table_idx] = radix_table.GetLocalSinkState(context);
		}
	}
};

void PhysicalHashAggregate::SetMultiScan(GlobalSinkState &state) {
	auto &gstate = (HashAggregateGlobalState &)state;
	for (auto &radix_state : gstate.radix_states) {
		RadixPartitionedHashTable::SetMultiScan(*radix_state);
	}
}

unique_ptr<GlobalSinkState> PhysicalHashAggregate::GetGlobalSinkState(ClientContext &context) const {
	return make_unique<HashAggregateGlobalState>(*this, context);
}

unique_ptr<LocalSinkState> PhysicalHashAggregate::GetLocalSinkState(ExecutionContext &context) const {
	D_ASSERT(sink_state);
	auto &gstate = *sink_state;
	return make_unique<HashAggregateLocalState>(*this, gstate, context);
}

void PhysicalHashAggregate::SinkDistinct(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate,
                                         DataChunk &input) const {
	auto &sink = (HashAggregateLocalState &)lstate;
	auto &global_sink = (HashAggregateGlobalState &)state;
	D_ASSERT(global_sink.distinct_data);
	auto &distinct_aggregate_data = *global_sink.distinct_data;
	auto &distinct_indices = distinct_aggregate_data.Indices();
	for (auto &idx : distinct_indices) {
		auto &aggregate = (BoundAggregateExpression &)*grouped_aggregate_data.aggregates[idx];

		idx_t table_idx = distinct_aggregate_data.table_map[idx];
		if (!distinct_aggregate_data.radix_tables[table_idx]) {
			continue;
		}
		D_ASSERT(distinct_aggregate_data.radix_tables[table_idx]);
		auto &radix_table = *distinct_aggregate_data.radix_tables[table_idx];
		auto &radix_global_sink = *distinct_aggregate_data.radix_states[table_idx];
		auto &radix_local_sink = *sink.distinct_states[table_idx];

		// if (aggregate.filter) {
		//	// Apply the filter before inserting into the hashtable
		//	auto &filtered_data = sink.filter_set.GetFilterData(idx);
		//	idx_t count = filtered_data.ApplyFilter(input);
		//	filtered_data.filtered_payload.SetCardinality(count);

		//	radix_table.Sink(context, radix_global_sink, radix_local_sink, filtered_data.filtered_payload,
		//	                 filtered_data.filtered_payload);
		//} else {
		//	radix_table.Sink(context, radix_global_sink, radix_local_sink, input, input);
		//}
		radix_table.Sink(context, radix_global_sink, radix_local_sink, input, input);
	}
}

SinkResultType PhysicalHashAggregate::Sink(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate,
                                           DataChunk &input) const {
	auto &llstate = (HashAggregateLocalState &)lstate;
	auto &gstate = (HashAggregateGlobalState &)state;

	if (gstate.distinct_data) {
		SinkDistinct(context, state, lstate, input);
	}

	DataChunk &aggregate_input_chunk = llstate.aggregate_input_chunk;

	auto &aggregates = grouped_aggregate_data.aggregates;
	idx_t aggregate_input_idx = 0;

	// Populate the aggregate child vectors
	for (auto &aggregate : aggregates) {
		auto &aggr = (BoundAggregateExpression &)*aggregate;
		for (auto &child_expr : aggr.children) {
			D_ASSERT(child_expr->type == ExpressionType::BOUND_REF);
			auto &bound_ref_expr = (BoundReferenceExpression &)*child_expr;
			D_ASSERT(bound_ref_expr.index < input.data.size());
			aggregate_input_chunk.data[aggregate_input_idx++].Reference(input.data[bound_ref_expr.index]);
		}
	}
	// Populate the filter vectors
	for (auto &aggregate : aggregates) {
		auto &aggr = (BoundAggregateExpression &)*aggregate;
		if (aggr.filter) {
			auto it = filter_indexes.find(aggr.filter.get());
			D_ASSERT(it != filter_indexes.end());
			D_ASSERT(it->second < input.data.size());
			aggregate_input_chunk.data[aggregate_input_idx++].Reference(input.data[it->second]);
		}
	}

	aggregate_input_chunk.SetCardinality(input.size());
	aggregate_input_chunk.Verify();

	for (idx_t i = 0; i < radix_tables.size(); i++) {
		// TODO: apply filter
		radix_tables[i].Sink(context, *gstate.radix_states[i], *llstate.radix_states[i], input, aggregate_input_chunk);
	}

	return SinkResultType::NEED_MORE_INPUT;
}

void PhysicalHashAggregate::CombineDistinct(ExecutionContext &context, GlobalSinkState &state,
                                            LocalSinkState &lstate) const {
	auto &global_sink = (HashAggregateGlobalState &)state;
	auto &source = (HashAggregateLocalState &)lstate;
	auto &distinct_aggregate_data = global_sink.distinct_data;

	if (!distinct_aggregate_data) {
		return;
	}
	auto table_count = distinct_aggregate_data->radix_tables.size();
	for (idx_t table_idx = 0; table_idx < table_count; table_idx++) {
		D_ASSERT(distinct_aggregate_data->radix_tables[table_idx]);
		auto &radix_table = *distinct_aggregate_data->radix_tables[table_idx];
		auto &radix_global_sink = *distinct_aggregate_data->radix_states[table_idx];
		auto &radix_local_sink = *source.radix_states[table_idx];

		radix_table.Combine(context, radix_global_sink, radix_local_sink);
	}
}

void PhysicalHashAggregate::Combine(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate) const {
	auto &gstate = (HashAggregateGlobalState &)state;
	auto &llstate = (HashAggregateLocalState &)lstate;

	for (idx_t i = 0; i < radix_tables.size(); i++) {
		radix_tables[i].Combine(context, *gstate.radix_states[i], *llstate.radix_states[i]);
	}
}

class HashAggregateFinalizeEvent : public BasePipelineEvent {
public:
	HashAggregateFinalizeEvent(const PhysicalHashAggregate &op_p, HashAggregateGlobalState &gstate_p,
	                           Pipeline *pipeline_p)
	    : BasePipelineEvent(*pipeline_p), op(op_p), gstate(gstate_p) {
	}

	const PhysicalHashAggregate &op;
	HashAggregateGlobalState &gstate;

public:
	void Schedule() override {
		vector<unique_ptr<Task>> tasks;
		for (idx_t i = 0; i < op.radix_tables.size(); i++) {
			op.radix_tables[i].ScheduleTasks(pipeline->executor, shared_from_this(), *gstate.radix_states[i], tasks);
		}
		D_ASSERT(!tasks.empty());
		SetTasks(move(tasks));
	}
};

SinkFinalizeType PhysicalHashAggregate::FinalizeDistinct(Pipeline &pipeline, Event &event, ClientContext &context,
                                                         GlobalSinkState &gstate_p) const {
	// auto &gstate = (HashAggregateGlobalState &)gstate_p;
	// D_ASSERT(gstate.distinct_data);
	// auto &distinct_aggregate_data = *gstate.distinct_data;
	// auto &payload_chunk = distinct_aggregate_data.payload_chunk;

	////! Copy of the payload chunk, used to store the data of the radix table for use with the expression executor
	////! We can not directly use the payload chunk because the input and the output to the expression executor can not
	/// be
	////! the same Vector
	// DataChunk expression_executor_input;
	// expression_executor_input.InitializeEmpty(payload_chunk.GetTypes());
	// expression_executor_input.SetCardinality(0);

	// bool any_partitioned = false;
	// for (idx_t table_idx = 0; table_idx < distinct_aggregate_data.radix_tables.size(); table_idx++) {
	//	auto &radix_table_p = distinct_aggregate_data.radix_tables[table_idx];
	//	auto &radix_state = *distinct_aggregate_data.radix_states[table_idx];
	//	bool partitioned = radix_table_p->Finalize(context, radix_state);
	//	if (partitioned) {
	//		any_partitioned = true;
	//	}
	// }
	// if (any_partitioned) {
	//	auto new_event = make_shared<DistinctCombineFinalizeEvent>(*this, gstate, pipeline, context);
	//	event.InsertEvent(move(new_event));
	// } else {
	//	//! Hashtables aren't partitioned, they dont need to be joined first
	//	//! So we can compute the aggregate already
	//	auto new_event = make_shared<DistinctAggregateFinalizeEvent>(*this, gstate, pipeline, context);
	//	event.InsertEvent(move(new_event));
	// }
	return SinkFinalizeType::READY;
}

SinkFinalizeType PhysicalHashAggregate::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                 GlobalSinkState &gstate_p) const {
	auto &gstate = (HashAggregateGlobalState &)gstate_p;
	bool any_partitioned = false;
	for (idx_t i = 0; i < gstate.radix_states.size(); i++) {
		bool is_partitioned = radix_tables[i].Finalize(context, *gstate.radix_states[i]);
		if (is_partitioned) {
			any_partitioned = true;
		}
	}
	if (any_partitioned) {
		auto new_event = make_shared<HashAggregateFinalizeEvent>(*this, gstate, &pipeline);
		event.InsertEvent(move(new_event));
	}
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class PhysicalHashAggregateGlobalSourceState : public GlobalSourceState {
public:
	PhysicalHashAggregateGlobalSourceState(ClientContext &context, const PhysicalHashAggregate &op)
	    : op(op), state_index(0) {
		for (auto &rt : op.radix_tables) {
			radix_states.push_back(rt.GetGlobalSourceState(context));
		}
	}

	const PhysicalHashAggregate &op;
	std::atomic<size_t> state_index;

	vector<unique_ptr<GlobalSourceState>> radix_states;

public:
#if 0
	idx_t MaxThreads() override {
		// If there are no tables, we only need one thread.
		if (op.radix_tables.empty()) {
			return 1;
		}

		auto &ht_state = (HashAggregateGlobalState &)*op.sink_state;
		idx_t count = 0;
		for (size_t sidx = 0; sidx < op.radix_tables.size(); ++sidx) {
			count += op.radix_tables[sidx].Size(*ht_state.radix_states[sidx]);
		}

		return (count + STANDARD_VECTOR_SIZE - 1 ) / STANDARD_VECTOR_SIZE;
	}
#endif
};

unique_ptr<GlobalSourceState> PhysicalHashAggregate::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<PhysicalHashAggregateGlobalSourceState>(context, *this);
}

class PhysicalHashAggregateLocalSourceState : public LocalSourceState {
public:
	explicit PhysicalHashAggregateLocalSourceState(ExecutionContext &context, const PhysicalHashAggregate &op) {
		for (auto &rt : op.radix_tables) {
			radix_states.push_back(rt.GetLocalSourceState(context));
		}
	}

	vector<unique_ptr<LocalSourceState>> radix_states;
};

unique_ptr<LocalSourceState> PhysicalHashAggregate::GetLocalSourceState(ExecutionContext &context,
                                                                        GlobalSourceState &gstate) const {
	return make_unique<PhysicalHashAggregateLocalSourceState>(context, *this);
}

void PhysicalHashAggregate::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate_p,
                                    LocalSourceState &lstate_p) const {
	auto &ht_state = (HashAggregateGlobalState &)*sink_state;
	auto &gstate = (PhysicalHashAggregateGlobalSourceState &)gstate_p;
	auto &lstate = (PhysicalHashAggregateLocalSourceState &)lstate_p;
	for (size_t sidx = gstate.state_index; sidx < radix_tables.size(); sidx = ++gstate.state_index) {
		radix_tables[sidx].GetData(context, chunk, *ht_state.radix_states[sidx], *gstate.radix_states[sidx],
		                           *lstate.radix_states[sidx]);
		if (chunk.size() != 0) {
			return;
		}
	}
}

string PhysicalHashAggregate::ParamsToString() const {
	string result;
	auto &groups = grouped_aggregate_data.groups;
	auto &aggregates = grouped_aggregate_data.aggregates;
	for (idx_t i = 0; i < groups.size(); i++) {
		if (i > 0) {
			result += "\n";
		}
		result += groups[i]->GetName();
	}
	for (idx_t i = 0; i < aggregates.size(); i++) {
		auto &aggregate = (BoundAggregateExpression &)*aggregates[i];
		if (i > 0 || !groups.empty()) {
			result += "\n";
		}
		result += aggregates[i]->GetName();
		if (aggregate.filter) {
			result += " Filter: " + aggregate.filter->GetName();
		}
	}
	return result;
}

} // namespace duckdb
