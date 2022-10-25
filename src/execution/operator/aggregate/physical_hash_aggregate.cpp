#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"

#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/aggregate_hashtable.hpp"
#include "duckdb/execution/partitionable_hashtable.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/parallel/base_pipeline_event.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/execution/operator/aggregate/distinct_aggregate_data.hpp"

namespace duckdb {

HashAggregateGroupingData::HashAggregateGroupingData(GroupingSet &grouping_set_p,
                                                     const GroupedAggregateData &grouped_aggregate_data,
                                                     unique_ptr<DistinctAggregateCollectionInfo> &info)
    : table_data(grouping_set_p, grouped_aggregate_data) {
	if (info) {
		distinct_data = make_unique<DistinctAggregateData>(*info);
	}
}

bool HashAggregateGroupingData::HasDistinct() const {
	return distinct_data != nullptr;
}

HashAggregateGroupingGlobalState::HashAggregateGroupingGlobalState(const HashAggregateGroupingData &data,
                                                                   ClientContext &context) {
	table_state = data.table_data.GetGlobalSinkState(context);
	if (data.HasDistinct()) {
		distinct_state = make_unique<DistinctAggregateState>(*data.distinct_data, context);
	}
}

HashAggregateGroupingLocalState::HashAggregateGroupingLocalState(const PhysicalHashAggregate &op,
                                                                 const HashAggregateGroupingData &data,
                                                                 ExecutionContext &context) {
	table_state = data.table_data.GetLocalSinkState(context);
	if (!data.HasDistinct()) {
		return;
	}
	auto &distinct_data = *data.distinct_data;

	auto &distinct_indices = op.distinct_collection_info->Indices();
	D_ASSERT(!distinct_indices.empty());

	distinct_states.resize(op.distinct_collection_info->aggregates.size());
	auto &table_map = op.distinct_collection_info->table_map;

	for (auto &idx : distinct_indices) {
		idx_t table_idx = table_map[idx];
		// FIXME: make this data accessible in the DistinctAggregateCollectionInfo
		if (data.distinct_data->radix_tables[table_idx] == nullptr) {
			// This aggregate has identical input as another aggregate, so no table is created for it
			continue;
		}
		// Initialize the states of the radix tables used for the distinct aggregates
		auto &radix_table = *data.distinct_data->radix_tables[table_idx];
		distinct_states[table_idx] = radix_table.GetLocalSinkState(context);
	}

	distinct_states.reserve(distinct_data.radix_tables.size());
	for (idx_t i = 0; i < distinct_data.radix_tables.size(); i++) {
		distinct_states.push_back(distinct_data.radix_tables[i]->GetLocalSinkState(context));
	}
}

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
	// Because everything that lives in this class should be read only at execution time
	idx_t aggregate_input_idx = 0;
	for (auto &aggregate : aggregates) {
		auto &aggr = (BoundAggregateExpression &)*aggregate;
		aggregate_input_idx += aggr.children.size();
	}
	vector<idx_t> distinct_indices;
	for (idx_t i = 0; i < aggregates.size(); i++) {
		auto &aggregate = aggregates[i];
		auto &aggr = (BoundAggregateExpression &)*aggregate;
		if (aggr.IsDistinct()) {
			distinct_indices.push_back(i);
		}
		if (aggr.filter) {
			auto &bound_ref_expr = (BoundReferenceExpression &)*aggr.filter;
			if (!filter_indexes.count(aggr.filter.get())) {
				// Replace the bound reference expression's index with the corresponding index into the payload chunk
				// FIXME: this adds only
				filter_indexes[aggr.filter.get()] = bound_ref_expr.index;
				bound_ref_expr.index = aggregate_input_idx;
			}
			aggregate_input_idx++;
		}
	}

	if (!distinct_indices.empty()) {
		distinct_collection_info =
		    make_unique<DistinctAggregateCollectionInfo>(grouped_aggregate_data.aggregates, move(distinct_indices));
	}

	for (idx_t i = 0; i < grouping_sets.size(); i++) {
		groupings.emplace_back(grouping_sets[i], grouped_aggregate_data, distinct_collection_info);
	}
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class HashAggregateGlobalState : public GlobalSinkState {
public:
	HashAggregateGlobalState(const PhysicalHashAggregate &op, ClientContext &context) {
		grouping_states.reserve(op.groupings.size());
		for (idx_t i = 0; i < op.groupings.size(); i++) {
			auto &grouping = op.groupings[i];
			grouping_states.emplace_back(grouping, context);
		}
	}

	vector<HashAggregateGroupingGlobalState> grouping_states;
	//! Whether or not the aggregate is finished
	bool finished = false;
};

class HashAggregateLocalState : public LocalSinkState {
public:
	HashAggregateLocalState(const PhysicalHashAggregate &op, ExecutionContext &context) {

		auto &payload_types = op.grouped_aggregate_data.payload_types;
		if (!payload_types.empty()) {
			aggregate_input_chunk.InitializeEmpty(payload_types);
		}

		grouping_states.reserve(op.groupings.size());
		for (auto &grouping : op.groupings) {
			grouping_states.emplace_back(op, grouping, context);
		}
		// FIXME: missing filter_set??
	}

	DataChunk aggregate_input_chunk;
	vector<HashAggregateGroupingLocalState> grouping_states;
};

void PhysicalHashAggregate::SetMultiScan(GlobalSinkState &state) {
	auto &gstate = (HashAggregateGlobalState &)state;
	for (auto &grouping_state : gstate.grouping_states) {
		auto &radix_state = grouping_state.table_state;
		RadixPartitionedHashTable::SetMultiScan(*radix_state);
		if (!grouping_state.distinct_state) {
			continue;
		}
		// FIXME: what does this do? is this the right action to take here ?
		for (auto &distinct_radix_state : grouping_state.distinct_state->radix_states) {
			if (!distinct_radix_state) {
				// Table unused
				continue;
			}
			RadixPartitionedHashTable::SetMultiScan(*distinct_radix_state);
		}
	}
}

unique_ptr<GlobalSinkState> PhysicalHashAggregate::GetGlobalSinkState(ClientContext &context) const {
	return make_unique<HashAggregateGlobalState>(*this, context);
}

unique_ptr<LocalSinkState> PhysicalHashAggregate::GetLocalSinkState(ExecutionContext &context) const {
	return make_unique<HashAggregateLocalState>(*this, context);
}

void PhysicalHashAggregate::SinkDistinctGrouping(ExecutionContext &context, GlobalSinkState &state,
                                                 LocalSinkState &lstate, DataChunk &input, idx_t grouping_idx) const {
	auto &sink = (HashAggregateLocalState &)lstate;
	auto &global_sink = (HashAggregateGlobalState &)state;

	auto &grouping_gstate = global_sink.grouping_states[grouping_idx];
	auto &grouping_lstate = sink.grouping_states[grouping_idx];
	auto &distinct_info = *distinct_collection_info;

	auto &distinct_indices = distinct_info.Indices();
	auto &distinct_state = grouping_gstate.distinct_state;
	auto &distinct_data = groupings[grouping_idx].distinct_data;
	for (auto &idx : distinct_indices) {
		auto &aggregate = (BoundAggregateExpression &)*grouped_aggregate_data.aggregates[idx];

		idx_t table_idx = distinct_info.table_map[idx];
		if (!distinct_data->radix_tables[table_idx]) {
			continue;
		}
		D_ASSERT(distinct_data->radix_tables[table_idx]);
		auto &radix_table = *distinct_data->radix_tables[table_idx];
		auto &radix_global_sink = *distinct_state->radix_states[table_idx];
		auto &radix_local_sink = *grouping_lstate.distinct_states[table_idx];

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
		radix_table.Sink(context, radix_global_sink, radix_local_sink, input, input, AggregateType::DISTINCT);
	}
}

void PhysicalHashAggregate::SinkDistinct(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate,
                                         DataChunk &input) const {
	for (idx_t i = 0; i < groupings.size(); i++) {
		SinkDistinctGrouping(context, state, lstate, input, i);
	}
}

SinkResultType PhysicalHashAggregate::Sink(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate,
                                           DataChunk &input) const {
	auto &llstate = (HashAggregateLocalState &)lstate;
	auto &gstate = (HashAggregateGlobalState &)state;

	if (distinct_collection_info) {
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

	// For every grouping set there is one radix_table
	for (idx_t i = 0; i < groupings.size(); i++) {
		auto &grouping_gstate = gstate.grouping_states[i];
		auto &grouping_lstate = llstate.grouping_states[i];

		auto &grouping = groupings[i];
		auto &table = grouping.table_data;
		table.Sink(context, *grouping_gstate.table_state, *grouping_lstate.table_state, input, aggregate_input_chunk,
		           AggregateType::NON_DISTINCT);
	}

	return SinkResultType::NEED_MORE_INPUT;
}

void PhysicalHashAggregate::CombineDistinct(ExecutionContext &context, GlobalSinkState &state,
                                            LocalSinkState &lstate) const {
	auto &global_sink = (HashAggregateGlobalState &)state;
	auto &sink = (HashAggregateLocalState &)lstate;

	if (!distinct_collection_info) {
		return;
	}
	for (idx_t i = 0; i < groupings.size(); i++) {
		auto &grouping_gstate = global_sink.grouping_states[i];
		auto &grouping_lstate = sink.grouping_states[i];

		auto &distinct_data = groupings[i].distinct_data;
		auto &distinct_state = grouping_gstate.distinct_state;

		const auto table_count = distinct_data->radix_tables.size();
		for (idx_t table_idx = 0; table_idx < table_count; table_idx++) {
			if (!distinct_data->radix_tables[table_idx]) {
				continue;
			}
			auto &radix_table = *distinct_data->radix_tables[table_idx];
			auto &radix_global_sink = *distinct_state->radix_states[table_idx];
			auto &radix_local_sink = *grouping_lstate.distinct_states[table_idx];

			radix_table.Combine(context, radix_global_sink, radix_local_sink);
		}
	}
}

void PhysicalHashAggregate::Combine(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate) const {
	auto &gstate = (HashAggregateGlobalState &)state;
	auto &llstate = (HashAggregateLocalState &)lstate;

	CombineDistinct(context, state, lstate);

	for (idx_t i = 0; i < groupings.size(); i++) {
		auto &grouping_gstate = gstate.grouping_states[i];
		auto &grouping_lstate = llstate.grouping_states[i];

		auto &grouping = groupings[i];
		auto &table = grouping.table_data;
		table.Combine(context, *grouping_gstate.table_state, *grouping_lstate.table_state);
	}
}

//! REGULAR FINALIZE EVENT

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
		for (idx_t i = 0; i < op.groupings.size(); i++) {
			auto &grouping_gstate = gstate.grouping_states[i];

			auto &grouping = op.groupings[i];
			auto &table = grouping.table_data;
			table.ScheduleTasks(pipeline->executor, shared_from_this(), *grouping_gstate.table_state, tasks);
		}
		D_ASSERT(!tasks.empty());
		SetTasks(move(tasks));
	}
};

//! DISTINCT FINALIZE TASK

class HashDistinctAggregateFinalizeTask : public ExecutorTask {
public:
	HashDistinctAggregateFinalizeTask(Pipeline &pipeline, shared_ptr<Event> event_p, HashAggregateGlobalState &state_p,
	                                  ClientContext &context, const PhysicalHashAggregate &op)
	    : ExecutorTask(pipeline.executor), pipeline(pipeline), event(move(event_p)), gstate(state_p), context(context),
	      op(op) {
	}

	void AggregateDistinctGrouping(DistinctAggregateCollectionInfo &info, DistinctAggregateData &data,
	                               DistinctAggregateState &state) {
		auto &aggregates = info.aggregates;
		auto &payload_chunk = state.payload_chunk;

		ThreadContext temp_thread_context(context);
		ExecutionContext temp_exec_context(context, temp_thread_context);

		idx_t payload_idx = 0;
		idx_t next_payload_idx = 0;

		//! Copy of the payload chunk, used to store the data of the radix table for use with the expression executor
		//! We can not directly use the payload chunk because the input and the output to the expression executor can
		//! not be the same Vector
		DataChunk expression_executor_input;
		expression_executor_input.InitializeEmpty(payload_chunk.GetTypes());
		expression_executor_input.SetCardinality(0);

		for (idx_t i = 0; i < aggregates.size(); i++) {
			auto &aggregate = (BoundAggregateExpression &)*aggregates[i];

			// Forward the payload idx
			payload_idx = next_payload_idx;
			next_payload_idx = payload_idx + aggregate.children.size();

			// If aggregate is not distinct, skip it
			if (!data.IsDistinct(i)) {
				continue;
			}
			D_ASSERT(data.info.table_map.count(i));
			auto table_idx = data.info.table_map.at(i);
			auto &radix_table_p = data.radix_tables[table_idx];
			auto &output_chunk = *state.distinct_output_chunks[table_idx];

			//! Create global and local state for the hashtable
			auto global_source_state = radix_table_p->GetGlobalSourceState(context);
			auto local_source_state = radix_table_p->GetLocalSourceState(temp_exec_context);

			//! Retrieve the stored data from the hashtable
			while (true) {
				payload_chunk.Reset();
				output_chunk.Reset();
				radix_table_p->GetData(temp_exec_context, output_chunk, *state.radix_states[table_idx],
				                       *global_source_state, *local_source_state);
				if (output_chunk.size() == 0) {
					break;
				}

				for (idx_t child_idx = 0; child_idx < aggregate.children.size(); child_idx++) {
					expression_executor_input.data[payload_idx + child_idx].Reference(output_chunk.data[child_idx]);
				}
				expression_executor_input.SetCardinality(output_chunk);
				// We dont need to resolve the filter, we already did this in Sink
				state.child_executor.SetChunk(expression_executor_input);

				payload_chunk.SetCardinality(output_chunk);

				//#ifdef DEBUG
				//				gstate.state.counts[i] += payload_chunk.size();
				//#endif

				// resolve the child expressions of the aggregate (if any)
				idx_t payload_cnt = 0;
				for (auto &child : aggregate.children) {
					//! Before executing, remap the indices to point to the payload_chunk
					//! Originally these indices correspond to the 'input' chunk
					//! So we need to filter out the data that was used for filters (if any)
					auto &child_ref = (BoundReferenceExpression &)*child;
					child_ref.index = payload_idx + payload_cnt;

					//! The child_executor contains a pointer to the expression we altered above
					state.child_executor.ExecuteExpression(payload_idx + payload_cnt,
					                                       payload_chunk.data[payload_idx + payload_cnt]);
					payload_cnt++;
				}

				auto start_of_input = payload_cnt ? &payload_chunk.data[payload_idx] : nullptr;
				//! Update the aggregate state
				AggregateInputData aggr_input_data(aggregate.bind_info.get(), Allocator::DefaultAllocator());
				// aggregate.function.simple_update(start_of_input, aggr_input_data, payload_cnt,
				//                                  gstate.state.aggregates[i].get(), payload_chunk.size());
			}
		}
		D_ASSERT(!gstate.finished);
		gstate.finished = true;
	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		D_ASSERT(op.distinct_collection_info);
		auto &info = *op.distinct_collection_info;
		for (idx_t i = 0; i < op.groupings.size(); i++) {
			auto &grouping = op.groupings[i];
			auto &distinct_data = *grouping.distinct_data;
			auto &distinct_state = *gstate.grouping_states[i].distinct_state;
			AggregateDistinctGrouping(info, distinct_data, distinct_state);
		}
		op.Finalize(pipeline, *event, context, gstate, false);
		event->FinishTask();
		return TaskExecutionResult::TASK_FINISHED;
	}

private:
	Pipeline &pipeline;
	shared_ptr<Event> event;
	HashAggregateGlobalState &gstate;
	ClientContext &context;
	const PhysicalHashAggregate &op;
};

//! DISTINCT FINALIZE EVENT

// TODO: Create tasks and run these in parallel instead of doing this all in Schedule, single threaded
class HashDistinctAggregateFinalizeEvent : public BasePipelineEvent {
public:
	HashDistinctAggregateFinalizeEvent(const PhysicalHashAggregate &op_p, HashAggregateGlobalState &gstate_p,
	                                   Pipeline &pipeline_p, ClientContext &context)
	    : BasePipelineEvent(pipeline_p), op(op_p), gstate(gstate_p), context(context) {
	}
	const PhysicalHashAggregate &op;
	HashAggregateGlobalState &gstate;
	ClientContext &context;

public:
	void Schedule() override {
		vector<unique_ptr<Task>> tasks;
		tasks.push_back(
		    make_unique<HashDistinctAggregateFinalizeTask>(*pipeline, shared_from_this(), gstate, context, op));
		D_ASSERT(!tasks.empty());
		SetTasks(move(tasks));
	}
};

//! DISTINCT COMBINE EVENT

class HashDistinctCombineFinalizeEvent : public BasePipelineEvent {
public:
	HashDistinctCombineFinalizeEvent(const PhysicalHashAggregate &op_p, HashAggregateGlobalState &gstate_p,
	                                 Pipeline &pipeline_p, ClientContext &client)
	    : BasePipelineEvent(pipeline_p), op(op_p), gstate(gstate_p), client(client) {
	}

	const PhysicalHashAggregate &op;
	HashAggregateGlobalState &gstate;
	ClientContext &client;

public:
	void Schedule() override {
		vector<unique_ptr<Task>> tasks;
		for (idx_t i = 0; i < op.groupings.size(); i++) {
			auto &grouping = op.groupings[i];
			auto &distinct_data = *grouping.distinct_data;
			auto &distinct_state = *gstate.grouping_states[i].distinct_state;
			for (idx_t table_idx = 0; table_idx < distinct_data.radix_tables.size(); table_idx++) {
				if (!distinct_data.radix_tables[table_idx]) {
					continue;
				}
				distinct_data.radix_tables[table_idx]->ScheduleTasks(pipeline->executor, shared_from_this(),
				                                                     *distinct_state.radix_states[table_idx], tasks);
			}
		}

		//! Now that all tables are combined, it's time to do the distinct aggregations
		auto new_event = make_shared<HashDistinctAggregateFinalizeEvent>(op, gstate, *pipeline, client);
		this->InsertEvent(move(new_event));

		D_ASSERT(!tasks.empty());
		SetTasks(move(tasks));
	}
};

//! FINALIZE

SinkFinalizeType PhysicalHashAggregate::FinalizeDistinct(Pipeline &pipeline, Event &event, ClientContext &context,
                                                         GlobalSinkState &gstate_p) const {
	auto &gstate = (HashAggregateGlobalState &)gstate_p;
	D_ASSERT(distinct_collection_info);
	auto &distinct_info = *distinct_collection_info;

	//! Copy of the payload chunk, used to store the data of the radix table for use with the expression executor
	//! We can not directly use the payload chunk because the input and the output to the expression executor can not
	//! be the same Vector
	bool any_partitioned = false;
	for (idx_t i = 0; i < groupings.size(); i++) {
		auto &grouping = groupings[i];
		auto &distinct_data = *grouping.distinct_data;
		auto &distinct_state = *gstate.grouping_states[i].distinct_state;
		auto &payload_chunk = distinct_state.payload_chunk;
		DataChunk expression_executor_input;
		expression_executor_input.InitializeEmpty(payload_chunk.GetTypes());
		expression_executor_input.SetCardinality(0);

		for (idx_t table_idx = 0; table_idx < distinct_data.radix_tables.size(); table_idx++) {
			if (!distinct_data.radix_tables[i]) {
				continue;
			}
			auto &radix_table = distinct_data.radix_tables[table_idx];
			auto &radix_state = *distinct_state.radix_states[table_idx];
			bool partitioned = radix_table->Finalize(context, radix_state);
			if (partitioned) {
				any_partitioned = true;
			}
		}
	}
	if (any_partitioned) {
		// If any of the groupings are partitioned then we first need to combine those, then aggregate
		auto new_event = make_shared<HashDistinctCombineFinalizeEvent>(*this, gstate, pipeline, context);
		event.InsertEvent(move(new_event));
	} else {
		//! Hashtables aren't partitioned, they dont need to be joined first
		//! So we can compute the aggregate already
		auto new_event = make_shared<HashDistinctAggregateFinalizeEvent>(*this, gstate, pipeline, context);
		event.InsertEvent(move(new_event));
	}
	return SinkFinalizeType::READY;
}

SinkFinalizeType PhysicalHashAggregate::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                 GlobalSinkState &gstate_p, bool check_distinct) const {
	auto &gstate = (HashAggregateGlobalState &)gstate_p;

	if (check_distinct && distinct_collection_info) {
		// There are distinct aggregates
		// If these are partitioned those need to be combined first
		// Then we Finalize again, skipping this step
		return FinalizeDistinct(pipeline, event, context, gstate_p);
	}

	bool any_partitioned = false;
	for (idx_t i = 0; i < groupings.size(); i++) {
		auto &grouping = groupings[i];
		auto &grouping_gstate = gstate.grouping_states[i];

		bool is_partitioned = grouping.table_data.Finalize(context, *grouping_gstate.table_state);
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

SinkFinalizeType PhysicalHashAggregate::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                 GlobalSinkState &gstate_p) const {
	return Finalize(pipeline, event, context, gstate_p, false); // DELETE ME
	// return Finalize(pipeline, event, context, gstate_p, true); //
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class PhysicalHashAggregateGlobalSourceState : public GlobalSourceState {
public:
	PhysicalHashAggregateGlobalSourceState(ClientContext &context, const PhysicalHashAggregate &op)
	    : op(op), state_index(0) {
		for (auto &grouping : op.groupings) {
			auto &rt = grouping.table_data;
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
		for (auto &grouping : op.groupings) {
			auto &rt = grouping.table_data;
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
	auto &sink_gstate = (HashAggregateGlobalState &)*sink_state;
	auto &gstate = (PhysicalHashAggregateGlobalSourceState &)gstate_p;
	auto &lstate = (PhysicalHashAggregateLocalSourceState &)lstate_p;
	for (size_t sidx = gstate.state_index; sidx < groupings.size(); sidx = ++gstate.state_index) {
		auto &grouping = groupings[sidx];
		auto &table = grouping.table_data;

		auto &grouping_gstate = sink_gstate.grouping_states[sidx];

		table.GetData(context, chunk, *grouping_gstate.table_state, *gstate.radix_states[sidx],
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
