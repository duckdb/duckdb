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
		distinct_data = make_unique<DistinctAggregateData>(*info, grouping_set_p, &grouped_aggregate_data.groups);
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
		auto &radix_table = distinct_data.radix_tables[table_idx];
		if (radix_table == nullptr) {
			// This aggregate has identical input as another aggregate, so no table is created for it
			continue;
		}
		// Initialize the states of the radix tables used for the distinct aggregates
		distinct_states[table_idx] = radix_table->GetLocalSinkState(context);
	}
}

static vector<LogicalType> CreateGroupChunkTypes(vector<unique_ptr<Expression>> &groups) {
	set<idx_t> group_indices;

	if (groups.empty()) {
		return {};
	}

	for (auto &group : groups) {
		D_ASSERT(group->type == ExpressionType::BOUND_REF);
		auto &bound_ref = (BoundReferenceExpression &)*group;
		group_indices.insert(bound_ref.index);
	}
	idx_t highest_index = *group_indices.rbegin();
	vector<LogicalType> types(highest_index + 1, LogicalType::SQLNULL);
	for (auto &group : groups) {
		auto &bound_ref = (BoundReferenceExpression &)*group;
		types[bound_ref.index] = bound_ref.return_type;
	}
	return types;
}

bool PhysicalHashAggregate::CanSkipRegularSink() const {
	if (!filter_indexes.empty()) {
		// If we have filters, we can't skip the regular sink, because we might lose groups otherwise.
		return false;
	}
	if (grouped_aggregate_data.aggregates.empty()) {
		// When there are no aggregates, we have to add to the main ht right away
		return false;
	}
	if (!non_distinct_filter.empty()) {
		return false;
	}
	return true;
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
	input_group_types = CreateGroupChunkTypes(groups_p);

	grouped_aggregate_data.InitializeGroupby(move(groups_p), move(expressions), move(grouping_functions_p));

	auto &aggregates = grouped_aggregate_data.aggregates;
	// filter_indexes must be pre-built, not lazily instantiated in parallel...
	// Because everything that lives in this class should be read-only at execution time
	idx_t aggregate_input_idx = 0;
	for (idx_t i = 0; i < aggregates.size(); i++) {
		auto &aggregate = aggregates[i];
		auto &aggr = (BoundAggregateExpression &)*aggregate;
		aggregate_input_idx += aggr.children.size();
		if (aggr.aggr_type == AggregateType::DISTINCT) {
			distinct_filter.push_back(i);
		} else if (aggr.aggr_type == AggregateType::NON_DISTINCT) {
			non_distinct_filter.push_back(i);
		} else { // LCOV_EXCL_START
			throw NotImplementedException("AggregateType not implemented in PhysicalHashAggregate");
		} // LCOV_EXCL_STOP
	}

	for (idx_t i = 0; i < aggregates.size(); i++) {
		auto &aggregate = aggregates[i];
		auto &aggr = (BoundAggregateExpression &)*aggregate;
		if (aggr.filter) {
			auto &bound_ref_expr = (BoundReferenceExpression &)*aggr.filter;
			if (!filter_indexes.count(aggr.filter.get())) {
				// Replace the bound reference expression's index with the corresponding index of the payload chunk
				filter_indexes[aggr.filter.get()] = bound_ref_expr.index;
				bound_ref_expr.index = aggregate_input_idx;
			}
			aggregate_input_idx++;
		}
	}

	distinct_collection_info = DistinctAggregateCollectionInfo::Create(grouped_aggregate_data.aggregates);

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
		vector<LogicalType> filter_types;
		for (auto &aggr : op.grouped_aggregate_data.aggregates) {
			auto &aggregate = (BoundAggregateExpression &)*aggr;
			for (auto &child : aggregate.children) {
				payload_types.push_back(child->return_type);
			}
			if (aggregate.filter) {
				filter_types.push_back(aggregate.filter->return_type);
			}
		}
		payload_types.reserve(payload_types.size() + filter_types.size());
		payload_types.insert(payload_types.end(), filter_types.begin(), filter_types.end());
	}

	vector<HashAggregateGroupingGlobalState> grouping_states;
	vector<LogicalType> payload_types;
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
		// The filter set is only needed here for the distinct aggregates
		// the filtering of data for the regular aggregates is done within the hashtable
		vector<AggregateObject> aggregate_objects;
		for (auto &aggregate : op.grouped_aggregate_data.aggregates) {
			auto &aggr = (BoundAggregateExpression &)*aggregate;
			aggregate_objects.emplace_back(&aggr);
		}

		filter_set.Initialize(Allocator::Get(context.client), aggregate_objects, payload_types);
	}

	DataChunk aggregate_input_chunk;
	vector<HashAggregateGroupingLocalState> grouping_states;
	AggregateFilterDataSet filter_set;
};

void PhysicalHashAggregate::SetMultiScan(GlobalSinkState &state) {
	auto &gstate = (HashAggregateGlobalState &)state;
	for (auto &grouping_state : gstate.grouping_states) {
		auto &radix_state = grouping_state.table_state;
		RadixPartitionedHashTable::SetMultiScan(*radix_state);
		if (!grouping_state.distinct_state) {
			continue;
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

	auto &distinct_state = grouping_gstate.distinct_state;
	auto &distinct_data = groupings[grouping_idx].distinct_data;

	DataChunk empty_chunk;

	// Create an empty filter for Sink, since we don't need to update any aggregate states here
	vector<idx_t> empty_filter;

	for (idx_t &idx : distinct_info.indices) {
		auto &aggregate = (BoundAggregateExpression &)*grouped_aggregate_data.aggregates[idx];

		D_ASSERT(distinct_info.table_map.count(idx));
		idx_t table_idx = distinct_info.table_map[idx];
		if (!distinct_data->radix_tables[table_idx]) {
			continue;
		}
		D_ASSERT(distinct_data->radix_tables[table_idx]);
		auto &radix_table = *distinct_data->radix_tables[table_idx];
		auto &radix_global_sink = *distinct_state->radix_states[table_idx];
		auto &radix_local_sink = *grouping_lstate.distinct_states[table_idx];

		if (aggregate.filter) {
			DataChunk filter_chunk;
			auto &filtered_data = sink.filter_set.GetFilterData(idx);
			filter_chunk.InitializeEmpty(filtered_data.filtered_payload.GetTypes());

			// Add the filter Vector (BOOL)
			auto it = filter_indexes.find(aggregate.filter.get());
			D_ASSERT(it != filter_indexes.end());
			D_ASSERT(it->second < input.data.size());
			auto &filter_bound_ref = (BoundReferenceExpression &)*aggregate.filter;
			filter_chunk.data[filter_bound_ref.index].Reference(input.data[it->second]);
			filter_chunk.SetCardinality(input.size());

			// We cant use the AggregateFilterData::ApplyFilter method, because the chunk we need to
			// apply the filter to also has the groups, and the filtered_data.filtered_payload does not have those.
			SelectionVector sel_vec(STANDARD_VECTOR_SIZE);
			idx_t count = filtered_data.filter_executor.SelectExpression(filter_chunk, sel_vec);

			if (count == 0) {
				continue;
			}

			// Because the 'input' chunk needs to be re-used after this, we need to create
			// a duplicate of it, that we can apply the filter to
			DataChunk filtered_input;
			filtered_input.InitializeEmpty(input.GetTypes());

			for (idx_t group_idx = 0; group_idx < grouped_aggregate_data.groups.size(); group_idx++) {
				auto &group = grouped_aggregate_data.groups[group_idx];
				auto &bound_ref = (BoundReferenceExpression &)*group;
				filtered_input.data[bound_ref.index].Reference(input.data[bound_ref.index]);
			}
			for (idx_t child_idx = 0; child_idx < aggregate.children.size(); child_idx++) {
				auto &child = aggregate.children[child_idx];
				auto &bound_ref = (BoundReferenceExpression &)*child;

				filtered_input.data[bound_ref.index].Reference(input.data[bound_ref.index]);
			}
			filtered_input.Slice(sel_vec, count);
			filtered_input.SetCardinality(count);

			radix_table.Sink(context, radix_global_sink, radix_local_sink, filtered_input, empty_chunk, empty_filter);
		} else {
			radix_table.Sink(context, radix_global_sink, radix_local_sink, input, empty_chunk, empty_filter);
		}
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

	if (CanSkipRegularSink()) {
		return SinkResultType::NEED_MORE_INPUT;
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
		           non_distinct_filter);
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

	if (CanSkipRegularSink()) {
		return;
	}
	for (idx_t i = 0; i < groupings.size(); i++) {
		auto &grouping_gstate = gstate.grouping_states[i];
		auto &grouping_lstate = llstate.grouping_states[i];

		auto &grouping = groupings[i];
		auto &table = grouping.table_data;
		table.Combine(context, *grouping_gstate.table_state, *grouping_lstate.table_state);
	}
}

//! REGULAR FINALIZE EVENT

class HashAggregateMergeEvent : public BasePipelineEvent {
public:
	HashAggregateMergeEvent(const PhysicalHashAggregate &op_p, HashAggregateGlobalState &gstate_p, Pipeline *pipeline_p)
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

//! REGULAR FINALIZE FROM DISTINCT FINALIZE

class HashAggregateFinalizeTask : public ExecutorTask {
public:
	HashAggregateFinalizeTask(Pipeline &pipeline, shared_ptr<Event> event_p, HashAggregateGlobalState &state_p,
	                          ClientContext &context, const PhysicalHashAggregate &op)
	    : ExecutorTask(pipeline.executor), pipeline(pipeline), event(move(event_p)), gstate(state_p), context(context),
	      op(op) {
	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		op.FinalizeInternal(pipeline, *event, context, gstate, false);
		D_ASSERT(!gstate.finished);
		gstate.finished = true;
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

class HashAggregateFinalizeEvent : public BasePipelineEvent {
public:
	HashAggregateFinalizeEvent(const PhysicalHashAggregate &op_p, HashAggregateGlobalState &gstate_p,
	                           Pipeline *pipeline_p, ClientContext &context)
	    : BasePipelineEvent(*pipeline_p), op(op_p), gstate(gstate_p), context(context) {
	}

	const PhysicalHashAggregate &op;
	HashAggregateGlobalState &gstate;
	ClientContext &context;

public:
	void Schedule() override {
		vector<unique_ptr<Task>> tasks;
		tasks.push_back(make_unique<HashAggregateFinalizeTask>(*pipeline, shared_from_this(), gstate, context, op));
		D_ASSERT(!tasks.empty());
		SetTasks(move(tasks));
	}
};

//! DISTINCT FINALIZE TASK

class HashDistinctAggregateFinalizeTask : public ExecutorTask {
public:
	HashDistinctAggregateFinalizeTask(Pipeline &pipeline, shared_ptr<Event> event_p, HashAggregateGlobalState &state_p,
	                                  ClientContext &context, const PhysicalHashAggregate &op,
	                                  vector<vector<unique_ptr<GlobalSourceState>>> &global_sources_p)
	    : ExecutorTask(pipeline.executor), pipeline(pipeline), event(move(event_p)), gstate(state_p), context(context),
	      op(op), global_sources(global_sources_p) {
	}

	void AggregateDistinctGrouping(DistinctAggregateCollectionInfo &info,
	                               const HashAggregateGroupingData &grouping_data,
	                               HashAggregateGroupingGlobalState &grouping_state, idx_t grouping_idx) {
		auto &aggregates = info.aggregates;
		auto &data = *grouping_data.distinct_data;
		auto &state = *grouping_state.distinct_state;
		auto &table_state = *grouping_state.table_state;

		ThreadContext temp_thread_context(context);
		ExecutionContext temp_exec_context(context, temp_thread_context, &pipeline);

		auto temp_local_state = grouping_data.table_data.GetLocalSinkState(temp_exec_context);

		// Create a chunk that mimics the 'input' chunk in Sink, for storing the group vectors
		DataChunk group_chunk;
		if (!op.input_group_types.empty()) {
			group_chunk.Initialize(context, op.input_group_types);
		}

		auto &groups = op.grouped_aggregate_data.groups;
		const idx_t group_by_size = groups.size();

		DataChunk aggregate_input_chunk;
		if (!gstate.payload_types.empty()) {
			aggregate_input_chunk.Initialize(context, gstate.payload_types);
		}

		idx_t payload_idx;
		idx_t next_payload_idx = 0;

		for (idx_t i = 0; i < op.grouped_aggregate_data.aggregates.size(); i++) {
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

			// Create a duplicate of the output_chunk, because of multi-threading we cant alter the original
			DataChunk output_chunk;
			output_chunk.Initialize(context, state.distinct_output_chunks[table_idx]->GetTypes());

			auto &global_source = global_sources[grouping_idx][i];
			auto local_source = radix_table_p->GetLocalSourceState(temp_exec_context);

			// Fetch all the data from the aggregate ht, and Sink it into the main ht
			while (true) {
				output_chunk.Reset();
				group_chunk.Reset();
				aggregate_input_chunk.Reset();
				radix_table_p->GetData(temp_exec_context, output_chunk, *state.radix_states[table_idx], *global_source,
				                       *local_source);

				if (output_chunk.size() == 0) {
					break;
				}

				auto &grouped_aggregate_data = *data.grouped_aggregate_data[table_idx];

				for (idx_t group_idx = 0; group_idx < group_by_size; group_idx++) {
					auto &group = grouped_aggregate_data.groups[group_idx];
					auto &bound_ref_expr = (BoundReferenceExpression &)*group;
					group_chunk.data[bound_ref_expr.index].Reference(output_chunk.data[group_idx]);
				}
				group_chunk.SetCardinality(output_chunk);

				for (idx_t child_idx = 0; child_idx < grouped_aggregate_data.groups.size() - group_by_size;
				     child_idx++) {
					aggregate_input_chunk.data[payload_idx + child_idx].Reference(
					    output_chunk.data[group_by_size + child_idx]);
				}
				aggregate_input_chunk.SetCardinality(output_chunk);

				// Sink it into the main ht
				grouping_data.table_data.Sink(temp_exec_context, table_state, *temp_local_state, group_chunk,
				                              aggregate_input_chunk, {i});
			}
		}
		grouping_data.table_data.Combine(temp_exec_context, table_state, *temp_local_state);
	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		D_ASSERT(op.distinct_collection_info);
		auto &info = *op.distinct_collection_info;
		for (idx_t i = 0; i < op.groupings.size(); i++) {
			auto &grouping = op.groupings[i];
			auto &grouping_state = gstate.grouping_states[i];
			AggregateDistinctGrouping(info, grouping, grouping_state, i);
		}
		event->FinishTask();
		return TaskExecutionResult::TASK_FINISHED;
	}

private:
	Pipeline &pipeline;
	shared_ptr<Event> event;
	HashAggregateGlobalState &gstate;
	ClientContext &context;
	const PhysicalHashAggregate &op;
	vector<vector<unique_ptr<GlobalSourceState>>> &global_sources;
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
	//! The GlobalSourceStates for all the radix tables of the distinct aggregates
	vector<vector<unique_ptr<GlobalSourceState>>> global_sources;

public:
	void Schedule() override {
		global_sources = CreateGlobalSources();

		vector<unique_ptr<Task>> tasks;
		auto &scheduler = TaskScheduler::GetScheduler(context);
		auto number_of_threads = scheduler.NumberOfThreads();
		tasks.reserve(number_of_threads);
		for (int32_t i = 0; i < number_of_threads; i++) {
			tasks.push_back(make_unique<HashDistinctAggregateFinalizeTask>(*pipeline, shared_from_this(), gstate,
			                                                               context, op, global_sources));
		}
		D_ASSERT(!tasks.empty());
		SetTasks(move(tasks));
	}

	void FinishEvent() override {
		//! Now that everything is added to the main ht, we can actually finalize
		auto new_event = make_shared<HashAggregateFinalizeEvent>(op, gstate, pipeline.get(), context);
		this->InsertEvent(move(new_event));
	}

private:
	vector<vector<unique_ptr<GlobalSourceState>>> CreateGlobalSources() {
		vector<vector<unique_ptr<GlobalSourceState>>> grouping_sources;
		grouping_sources.reserve(op.groupings.size());
		for (idx_t grouping_idx = 0; grouping_idx < op.groupings.size(); grouping_idx++) {
			auto &grouping = op.groupings[grouping_idx];
			auto &data = *grouping.distinct_data;

			vector<unique_ptr<GlobalSourceState>> aggregate_sources;
			aggregate_sources.reserve(op.grouped_aggregate_data.aggregates.size());

			for (idx_t i = 0; i < op.grouped_aggregate_data.aggregates.size(); i++) {
				auto &aggregate = op.grouped_aggregate_data.aggregates[i];
				auto &aggr = (BoundAggregateExpression &)*aggregate;

				if (!aggr.IsDistinct()) {
					aggregate_sources.push_back(nullptr);
					continue;
				}

				D_ASSERT(data.info.table_map.count(i));
				auto table_idx = data.info.table_map.at(i);
				auto &radix_table_p = data.radix_tables[table_idx];
				aggregate_sources.push_back(radix_table_p->GetGlobalSourceState(context));
			}
			grouping_sources.push_back(move(aggregate_sources));
		}
		return grouping_sources;
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

		D_ASSERT(!tasks.empty());
		SetTasks(move(tasks));
	}

	void FinishEvent() override {
		//! Now that all tables are combined, it's time to do the distinct aggregations
		auto new_event = make_shared<HashDistinctAggregateFinalizeEvent>(op, gstate, *pipeline, client);
		this->InsertEvent(move(new_event));
	}
};

//! FINALIZE

SinkFinalizeType PhysicalHashAggregate::FinalizeDistinct(Pipeline &pipeline, Event &event, ClientContext &context,
                                                         GlobalSinkState &gstate_p) const {
	auto &gstate = (HashAggregateGlobalState &)gstate_p;
	D_ASSERT(distinct_collection_info);

	bool any_partitioned = false;
	for (idx_t i = 0; i < groupings.size(); i++) {
		auto &grouping = groupings[i];
		auto &distinct_data = *grouping.distinct_data;
		auto &distinct_state = *gstate.grouping_states[i].distinct_state;

		for (idx_t table_idx = 0; table_idx < distinct_data.radix_tables.size(); table_idx++) {
			if (!distinct_data.radix_tables[table_idx]) {
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
		// Hashtables aren't partitioned, they dont need to be joined first
		// so we can already compute the aggregate
		auto new_event = make_shared<HashDistinctAggregateFinalizeEvent>(*this, gstate, pipeline, context);
		event.InsertEvent(move(new_event));
	}
	return SinkFinalizeType::READY;
}

SinkFinalizeType PhysicalHashAggregate::FinalizeInternal(Pipeline &pipeline, Event &event, ClientContext &context,
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
		auto new_event = make_shared<HashAggregateMergeEvent>(*this, gstate, &pipeline);
		event.InsertEvent(move(new_event));
	}
	return SinkFinalizeType::READY;
}

SinkFinalizeType PhysicalHashAggregate::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                 GlobalSinkState &gstate_p) const {
	return FinalizeInternal(pipeline, event, context, gstate_p, true);
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
	mutex lock;
	atomic<idx_t> state_index;

	vector<unique_ptr<GlobalSourceState>> radix_states;

public:
	idx_t MaxThreads() override {
		// If there are no tables, we only need one thread.
		if (op.groupings.empty()) {
			return 1;
		}

		auto &ht_state = (HashAggregateGlobalState &)*op.sink_state;
		idx_t count = 0;
		for (size_t sidx = 0; sidx < op.groupings.size(); ++sidx) {
			auto &grouping = op.groupings[sidx];
			auto &grouping_gstate = ht_state.grouping_states[sidx];
			count += grouping.table_data.Size(*grouping_gstate.table_state);
		}
		return MaxValue<idx_t>(1, count / RowGroup::ROW_GROUP_SIZE);
	}
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
	while (true) {
		idx_t radix_idx = gstate.state_index;
		if (radix_idx >= groupings.size()) {
			break;
		}
		auto &grouping = groupings[radix_idx];
		auto &radix_table = grouping.table_data;
		auto &grouping_gstate = sink_gstate.grouping_states[radix_idx];
		radix_table.GetData(context, chunk, *grouping_gstate.table_state, *gstate.radix_states[radix_idx],
		                    *lstate.radix_states[radix_idx]);
		if (chunk.size() != 0) {
			return;
		}
		// move to the next table
		lock_guard<mutex> l(gstate.lock);
		radix_idx++;
		if (radix_idx > gstate.state_index) {
			// we have not yet worked on the table
			// move the global index forwards
			gstate.state_index = radix_idx;
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
