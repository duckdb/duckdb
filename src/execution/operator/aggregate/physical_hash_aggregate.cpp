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
				filter_indexes[aggr.filter.get()] = bound_ref_expr.index;
				bound_ref_expr.index = aggregate_input_idx++; // PUT THIS BACK IF STUFF STARTS BREAKING
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
		// FIXME: missing filter_set??
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

	auto &distinct_state = grouping_gstate.distinct_state;
	auto &distinct_data = groupings[grouping_idx].distinct_data;

	DataChunk empty_chunk;

	idx_t total_child_count = grouped_aggregate_data.payload_types.size() - grouped_aggregate_data.filter_count;
	idx_t filter_count = 0;
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
			// FIXME: might be better to just use the filtered_data.filtered_payload directly?
			DataChunk filter_chunk;
			auto &filtered_data = sink.filter_set.GetFilterData(idx);
			filter_chunk.InitializeEmpty(filtered_data.filtered_payload.GetTypes());

			// Add the filter Vector (BOOL)
			auto it = filter_indexes.find(aggregate.filter.get());
			D_ASSERT(it != filter_indexes.end());
			D_ASSERT(it->second < input.data.size());
			filter_chunk.data[total_child_count + filter_count].Reference(input.data[it->second]);
			filter_count++;

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

			radix_table.Sink(context, radix_global_sink, radix_local_sink, filtered_input, empty_chunk,
			                 AggregateType::DISTINCT);
		} else {
			radix_table.Sink(context, radix_global_sink, radix_local_sink, input, empty_chunk, AggregateType::DISTINCT);
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

	void AggregateDistinctGrouping(DistinctAggregateCollectionInfo &info,
	                               const HashAggregateGroupingData &grouping_data,
	                               HashAggregateGroupingGlobalState &grouping_state, idx_t grouping_idx) {
		auto &aggregates = info.aggregates;
		auto &data = *grouping_data.distinct_data;
		auto &state = *grouping_state.distinct_state;

		ThreadContext temp_thread_context(context);
		ExecutionContext temp_exec_context(context, temp_thread_context);

		auto temp_local_state = grouping_data.table_data.GetLocalSinkState(temp_exec_context);

		// Create a chunk that mimics the 'input' chunk in Sink, for storing the group vectors
		DataChunk group_chunk;
		if (!op.input_group_types.empty()) {
			group_chunk.Initialize(context, op.input_group_types);
		}

		auto &groups = op.grouped_aggregate_data.groups;
		// Retrieve the stored data from the hashtable
		const idx_t group_by_size = groups.size();

		DataChunk aggregate_input_chunk;
		if (!gstate.payload_types.empty()) {
			aggregate_input_chunk.Initialize(context, gstate.payload_types);
		}

		vector<unique_ptr<GlobalSourceState>> global_sources(aggregates.size());
		vector<unique_ptr<LocalSourceState>> local_sources(aggregates.size());

		for (auto &idx : info.indices) {
			D_ASSERT(data.info.table_map.count(idx));
			auto table_idx = data.info.table_map.at(idx);
			auto &radix_table_p = data.radix_tables[table_idx];

			// Create global and local state for the hashtable
			if (global_sources[table_idx] == nullptr) {
				global_sources[table_idx] = radix_table_p->GetGlobalSourceState(context);
				local_sources[table_idx] = radix_table_p->GetLocalSourceState(temp_exec_context);
			}
		}

		while (true) {
			idx_t distinct_agg_count = 0;

			idx_t payload_idx = 0;
			idx_t next_payload_idx = 0;

			// Fill the 'aggregate_input_chunk' with the data from all the distinct aggregates, then sink
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
				auto &output_chunk = *state.distinct_output_chunks[table_idx];

				// ????
				output_chunk.Reset();
				radix_table_p->GetData(temp_exec_context, output_chunk, *state.radix_states[table_idx],
				                       *global_sources[table_idx], *local_sources[table_idx]);

				// FIXME: if this is the case, is this true for all aggregates??
				if (output_chunk.size() == 0) {
					goto finished_scan;
				}

				auto &grouped_aggregate_data = *data.grouped_aggregate_data[table_idx];

				// Skip the group_by vectors (located at the start of the 'groups' vector)
				// Map from the output_chunk to the aggregate_input_chunk, using the child expressions

				if (!distinct_agg_count) {
					// Only need to fetch the group for one of the aggregates, as they are identical
					for (idx_t group_idx = 0; group_idx < group_by_size; group_idx++) {
						auto &group = grouped_aggregate_data.groups[group_idx];
						auto &bound_ref_expr = (BoundReferenceExpression &)*group;
						group_chunk.data[bound_ref_expr.index].Reference(output_chunk.data[group_idx]);
					}
					group_chunk.SetCardinality(output_chunk);
				}

				for (idx_t child_idx = 0; child_idx < grouped_aggregate_data.groups.size() - group_by_size;
				     child_idx++) {
					aggregate_input_chunk.data[payload_idx + child_idx].Reference(
					    output_chunk.data[group_by_size + child_idx]);
				}
				aggregate_input_chunk.SetCardinality(output_chunk);
				distinct_agg_count++;
			}
			// Sink it into the main ht
			// AHA I need to populate the aggregate_input_chunk with all data for the given grouping, THEN sink
			grouping_data.table_data.Sink(temp_exec_context, *grouping_state.table_state, *temp_local_state,
			                              group_chunk, aggregate_input_chunk, AggregateType::DISTINCT);
		}
	finished_scan:
		// FIXME: this is needed?
		grouping_data.table_data.Combine(temp_exec_context, *grouping_state.table_state, *temp_local_state);
	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		D_ASSERT(op.distinct_collection_info);
		auto &info = *op.distinct_collection_info;
		for (idx_t i = 0; i < op.groupings.size(); i++) {
			auto &grouping = op.groupings[i];
			auto &grouping_state = gstate.grouping_states[i];
			AggregateDistinctGrouping(info, grouping, grouping_state, i);
		}
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
		auto new_event = make_shared<HashAggregateFinalizeEvent>(*this, gstate, &pipeline);
		event.InsertEvent(move(new_event));
	}
	return SinkFinalizeType::READY;
}

SinkFinalizeType PhysicalHashAggregate::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                 GlobalSinkState &gstate_p) const {
	// return Finalize(pipeline, event, context, gstate_p, false); // DELETE ME
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
