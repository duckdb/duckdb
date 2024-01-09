#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"

#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/aggregate_hashtable.hpp"
#include "duckdb/execution/operator/aggregate/distinct_aggregate_data.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/base_pipeline_event.hpp"
#include "duckdb/parallel/interrupt.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

HashAggregateGroupingData::HashAggregateGroupingData(GroupingSet &grouping_set_p,
                                                     const GroupedAggregateData &grouped_aggregate_data,
                                                     unique_ptr<DistinctAggregateCollectionInfo> &info)
    : table_data(grouping_set_p, grouped_aggregate_data) {
	if (info) {
		distinct_data = make_uniq<DistinctAggregateData>(*info, grouping_set_p, &grouped_aggregate_data.groups);
	}
}

bool HashAggregateGroupingData::HasDistinct() const {
	return distinct_data != nullptr;
}

HashAggregateGroupingGlobalState::HashAggregateGroupingGlobalState(const HashAggregateGroupingData &data,
                                                                   ClientContext &context) {
	table_state = data.table_data.GetGlobalSinkState(context);
	if (data.HasDistinct()) {
		distinct_state = make_uniq<DistinctAggregateState>(*data.distinct_data, context);
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
		auto &bound_ref = group->Cast<BoundReferenceExpression>();
		group_indices.insert(bound_ref.index);
	}
	idx_t highest_index = *group_indices.rbegin();
	vector<LogicalType> types(highest_index + 1, LogicalType::SQLNULL);
	for (auto &group : groups) {
		auto &bound_ref = group->Cast<BoundReferenceExpression>();
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
    : PhysicalHashAggregate(context, std::move(types), std::move(expressions), {}, estimated_cardinality) {
}

PhysicalHashAggregate::PhysicalHashAggregate(ClientContext &context, vector<LogicalType> types,
                                             vector<unique_ptr<Expression>> expressions,
                                             vector<unique_ptr<Expression>> groups_p, idx_t estimated_cardinality)
    : PhysicalHashAggregate(context, std::move(types), std::move(expressions), std::move(groups_p), {}, {},
                            estimated_cardinality) {
}

PhysicalHashAggregate::PhysicalHashAggregate(ClientContext &context, vector<LogicalType> types,
                                             vector<unique_ptr<Expression>> expressions,
                                             vector<unique_ptr<Expression>> groups_p,
                                             vector<GroupingSet> grouping_sets_p,
                                             vector<unsafe_vector<idx_t>> grouping_functions_p,
                                             idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::HASH_GROUP_BY, std::move(types), estimated_cardinality),
      grouping_sets(std::move(grouping_sets_p)) {
	// get a list of all aggregates to be computed
	const idx_t group_count = groups_p.size();
	if (grouping_sets.empty()) {
		GroupingSet set;
		for (idx_t i = 0; i < group_count; i++) {
			set.insert(i);
		}
		grouping_sets.push_back(std::move(set));
	}
	input_group_types = CreateGroupChunkTypes(groups_p);

	grouped_aggregate_data.InitializeGroupby(std::move(groups_p), std::move(expressions),
	                                         std::move(grouping_functions_p));

	auto &aggregates = grouped_aggregate_data.aggregates;
	// filter_indexes must be pre-built, not lazily instantiated in parallel...
	// Because everything that lives in this class should be read-only at execution time
	idx_t aggregate_input_idx = 0;
	for (idx_t i = 0; i < aggregates.size(); i++) {
		auto &aggregate = aggregates[i];
		auto &aggr = aggregate->Cast<BoundAggregateExpression>();
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
		auto &aggr = aggregate->Cast<BoundAggregateExpression>();
		if (aggr.filter) {
			auto &bound_ref_expr = aggr.filter->Cast<BoundReferenceExpression>();
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
class HashAggregateGlobalSinkState : public GlobalSinkState {
public:
	HashAggregateGlobalSinkState(const PhysicalHashAggregate &op, ClientContext &context) {
		grouping_states.reserve(op.groupings.size());
		for (idx_t i = 0; i < op.groupings.size(); i++) {
			auto &grouping = op.groupings[i];
			grouping_states.emplace_back(grouping, context);
		}
		vector<LogicalType> filter_types;
		for (auto &aggr : op.grouped_aggregate_data.aggregates) {
			auto &aggregate = aggr->Cast<BoundAggregateExpression>();
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

class HashAggregateLocalSinkState : public LocalSinkState {
public:
	HashAggregateLocalSinkState(const PhysicalHashAggregate &op, ExecutionContext &context) {

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
			auto &aggr = aggregate->Cast<BoundAggregateExpression>();
			aggregate_objects.emplace_back(&aggr);
		}

		filter_set.Initialize(context.client, aggregate_objects, payload_types);
	}

	DataChunk aggregate_input_chunk;
	vector<HashAggregateGroupingLocalState> grouping_states;
	AggregateFilterDataSet filter_set;
};

void PhysicalHashAggregate::SetMultiScan(GlobalSinkState &state) {
	auto &gstate = state.Cast<HashAggregateGlobalSinkState>();
	for (auto &grouping_state : gstate.grouping_states) {
		RadixPartitionedHashTable::SetMultiScan(*grouping_state.table_state);
		if (!grouping_state.distinct_state) {
			continue;
		}
	}
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
unique_ptr<GlobalSinkState> PhysicalHashAggregate::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<HashAggregateGlobalSinkState>(*this, context);
}

unique_ptr<LocalSinkState> PhysicalHashAggregate::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<HashAggregateLocalSinkState>(*this, context);
}

void PhysicalHashAggregate::SinkDistinctGrouping(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input,
                                                 idx_t grouping_idx) const {
	auto &sink = input.local_state.Cast<HashAggregateLocalSinkState>();
	auto &global_sink = input.global_state.Cast<HashAggregateGlobalSinkState>();

	auto &grouping_gstate = global_sink.grouping_states[grouping_idx];
	auto &grouping_lstate = sink.grouping_states[grouping_idx];
	auto &distinct_info = *distinct_collection_info;

	auto &distinct_state = grouping_gstate.distinct_state;
	auto &distinct_data = groupings[grouping_idx].distinct_data;

	DataChunk empty_chunk;

	// Create an empty filter for Sink, since we don't need to update any aggregate states here
	unsafe_vector<idx_t> empty_filter;

	for (idx_t &idx : distinct_info.indices) {
		auto &aggregate = grouped_aggregate_data.aggregates[idx]->Cast<BoundAggregateExpression>();

		D_ASSERT(distinct_info.table_map.count(idx));
		idx_t table_idx = distinct_info.table_map[idx];
		if (!distinct_data->radix_tables[table_idx]) {
			continue;
		}
		D_ASSERT(distinct_data->radix_tables[table_idx]);
		auto &radix_table = *distinct_data->radix_tables[table_idx];
		auto &radix_global_sink = *distinct_state->radix_states[table_idx];
		auto &radix_local_sink = *grouping_lstate.distinct_states[table_idx];

		InterruptState interrupt_state;
		OperatorSinkInput sink_input {radix_global_sink, radix_local_sink, interrupt_state};

		if (aggregate.filter) {
			DataChunk filter_chunk;
			auto &filtered_data = sink.filter_set.GetFilterData(idx);
			filter_chunk.InitializeEmpty(filtered_data.filtered_payload.GetTypes());

			// Add the filter Vector (BOOL)
			auto it = filter_indexes.find(aggregate.filter.get());
			D_ASSERT(it != filter_indexes.end());
			D_ASSERT(it->second < chunk.data.size());
			auto &filter_bound_ref = aggregate.filter->Cast<BoundReferenceExpression>();
			filter_chunk.data[filter_bound_ref.index].Reference(chunk.data[it->second]);
			filter_chunk.SetCardinality(chunk.size());

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
			filtered_input.InitializeEmpty(chunk.GetTypes());

			for (idx_t group_idx = 0; group_idx < grouped_aggregate_data.groups.size(); group_idx++) {
				auto &group = grouped_aggregate_data.groups[group_idx];
				auto &bound_ref = group->Cast<BoundReferenceExpression>();
				filtered_input.data[bound_ref.index].Reference(chunk.data[bound_ref.index]);
			}
			for (idx_t child_idx = 0; child_idx < aggregate.children.size(); child_idx++) {
				auto &child = aggregate.children[child_idx];
				auto &bound_ref = child->Cast<BoundReferenceExpression>();

				filtered_input.data[bound_ref.index].Reference(chunk.data[bound_ref.index]);
			}
			filtered_input.Slice(sel_vec, count);
			filtered_input.SetCardinality(count);

			radix_table.Sink(context, filtered_input, sink_input, empty_chunk, empty_filter);
		} else {
			radix_table.Sink(context, chunk, sink_input, empty_chunk, empty_filter);
		}
	}
}

void PhysicalHashAggregate::SinkDistinct(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	for (idx_t i = 0; i < groupings.size(); i++) {
		SinkDistinctGrouping(context, chunk, input, i);
	}
}

SinkResultType PhysicalHashAggregate::Sink(ExecutionContext &context, DataChunk &chunk,
                                           OperatorSinkInput &input) const {
	auto &local_state = input.local_state.Cast<HashAggregateLocalSinkState>();
	auto &global_state = input.global_state.Cast<HashAggregateGlobalSinkState>();

	if (distinct_collection_info) {
		SinkDistinct(context, chunk, input);
	}

	if (CanSkipRegularSink()) {
		return SinkResultType::NEED_MORE_INPUT;
	}

	DataChunk &aggregate_input_chunk = local_state.aggregate_input_chunk;
	auto &aggregates = grouped_aggregate_data.aggregates;
	idx_t aggregate_input_idx = 0;

	// Populate the aggregate child vectors
	for (auto &aggregate : aggregates) {
		auto &aggr = aggregate->Cast<BoundAggregateExpression>();
		for (auto &child_expr : aggr.children) {
			D_ASSERT(child_expr->type == ExpressionType::BOUND_REF);
			auto &bound_ref_expr = child_expr->Cast<BoundReferenceExpression>();
			D_ASSERT(bound_ref_expr.index < chunk.data.size());
			aggregate_input_chunk.data[aggregate_input_idx++].Reference(chunk.data[bound_ref_expr.index]);
		}
	}
	// Populate the filter vectors
	for (auto &aggregate : aggregates) {
		auto &aggr = aggregate->Cast<BoundAggregateExpression>();
		if (aggr.filter) {
			auto it = filter_indexes.find(aggr.filter.get());
			D_ASSERT(it != filter_indexes.end());
			D_ASSERT(it->second < chunk.data.size());
			aggregate_input_chunk.data[aggregate_input_idx++].Reference(chunk.data[it->second]);
		}
	}

	aggregate_input_chunk.SetCardinality(chunk.size());
	aggregate_input_chunk.Verify();

	// For every grouping set there is one radix_table
	for (idx_t i = 0; i < groupings.size(); i++) {
		auto &grouping_global_state = global_state.grouping_states[i];
		auto &grouping_local_state = local_state.grouping_states[i];
		InterruptState interrupt_state;
		OperatorSinkInput sink_input {*grouping_global_state.table_state, *grouping_local_state.table_state,
		                              interrupt_state};

		auto &grouping = groupings[i];
		auto &table = grouping.table_data;
		table.Sink(context, chunk, sink_input, aggregate_input_chunk, non_distinct_filter);
	}

	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Combine
//===--------------------------------------------------------------------===//
void PhysicalHashAggregate::CombineDistinct(ExecutionContext &context, OperatorSinkCombineInput &input) const {

	auto &global_sink = input.global_state.Cast<HashAggregateGlobalSinkState>();
	auto &sink = input.local_state.Cast<HashAggregateLocalSinkState>();

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

SinkCombineResultType PhysicalHashAggregate::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<HashAggregateGlobalSinkState>();
	auto &llstate = input.local_state.Cast<HashAggregateLocalSinkState>();

	OperatorSinkCombineInput combine_distinct_input {gstate, llstate, input.interrupt_state};
	CombineDistinct(context, combine_distinct_input);

	if (CanSkipRegularSink()) {
		return SinkCombineResultType::FINISHED;
	}
	for (idx_t i = 0; i < groupings.size(); i++) {
		auto &grouping_gstate = gstate.grouping_states[i];
		auto &grouping_lstate = llstate.grouping_states[i];

		auto &grouping = groupings[i];
		auto &table = grouping.table_data;
		table.Combine(context, *grouping_gstate.table_state, *grouping_lstate.table_state);
	}

	return SinkCombineResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
class HashAggregateFinalizeEvent : public BasePipelineEvent {
public:
	//! "Regular" Finalize Event that is scheduled after combining the thread-local distinct HTs
	HashAggregateFinalizeEvent(ClientContext &context, Pipeline *pipeline_p, const PhysicalHashAggregate &op_p,
	                           HashAggregateGlobalSinkState &gstate_p)
	    : BasePipelineEvent(*pipeline_p), context(context), op(op_p), gstate(gstate_p) {
	}

public:
	void Schedule() override;

private:
	ClientContext &context;

	const PhysicalHashAggregate &op;
	HashAggregateGlobalSinkState &gstate;
};

class HashAggregateFinalizeTask : public ExecutorTask {
public:
	HashAggregateFinalizeTask(ClientContext &context, Pipeline &pipeline, shared_ptr<Event> event_p,
	                          const PhysicalHashAggregate &op, HashAggregateGlobalSinkState &state_p)
	    : ExecutorTask(pipeline.executor), context(context), pipeline(pipeline), event(std::move(event_p)), op(op),
	      gstate(state_p) {
	}

public:
	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override;

private:
	ClientContext &context;
	Pipeline &pipeline;
	shared_ptr<Event> event;

	const PhysicalHashAggregate &op;
	HashAggregateGlobalSinkState &gstate;
};

void HashAggregateFinalizeEvent::Schedule() {
	vector<shared_ptr<Task>> tasks;
	tasks.push_back(make_uniq<HashAggregateFinalizeTask>(context, *pipeline, shared_from_this(), op, gstate));
	D_ASSERT(!tasks.empty());
	SetTasks(std::move(tasks));
}

TaskExecutionResult HashAggregateFinalizeTask::ExecuteTask(TaskExecutionMode mode) {
	op.FinalizeInternal(pipeline, *event, context, gstate, false);
	D_ASSERT(!gstate.finished);
	gstate.finished = true;
	event->FinishTask();
	return TaskExecutionResult::TASK_FINISHED;
}

class HashAggregateDistinctFinalizeEvent : public BasePipelineEvent {
public:
	//! Distinct Finalize Event that is scheduled if we have distinct aggregates
	HashAggregateDistinctFinalizeEvent(ClientContext &context, Pipeline &pipeline_p, const PhysicalHashAggregate &op_p,
	                                   HashAggregateGlobalSinkState &gstate_p)
	    : BasePipelineEvent(pipeline_p), context(context), op(op_p), gstate(gstate_p) {
	}

public:
	void Schedule() override;
	void FinishEvent() override;

private:
	void CreateGlobalSources();

private:
	ClientContext &context;

	const PhysicalHashAggregate &op;
	HashAggregateGlobalSinkState &gstate;

public:
	//! The GlobalSourceStates for all the radix tables of the distinct aggregates
	vector<vector<unique_ptr<GlobalSourceState>>> global_source_states;
};

class HashAggregateDistinctFinalizeTask : public ExecutorTask {
public:
	HashAggregateDistinctFinalizeTask(Pipeline &pipeline, shared_ptr<Event> event_p, const PhysicalHashAggregate &op,
	                                  HashAggregateGlobalSinkState &state_p)
	    : ExecutorTask(pipeline.executor), pipeline(pipeline), event(std::move(event_p)), op(op), gstate(state_p) {
	}

public:
	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override;

private:
	void AggregateDistinctGrouping(const idx_t grouping_idx);

private:
	Pipeline &pipeline;
	shared_ptr<Event> event;

	const PhysicalHashAggregate &op;
	HashAggregateGlobalSinkState &gstate;
};

void HashAggregateDistinctFinalizeEvent::Schedule() {
	CreateGlobalSources();

	const idx_t n_threads = TaskScheduler::GetScheduler(context).NumberOfThreads();
	vector<shared_ptr<Task>> tasks;
	for (idx_t i = 0; i < n_threads; i++) {
		tasks.push_back(make_uniq<HashAggregateDistinctFinalizeTask>(*pipeline, shared_from_this(), op, gstate));
	}
	SetTasks(std::move(tasks));
}

void HashAggregateDistinctFinalizeEvent::CreateGlobalSources() {
	auto &aggregates = op.grouped_aggregate_data.aggregates;
	global_source_states.reserve(op.groupings.size());
	for (idx_t grouping_idx = 0; grouping_idx < op.groupings.size(); grouping_idx++) {
		auto &grouping = op.groupings[grouping_idx];
		auto &distinct_data = *grouping.distinct_data;

		vector<unique_ptr<GlobalSourceState>> aggregate_sources;
		aggregate_sources.reserve(aggregates.size());
		for (idx_t agg_idx = 0; agg_idx < aggregates.size(); agg_idx++) {
			auto &aggregate = aggregates[agg_idx];
			auto &aggr = aggregate->Cast<BoundAggregateExpression>();

			if (!aggr.IsDistinct()) {
				aggregate_sources.push_back(nullptr);
				continue;
			}
			D_ASSERT(distinct_data.info.table_map.count(agg_idx));

			auto table_idx = distinct_data.info.table_map.at(agg_idx);
			auto &radix_table_p = distinct_data.radix_tables[table_idx];
			aggregate_sources.push_back(radix_table_p->GetGlobalSourceState(context));
		}
		global_source_states.push_back(std::move(aggregate_sources));
	}
}

void HashAggregateDistinctFinalizeEvent::FinishEvent() {
	// Now that everything is added to the main ht, we can actually finalize
	auto new_event = make_shared<HashAggregateFinalizeEvent>(context, pipeline.get(), op, gstate);
	this->InsertEvent(std::move(new_event));
}

TaskExecutionResult HashAggregateDistinctFinalizeTask::ExecuteTask(TaskExecutionMode mode) {
	for (idx_t grouping_idx = 0; grouping_idx < op.groupings.size(); grouping_idx++) {
		AggregateDistinctGrouping(grouping_idx);
	}
	event->FinishTask();
	return TaskExecutionResult::TASK_FINISHED;
}

void HashAggregateDistinctFinalizeTask::AggregateDistinctGrouping(const idx_t grouping_idx) {
	D_ASSERT(op.distinct_collection_info);
	auto &info = *op.distinct_collection_info;

	auto &grouping_data = op.groupings[grouping_idx];
	auto &grouping_state = gstate.grouping_states[grouping_idx];
	D_ASSERT(grouping_state.distinct_state);
	auto &distinct_state = *grouping_state.distinct_state;
	auto &distinct_data = *grouping_data.distinct_data;

	auto &aggregates = info.aggregates;

	// Thread-local contexts
	ThreadContext thread_context(executor.context);
	ExecutionContext execution_context(executor.context, thread_context, &pipeline);

	// Sink state to sink into global HTs
	InterruptState interrupt_state;
	auto &global_sink_state = *grouping_state.table_state;
	auto local_sink_state = grouping_data.table_data.GetLocalSinkState(execution_context);
	OperatorSinkInput sink_input {global_sink_state, *local_sink_state, interrupt_state};

	// Create a chunk that mimics the 'input' chunk in Sink, for storing the group vectors
	DataChunk group_chunk;
	if (!op.input_group_types.empty()) {
		group_chunk.Initialize(executor.context, op.input_group_types);
	}

	auto &groups = op.grouped_aggregate_data.groups;
	const idx_t group_by_size = groups.size();

	DataChunk aggregate_input_chunk;
	if (!gstate.payload_types.empty()) {
		aggregate_input_chunk.Initialize(executor.context, gstate.payload_types);
	}

	auto &finalize_event = event->Cast<HashAggregateDistinctFinalizeEvent>();

	idx_t payload_idx;
	idx_t next_payload_idx = 0;
	for (idx_t agg_idx = 0; agg_idx < op.grouped_aggregate_data.aggregates.size(); agg_idx++) {
		auto &aggregate = aggregates[agg_idx]->Cast<BoundAggregateExpression>();

		// Forward the payload idx
		payload_idx = next_payload_idx;
		next_payload_idx = payload_idx + aggregate.children.size();

		// If aggregate is not distinct, skip it
		if (!distinct_data.IsDistinct(agg_idx)) {
			continue;
		}

		D_ASSERT(distinct_data.info.table_map.count(agg_idx));
		const auto &table_idx = distinct_data.info.table_map.at(agg_idx);
		auto &radix_table = distinct_data.radix_tables[table_idx];

		auto &sink = *distinct_state.radix_states[table_idx];
		auto local_source = radix_table->GetLocalSourceState(execution_context);
		OperatorSourceInput source_input {*finalize_event.global_source_states[grouping_idx][agg_idx], *local_source,
		                                  interrupt_state};

		// Create a duplicate of the output_chunk, because of multi-threading we cant alter the original
		DataChunk output_chunk;
		output_chunk.Initialize(executor.context, distinct_state.distinct_output_chunks[table_idx]->GetTypes());

		// Fetch all the data from the aggregate ht, and Sink it into the main ht
		while (true) {
			output_chunk.Reset();
			group_chunk.Reset();
			aggregate_input_chunk.Reset();

			auto res = radix_table->GetData(execution_context, output_chunk, sink, source_input);
			if (res == SourceResultType::FINISHED) {
				D_ASSERT(output_chunk.size() == 0);
				break;
			} else if (res == SourceResultType::BLOCKED) {
				throw InternalException(
				    "Unexpected interrupt from radix table GetData in HashAggregateDistinctFinalizeTask");
			}

			auto &grouped_aggregate_data = *distinct_data.grouped_aggregate_data[table_idx];
			for (idx_t group_idx = 0; group_idx < group_by_size; group_idx++) {
				auto &group = grouped_aggregate_data.groups[group_idx];
				auto &bound_ref_expr = group->Cast<BoundReferenceExpression>();
				group_chunk.data[bound_ref_expr.index].Reference(output_chunk.data[group_idx]);
			}
			group_chunk.SetCardinality(output_chunk);

			for (idx_t child_idx = 0; child_idx < grouped_aggregate_data.groups.size() - group_by_size; child_idx++) {
				aggregate_input_chunk.data[payload_idx + child_idx].Reference(
				    output_chunk.data[group_by_size + child_idx]);
			}
			aggregate_input_chunk.SetCardinality(output_chunk);

			// Sink it into the main ht
			grouping_data.table_data.Sink(execution_context, group_chunk, sink_input, aggregate_input_chunk, {agg_idx});
		}
	}
	grouping_data.table_data.Combine(execution_context, global_sink_state, *local_sink_state);
}

SinkFinalizeType PhysicalHashAggregate::FinalizeDistinct(Pipeline &pipeline, Event &event, ClientContext &context,
                                                         GlobalSinkState &gstate_p) const {
	auto &gstate = gstate_p.Cast<HashAggregateGlobalSinkState>();
	D_ASSERT(distinct_collection_info);

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
			radix_table->Finalize(context, radix_state);
		}
	}
	auto new_event = make_shared<HashAggregateDistinctFinalizeEvent>(context, pipeline, *this, gstate);
	event.InsertEvent(std::move(new_event));
	return SinkFinalizeType::READY;
}

SinkFinalizeType PhysicalHashAggregate::FinalizeInternal(Pipeline &pipeline, Event &event, ClientContext &context,
                                                         GlobalSinkState &gstate_p, bool check_distinct) const {
	auto &gstate = gstate_p.Cast<HashAggregateGlobalSinkState>();

	if (check_distinct && distinct_collection_info) {
		// There are distinct aggregates
		// If these are partitioned those need to be combined first
		// Then we Finalize again, skipping this step
		return FinalizeDistinct(pipeline, event, context, gstate_p);
	}

	for (idx_t i = 0; i < groupings.size(); i++) {
		auto &grouping = groupings[i];
		auto &grouping_gstate = gstate.grouping_states[i];
		grouping.table_data.Finalize(context, *grouping_gstate.table_state);
	}
	return SinkFinalizeType::READY;
}

SinkFinalizeType PhysicalHashAggregate::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                 OperatorSinkFinalizeInput &input) const {
	return FinalizeInternal(pipeline, event, context, input.global_state, true);
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class HashAggregateGlobalSourceState : public GlobalSourceState {
public:
	HashAggregateGlobalSourceState(ClientContext &context, const PhysicalHashAggregate &op) : op(op), state_index(0) {
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

		auto &ht_state = op.sink_state->Cast<HashAggregateGlobalSinkState>();
		idx_t partitions = 0;
		for (size_t sidx = 0; sidx < op.groupings.size(); ++sidx) {
			auto &grouping = op.groupings[sidx];
			auto &grouping_gstate = ht_state.grouping_states[sidx];
			partitions += grouping.table_data.NumberOfPartitions(*grouping_gstate.table_state);
		}
		return MaxValue<idx_t>(1, partitions);
	}
};

unique_ptr<GlobalSourceState> PhysicalHashAggregate::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<HashAggregateGlobalSourceState>(context, *this);
}

class HashAggregateLocalSourceState : public LocalSourceState {
public:
	explicit HashAggregateLocalSourceState(ExecutionContext &context, const PhysicalHashAggregate &op) {
		for (auto &grouping : op.groupings) {
			auto &rt = grouping.table_data;
			radix_states.push_back(rt.GetLocalSourceState(context));
		}
	}

	vector<unique_ptr<LocalSourceState>> radix_states;
};

unique_ptr<LocalSourceState> PhysicalHashAggregate::GetLocalSourceState(ExecutionContext &context,
                                                                        GlobalSourceState &gstate) const {
	return make_uniq<HashAggregateLocalSourceState>(context, *this);
}

SourceResultType PhysicalHashAggregate::GetData(ExecutionContext &context, DataChunk &chunk,
                                                OperatorSourceInput &input) const {
	auto &sink_gstate = sink_state->Cast<HashAggregateGlobalSinkState>();
	auto &gstate = input.global_state.Cast<HashAggregateGlobalSourceState>();
	auto &lstate = input.local_state.Cast<HashAggregateLocalSourceState>();
	while (true) {
		idx_t radix_idx = gstate.state_index;
		if (radix_idx >= groupings.size()) {
			break;
		}
		auto &grouping = groupings[radix_idx];
		auto &radix_table = grouping.table_data;
		auto &grouping_gstate = sink_gstate.grouping_states[radix_idx];

		InterruptState interrupt_state;
		OperatorSourceInput source_input {*gstate.radix_states[radix_idx], *lstate.radix_states[radix_idx],
		                                  interrupt_state};
		auto res = radix_table.GetData(context, chunk, *grouping_gstate.table_state, source_input);
		if (chunk.size() != 0) {
			return SourceResultType::HAVE_MORE_OUTPUT;
		} else if (res == SourceResultType::BLOCKED) {
			throw InternalException("Unexpectedly Blocked from radix_table");
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

	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
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
		auto &aggregate = aggregates[i]->Cast<BoundAggregateExpression>();
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
