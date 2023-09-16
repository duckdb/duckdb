#include "duckdb/execution/operator/aggregate/physical_ungrouped_aggregate.hpp"

#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/operator/aggregate/aggregate_object.hpp"
#include "duckdb/execution/operator/aggregate/distinct_aggregate_data.hpp"
#include "duckdb/execution/radix_partitioned_hashtable.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/base_pipeline_event.hpp"
#include "duckdb/parallel/interrupt.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

#include <functional>

namespace duckdb {

PhysicalUngroupedAggregate::PhysicalUngroupedAggregate(vector<LogicalType> types,
                                                       vector<unique_ptr<Expression>> expressions,
                                                       idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::UNGROUPED_AGGREGATE, std::move(types), estimated_cardinality),
      aggregates(std::move(expressions)) {

	distinct_collection_info = DistinctAggregateCollectionInfo::Create(aggregates);
	if (!distinct_collection_info) {
		return;
	}
	distinct_data = make_uniq<DistinctAggregateData>(*distinct_collection_info);
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
struct AggregateState {
	explicit AggregateState(const vector<unique_ptr<Expression>> &aggregate_expressions) {
		counts = make_uniq_array<atomic<idx_t>>(aggregate_expressions.size());
		for (idx_t i = 0; i < aggregate_expressions.size(); i++) {
			auto &aggregate = aggregate_expressions[i];
			D_ASSERT(aggregate->GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE);
			auto &aggr = aggregate->Cast<BoundAggregateExpression>();
			auto state = make_unsafe_uniq_array<data_t>(aggr.function.state_size());
			aggr.function.initialize(state.get());
			aggregates.push_back(std::move(state));
			bind_data.push_back(aggr.bind_info.get());
			destructors.push_back(aggr.function.destructor);
#ifdef DEBUG
			counts[i] = 0;
#endif
		}
	}
	~AggregateState() {
		D_ASSERT(destructors.size() == aggregates.size());
		for (idx_t i = 0; i < destructors.size(); i++) {
			if (!destructors[i]) {
				continue;
			}
			Vector state_vector(Value::POINTER(CastPointerToValue(aggregates[i].get())));
			state_vector.SetVectorType(VectorType::FLAT_VECTOR);

			ArenaAllocator allocator(Allocator::DefaultAllocator());
			AggregateInputData aggr_input_data(bind_data[i], allocator);
			destructors[i](state_vector, aggr_input_data, 1);
		}
	}

	void Move(AggregateState &other) {
		other.aggregates = std::move(aggregates);
		other.destructors = std::move(destructors);
	}

	//! The aggregate values
	vector<unsafe_unique_array<data_t>> aggregates;
	//! The bind data
	vector<FunctionData *> bind_data;
	//! The destructors
	vector<aggregate_destructor_t> destructors;
	//! Counts (used for verification)
	unique_array<atomic<idx_t>> counts;
};

class UngroupedAggregateGlobalSinkState : public GlobalSinkState {
public:
	UngroupedAggregateGlobalSinkState(const PhysicalUngroupedAggregate &op, ClientContext &client)
	    : state(op.aggregates), finished(false), allocator(BufferAllocator::Get(client)) {
		if (op.distinct_data) {
			distinct_state = make_uniq<DistinctAggregateState>(*op.distinct_data, client);
		}
	}

	//! The lock for updating the global aggregate state
	mutex lock;
	//! The global aggregate state
	AggregateState state;
	//! Whether or not the aggregate is finished
	bool finished;
	//! The data related to the distinct aggregates (if there are any)
	unique_ptr<DistinctAggregateState> distinct_state;
	//! Global arena allocator
	ArenaAllocator allocator;
};

class UngroupedAggregateLocalSinkState : public LocalSinkState {
public:
	UngroupedAggregateLocalSinkState(const PhysicalUngroupedAggregate &op, const vector<LogicalType> &child_types,
	                                 GlobalSinkState &gstate_p, ExecutionContext &context)
	    : allocator(BufferAllocator::Get(context.client)), state(op.aggregates), child_executor(context.client),
	      aggregate_input_chunk(), filter_set() {
		auto &gstate = gstate_p.Cast<UngroupedAggregateGlobalSinkState>();

		auto &allocator = BufferAllocator::Get(context.client);
		InitializeDistinctAggregates(op, gstate, context);

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

	//! Local arena allocator
	ArenaAllocator allocator;
	//! The local aggregate state
	AggregateState state;
	//! The executor
	ExpressionExecutor child_executor;
	//! The payload chunk, containing all the Vectors for the aggregates
	DataChunk aggregate_input_chunk;
	//! Aggregate filter data set
	AggregateFilterDataSet filter_set;
	//! The local sink states of the distinct aggregates hash tables
	vector<unique_ptr<LocalSinkState>> radix_states;

public:
	void Reset() {
		aggregate_input_chunk.Reset();
	}
	void InitializeDistinctAggregates(const PhysicalUngroupedAggregate &op,
	                                  const UngroupedAggregateGlobalSinkState &gstate, ExecutionContext &context) {

		if (!op.distinct_data) {
			return;
		}
		auto &data = *op.distinct_data;
		auto &state = *gstate.distinct_state;
		D_ASSERT(!data.radix_tables.empty());

		const idx_t aggregate_count = state.radix_states.size();
		radix_states.resize(aggregate_count);

		auto &distinct_info = *op.distinct_collection_info;

		for (auto &idx : distinct_info.indices) {
			idx_t table_idx = distinct_info.table_map[idx];
			if (data.radix_tables[table_idx] == nullptr) {
				// This aggregate has identical input as another aggregate, so no table is created for it
				continue;
			}
			auto &radix_table = *data.radix_tables[table_idx];
			radix_states[table_idx] = radix_table.GetLocalSinkState(context);
		}
	}
};

bool PhysicalUngroupedAggregate::SinkOrderDependent() const {
	for (auto &expr : aggregates) {
		auto &aggr = expr->Cast<BoundAggregateExpression>();
		if (aggr.function.order_dependent == AggregateOrderDependent::ORDER_DEPENDENT) {
			return true;
		}
	}
	return false;
}

unique_ptr<GlobalSinkState> PhysicalUngroupedAggregate::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<UngroupedAggregateGlobalSinkState>(*this, context);
}

unique_ptr<LocalSinkState> PhysicalUngroupedAggregate::GetLocalSinkState(ExecutionContext &context) const {
	D_ASSERT(sink_state);
	auto &gstate = *sink_state;
	return make_uniq<UngroupedAggregateLocalSinkState>(*this, children[0]->GetTypes(), gstate, context);
}

void PhysicalUngroupedAggregate::SinkDistinct(ExecutionContext &context, DataChunk &chunk,
                                              OperatorSinkInput &input) const {
	auto &sink = input.local_state.Cast<UngroupedAggregateLocalSinkState>();
	auto &global_sink = input.global_state.Cast<UngroupedAggregateGlobalSinkState>();
	D_ASSERT(distinct_data);
	auto &distinct_state = *global_sink.distinct_state;
	auto &distinct_info = *distinct_collection_info;
	auto &distinct_indices = distinct_info.Indices();

	DataChunk empty_chunk;

	auto &distinct_filter = distinct_info.Indices();

	for (auto &idx : distinct_indices) {
		auto &aggregate = aggregates[idx]->Cast<BoundAggregateExpression>();

		idx_t table_idx = distinct_info.table_map[idx];
		if (!distinct_data->radix_tables[table_idx]) {
			// This distinct aggregate shares its data with another
			continue;
		}
		D_ASSERT(distinct_data->radix_tables[table_idx]);
		auto &radix_table = *distinct_data->radix_tables[table_idx];
		auto &radix_global_sink = *distinct_state.radix_states[table_idx];
		auto &radix_local_sink = *sink.radix_states[table_idx];
		OperatorSinkInput sink_input {radix_global_sink, radix_local_sink, input.interrupt_state};

		if (aggregate.filter) {
			// The hashtable can apply a filter, but only on the payload
			// And in our case, we need to filter the groups (the distinct aggr children)

			// Apply the filter before inserting into the hashtable
			auto &filtered_data = sink.filter_set.GetFilterData(idx);
			idx_t count = filtered_data.ApplyFilter(chunk);
			filtered_data.filtered_payload.SetCardinality(count);

			radix_table.Sink(context, filtered_data.filtered_payload, sink_input, empty_chunk, distinct_filter);
		} else {
			radix_table.Sink(context, chunk, sink_input, empty_chunk, distinct_filter);
		}
	}
}

SinkResultType PhysicalUngroupedAggregate::Sink(ExecutionContext &context, DataChunk &chunk,
                                                OperatorSinkInput &input) const {
	auto &sink = input.local_state.Cast<UngroupedAggregateLocalSinkState>();

	// perform the aggregation inside the local state
	sink.Reset();

	if (distinct_data) {
		SinkDistinct(context, chunk, input);
	}

	DataChunk &payload_chunk = sink.aggregate_input_chunk;

	idx_t payload_idx = 0;
	idx_t next_payload_idx = 0;

	for (idx_t aggr_idx = 0; aggr_idx < aggregates.size(); aggr_idx++) {
		auto &aggregate = aggregates[aggr_idx]->Cast<BoundAggregateExpression>();

		payload_idx = next_payload_idx;
		next_payload_idx = payload_idx + aggregate.children.size();

		if (aggregate.IsDistinct()) {
			continue;
		}

		idx_t payload_cnt = 0;
		// resolve the filter (if any)
		if (aggregate.filter) {
			auto &filtered_data = sink.filter_set.GetFilterData(aggr_idx);
			auto count = filtered_data.ApplyFilter(chunk);

			sink.child_executor.SetChunk(filtered_data.filtered_payload);
			payload_chunk.SetCardinality(count);
		} else {
			sink.child_executor.SetChunk(chunk);
			payload_chunk.SetCardinality(chunk);
		}

#ifdef DEBUG
		sink.state.counts[aggr_idx] += payload_chunk.size();
#endif

		// resolve the child expressions of the aggregate (if any)
		for (idx_t i = 0; i < aggregate.children.size(); ++i) {
			sink.child_executor.ExecuteExpression(payload_idx + payload_cnt,
			                                      payload_chunk.data[payload_idx + payload_cnt]);
			payload_cnt++;
		}

		auto start_of_input = payload_cnt == 0 ? nullptr : &payload_chunk.data[payload_idx];
		AggregateInputData aggr_input_data(aggregate.bind_info.get(), sink.allocator);
		aggregate.function.simple_update(start_of_input, aggr_input_data, payload_cnt,
		                                 sink.state.aggregates[aggr_idx].get(), payload_chunk.size());
	}
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Combine
//===--------------------------------------------------------------------===//
void PhysicalUngroupedAggregate::CombineDistinct(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<UngroupedAggregateGlobalSinkState>();
	auto &lstate = input.local_state.Cast<UngroupedAggregateLocalSinkState>();

	if (!distinct_data) {
		return;
	}
	auto &distinct_state = gstate.distinct_state;
	auto table_count = distinct_data->radix_tables.size();
	for (idx_t table_idx = 0; table_idx < table_count; table_idx++) {
		D_ASSERT(distinct_data->radix_tables[table_idx]);
		auto &radix_table = *distinct_data->radix_tables[table_idx];
		auto &radix_global_sink = *distinct_state->radix_states[table_idx];
		auto &radix_local_sink = *lstate.radix_states[table_idx];

		radix_table.Combine(context, radix_global_sink, radix_local_sink);
	}
}

SinkCombineResultType PhysicalUngroupedAggregate::Combine(ExecutionContext &context,
                                                          OperatorSinkCombineInput &input) const {
	auto &gstate = input.global_state.Cast<UngroupedAggregateGlobalSinkState>();
	auto &lstate = input.local_state.Cast<UngroupedAggregateLocalSinkState>();
	D_ASSERT(!gstate.finished);

	// finalize: combine the local state into the global state
	// all aggregates are combinable: we might be doing a parallel aggregate
	// use the combine method to combine the partial aggregates
	OperatorSinkCombineInput distinct_input {gstate, lstate, input.interrupt_state};
	CombineDistinct(context, distinct_input);

	lock_guard<mutex> glock(gstate.lock);
	for (idx_t aggr_idx = 0; aggr_idx < aggregates.size(); aggr_idx++) {
		auto &aggregate = aggregates[aggr_idx]->Cast<BoundAggregateExpression>();

		if (aggregate.IsDistinct()) {
			continue;
		}

		Vector source_state(Value::POINTER(CastPointerToValue(lstate.state.aggregates[aggr_idx].get())));
		Vector dest_state(Value::POINTER(CastPointerToValue(gstate.state.aggregates[aggr_idx].get())));

		AggregateInputData aggr_input_data(aggregate.bind_info.get(), gstate.allocator);
		aggregate.function.combine(source_state, dest_state, aggr_input_data, 1);
#ifdef DEBUG
		gstate.state.counts[aggr_idx] += lstate.state.counts[aggr_idx];
#endif
	}
	lstate.allocator.Destroy();

	auto &client_profiler = QueryProfiler::Get(context.client);
	context.thread.profiler.Flush(*this, lstate.child_executor, "child_executor", 0);
	client_profiler.Flush(context.thread.profiler);

	return SinkCombineResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
class UngroupedDistinctAggregateFinalizeEvent : public BasePipelineEvent {
public:
	UngroupedDistinctAggregateFinalizeEvent(ClientContext &context, const PhysicalUngroupedAggregate &op_p,
	                                        UngroupedAggregateGlobalSinkState &gstate_p, Pipeline &pipeline_p)
	    : BasePipelineEvent(pipeline_p), context(context), op(op_p), gstate(gstate_p), tasks_scheduled(0),
	      tasks_done(0) {
	}

public:
	void Schedule() override;

private:
	ClientContext &context;

	const PhysicalUngroupedAggregate &op;
	UngroupedAggregateGlobalSinkState &gstate;

public:
	mutex lock;
	idx_t tasks_scheduled;
	idx_t tasks_done;

	vector<unique_ptr<GlobalSourceState>> global_source_states;
};

class UngroupedDistinctAggregateFinalizeTask : public ExecutorTask {
public:
	UngroupedDistinctAggregateFinalizeTask(Executor &executor, shared_ptr<Event> event_p,
	                                       const PhysicalUngroupedAggregate &op,
	                                       UngroupedAggregateGlobalSinkState &state_p)
	    : ExecutorTask(executor), event(std::move(event_p)), op(op), gstate(state_p),
	      allocator(BufferAllocator::Get(executor.context)) {
	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override;

private:
	void AggregateDistinct();

private:
	shared_ptr<Event> event;

	const PhysicalUngroupedAggregate &op;
	UngroupedAggregateGlobalSinkState &gstate;

	ArenaAllocator allocator;
};

void UngroupedDistinctAggregateFinalizeEvent::Schedule() {
	D_ASSERT(gstate.distinct_state);
	auto &aggregates = op.aggregates;
	auto &distinct_data = *op.distinct_data;

	idx_t payload_idx = 0;
	idx_t next_payload_idx = 0;
	for (idx_t agg_idx = 0; agg_idx < aggregates.size(); agg_idx++) {
		auto &aggregate = aggregates[agg_idx]->Cast<BoundAggregateExpression>();

		// Forward the payload idx
		payload_idx = next_payload_idx;
		next_payload_idx = payload_idx + aggregate.children.size();

		// If aggregate is not distinct, skip it
		if (!distinct_data.IsDistinct(agg_idx)) {
			global_source_states.push_back(nullptr);
			continue;
		}
		D_ASSERT(distinct_data.info.table_map.count(agg_idx));

		// Create global state for scanning
		auto table_idx = distinct_data.info.table_map.at(agg_idx);
		auto &radix_table_p = *distinct_data.radix_tables[table_idx];
		global_source_states.push_back(radix_table_p.GetGlobalSourceState(context));
	}

	const idx_t n_threads = TaskScheduler::GetScheduler(context).NumberOfThreads();
	vector<shared_ptr<Task>> tasks;
	for (idx_t i = 0; i < n_threads; i++) {
		tasks.push_back(
		    make_uniq<UngroupedDistinctAggregateFinalizeTask>(pipeline->executor, shared_from_this(), op, gstate));
		tasks_scheduled++;
	}
	SetTasks(std::move(tasks));
}

TaskExecutionResult UngroupedDistinctAggregateFinalizeTask::ExecuteTask(TaskExecutionMode mode) {
	AggregateDistinct();
	event->FinishTask();
	return TaskExecutionResult::TASK_FINISHED;
}

void UngroupedDistinctAggregateFinalizeTask::AggregateDistinct() {
	D_ASSERT(gstate.distinct_state);
	auto &distinct_state = *gstate.distinct_state;
	auto &distinct_data = *op.distinct_data;

	// Create thread-local copy of aggregate state
	auto &aggregates = op.aggregates;
	AggregateState state(aggregates);

	// Thread-local contexts
	ThreadContext thread_context(executor.context);
	ExecutionContext execution_context(executor.context, thread_context, nullptr);

	auto &finalize_event = event->Cast<UngroupedDistinctAggregateFinalizeEvent>();

	// Now loop through the distinct aggregates, scanning the distinct HTs
	idx_t payload_idx = 0;
	idx_t next_payload_idx = 0;
	for (idx_t agg_idx = 0; agg_idx < aggregates.size(); agg_idx++) {
		auto &aggregate = aggregates[agg_idx]->Cast<BoundAggregateExpression>();

		// Forward the payload idx
		payload_idx = next_payload_idx;
		next_payload_idx = payload_idx + aggregate.children.size();

		// If aggregate is not distinct, skip it
		if (!distinct_data.IsDistinct(agg_idx)) {
			continue;
		}

		const auto table_idx = distinct_data.info.table_map.at(agg_idx);
		auto &radix_table = *distinct_data.radix_tables[table_idx];
		auto lstate = radix_table.GetLocalSourceState(execution_context);

		auto &sink = *distinct_state.radix_states[table_idx];
		InterruptState interrupt_state;
		OperatorSourceInput source_input {*finalize_event.global_source_states[agg_idx], *lstate, interrupt_state};

		DataChunk output_chunk;
		output_chunk.Initialize(executor.context, distinct_state.distinct_output_chunks[table_idx]->GetTypes());

		DataChunk payload_chunk;
		payload_chunk.InitializeEmpty(distinct_data.grouped_aggregate_data[table_idx]->group_types);
		payload_chunk.SetCardinality(0);

		AggregateInputData aggr_input_data(aggregate.bind_info.get(), allocator);
		while (true) {
			output_chunk.Reset();

			auto res = radix_table.GetData(execution_context, output_chunk, sink, source_input);
			if (res == SourceResultType::FINISHED) {
				D_ASSERT(output_chunk.size() == 0);
				break;
			} else if (res == SourceResultType::BLOCKED) {
				throw InternalException(
				    "Unexpected interrupt from radix table GetData in UngroupedDistinctAggregateFinalizeTask");
			}

			// We dont need to resolve the filter, we already did this in Sink
			idx_t payload_cnt = aggregate.children.size();
			for (idx_t i = 0; i < payload_cnt; i++) {
				payload_chunk.data[i].Reference(output_chunk.data[i]);
			}
			payload_chunk.SetCardinality(output_chunk);

#ifdef DEBUG
			gstate.state.counts[agg_idx] += payload_chunk.size();
#endif

			// Update the aggregate state
			auto start_of_input = payload_cnt ? &payload_chunk.data[0] : nullptr;
			aggregate.function.simple_update(start_of_input, aggr_input_data, payload_cnt,
			                                 state.aggregates[agg_idx].get(), payload_chunk.size());
		}
	}

	// After scanning the distinct HTs, we can combine the thread-local agg states with the thread-global
	lock_guard<mutex> guard(finalize_event.lock);
	payload_idx = 0;
	next_payload_idx = 0;
	for (idx_t agg_idx = 0; agg_idx < aggregates.size(); agg_idx++) {
		if (!distinct_data.IsDistinct(agg_idx)) {
			continue;
		}

		auto &aggregate = aggregates[agg_idx]->Cast<BoundAggregateExpression>();
		AggregateInputData aggr_input_data(aggregate.bind_info.get(), allocator);

		Vector state_vec(Value::POINTER(CastPointerToValue(state.aggregates[agg_idx].get())));
		Vector combined_vec(Value::POINTER(CastPointerToValue(gstate.state.aggregates[agg_idx].get())));
		aggregate.function.combine(state_vec, combined_vec, aggr_input_data, 1);
	}

	D_ASSERT(!gstate.finished);
	if (++finalize_event.tasks_done == finalize_event.tasks_scheduled) {
		gstate.finished = true;
	}
}

SinkFinalizeType PhysicalUngroupedAggregate::FinalizeDistinct(Pipeline &pipeline, Event &event, ClientContext &context,
                                                              GlobalSinkState &gstate_p) const {
	auto &gstate = gstate_p.Cast<UngroupedAggregateGlobalSinkState>();
	D_ASSERT(distinct_data);
	auto &distinct_state = *gstate.distinct_state;

	for (idx_t table_idx = 0; table_idx < distinct_data->radix_tables.size(); table_idx++) {
		auto &radix_table_p = distinct_data->radix_tables[table_idx];
		auto &radix_state = *distinct_state.radix_states[table_idx];
		radix_table_p->Finalize(context, radix_state);
	}
	auto new_event = make_shared<UngroupedDistinctAggregateFinalizeEvent>(context, *this, gstate, pipeline);
	event.InsertEvent(std::move(new_event));
	return SinkFinalizeType::READY;
}

SinkFinalizeType PhysicalUngroupedAggregate::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                      OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<UngroupedAggregateGlobalSinkState>();

	if (distinct_data) {
		return FinalizeDistinct(pipeline, event, context, input.global_state);
	}

	D_ASSERT(!gstate.finished);
	gstate.finished = true;
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
void VerifyNullHandling(DataChunk &chunk, AggregateState &state, const vector<unique_ptr<Expression>> &aggregates) {
#ifdef DEBUG
	for (idx_t aggr_idx = 0; aggr_idx < aggregates.size(); aggr_idx++) {
		auto &aggr = aggregates[aggr_idx]->Cast<BoundAggregateExpression>();
		if (state.counts[aggr_idx] == 0 && aggr.function.null_handling == FunctionNullHandling::DEFAULT_NULL_HANDLING) {
			// Default is when 0 values go in, NULL comes out
			UnifiedVectorFormat vdata;
			chunk.data[aggr_idx].ToUnifiedFormat(1, vdata);
			D_ASSERT(!vdata.validity.RowIsValid(vdata.sel->get_index(0)));
		}
	}
#endif
}

SourceResultType PhysicalUngroupedAggregate::GetData(ExecutionContext &context, DataChunk &chunk,
                                                     OperatorSourceInput &input) const {
	auto &gstate = sink_state->Cast<UngroupedAggregateGlobalSinkState>();
	D_ASSERT(gstate.finished);

	// initialize the result chunk with the aggregate values
	chunk.SetCardinality(1);
	for (idx_t aggr_idx = 0; aggr_idx < aggregates.size(); aggr_idx++) {
		auto &aggregate = aggregates[aggr_idx]->Cast<BoundAggregateExpression>();

		Vector state_vector(Value::POINTER(CastPointerToValue(gstate.state.aggregates[aggr_idx].get())));
		AggregateInputData aggr_input_data(aggregate.bind_info.get(), gstate.allocator);
		aggregate.function.finalize(state_vector, aggr_input_data, chunk.data[aggr_idx], 1, 0);
	}
	VerifyNullHandling(chunk, gstate.state, aggregates);

	return SourceResultType::FINISHED;
}

string PhysicalUngroupedAggregate::ParamsToString() const {
	string result;
	for (idx_t i = 0; i < aggregates.size(); i++) {
		auto &aggregate = aggregates[i]->Cast<BoundAggregateExpression>();
		if (i > 0) {
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
