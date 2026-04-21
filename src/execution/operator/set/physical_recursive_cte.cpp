#include "duckdb/execution/operator/set/physical_recursive_cte.hpp"

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/execution/aggregate_hashtable.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"
#include "duckdb/execution/operator/join/physical_blockwise_nl_join.hpp"
#include "duckdb/execution/operator/join/physical_delim_join.hpp"
#include "duckdb/execution/operator/join/physical_hash_join.hpp"
#include "duckdb/execution/operator/join/physical_nested_loop_join.hpp"
#include "duckdb/execution/operator/scan/physical_column_data_scan.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/parallel/base_pipeline_event.hpp"
#include "duckdb/parallel/event.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/parallel/pipeline_executor.hpp"
#include "duckdb/parallel/pipeline_complete_event.hpp"
#include "duckdb/parallel/pipeline_finish_event.hpp"
#include "duckdb/parallel/pipeline_prepare_finish_event.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/storage/buffer_manager.hpp"

#include <utility>

namespace duckdb {

struct RecursiveCTEInlinePlan;
struct RecursiveExecutorPool {
	mutex lock;
	PhysicalRecursiveCTE::executor_cache_t executors;
};

PhysicalRecursiveCTE::PhysicalRecursiveCTE(PhysicalPlan &physical_plan, string ctename, TableIndex table_index,
                                           vector<LogicalType> types, bool union_all, PhysicalOperator &top,
                                           PhysicalOperator &bottom, idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::RECURSIVE_CTE, std::move(types), estimated_cardinality),
      ctename(std::move(ctename)), table_index(table_index), union_all(union_all),
      shared_executor_pool(make_shared_ptr<RecursiveExecutorPool>()) {
	children.push_back(top);
	children.push_back(bottom);
}

PhysicalRecursiveCTE::~PhysicalRecursiveCTE() {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class RecursiveCTEState : public GlobalSinkState {
public:
	explicit RecursiveCTEState(ClientContext &context, const PhysicalRecursiveCTE &op)
	    : op(op), executor(context), allow_executor_reuse(context.config.enable_caching_operators),
	      executor_pool(op.shared_executor_pool),
	      intermediate_table(context, op.using_key ? op.internal_types : op.GetTypes()),
	      new_groups(STANDARD_VECTOR_SIZE), dummy_addresses(LogicalType::POINTER) {
		vector<LogicalType> aggr_input_types;
		vector<AggregateObject> payload_aggregates;
		for (idx_t i = 0; i < op.payload_aggregates.size(); i++) {
			D_ASSERT(op.payload_aggregates[i]->GetExpressionClass() == ExpressionClass::BOUND_AGGREGATE);
			auto &bound_aggr_expr = op.payload_aggregates[i]->Cast<BoundAggregateExpression>();
			for (auto &child_expr : bound_aggr_expr.children) {
				executor.AddExpression(*child_expr);
				aggr_input_types.push_back(child_expr->GetReturnType());
			}
			payload_aggregates.emplace_back(bound_aggr_expr);
		}

		payload_rows.Initialize(Allocator::Get(context), aggr_input_types);

		ht = make_uniq<GroupedAggregateHashTable>(context, BufferAllocator::Get(context), op.distinct_types,
		                                          op.payload_types, std::move(payload_aggregates));
		if (op.using_key) {
			distinct_rows.Initialize(Allocator::DefaultAllocator(), op.distinct_types);
			source_distinct_rows.Initialize(Allocator::DefaultAllocator(), op.distinct_types);
			source_payload_rows.Initialize(Allocator::DefaultAllocator(), op.payload_types);
		}
		source_result.Initialize(Allocator::DefaultAllocator(), op.GetTypes());
		InitializeIntermediateAppend();
		op.working_table->InitializeAppend(working_append_state);
		if (op.recurring_table) {
			op.recurring_table->InitializeAppend(recurring_append_state);
		}
	}

	~RecursiveCTEState() override {
		ClearCachedExecutors();
	}

	void InitializeIntermediateAppend() {
		intermediate_table.InitializeAppend(intermediate_append_state);
	}

	void ResetRecurringTable() {
		D_ASSERT(op.recurring_table);
		op.recurring_table->Reset();
		op.recurring_table->InitializeAppend(recurring_append_state);
	}

	ColumnDataCollection &CurrentOutputTable() {
		if (op.using_key || !output_is_working) {
			return intermediate_table;
		}
		D_ASSERT(op.working_table);
		return *op.working_table;
	}

	ColumnDataCollection &CurrentInputTable() {
		if (op.using_key) {
			D_ASSERT(op.working_table);
			return *op.working_table;
		}
		if (output_is_working) {
			return intermediate_table;
		}
		D_ASSERT(op.working_table);
		return *op.working_table;
	}

	const ColumnDataCollection &CurrentInputTable() const {
		if (op.using_key) {
			D_ASSERT(op.working_table);
			return *op.working_table;
		}
		if (output_is_working) {
			return intermediate_table;
		}
		D_ASSERT(op.working_table);
		return *op.working_table;
	}

	ColumnDataAppendState &CurrentOutputAppendState() {
		if (op.using_key || !output_is_working) {
			return intermediate_append_state;
		}
		return working_append_state;
	}

	void AdvanceIterationBuffers() {
		if (!op.using_key) {
			output_is_working = !output_is_working;
		}
	}

	void ResetCurrentOutputTableForReuse() {
		auto &output = CurrentOutputTable();
		output.ResetForReuse();
		if (op.using_key || !output_is_working) {
			InitializeIntermediateAppend();
		} else {
			D_ASSERT(op.working_table);
			op.working_table->InitializeAppend(working_append_state);
		}
	}

	void RebindRecursiveScans() {
		if (op.using_key) {
			return;
		}
		auto &input_table = CurrentInputTable();
		for (auto &scan_ref : op.recursive_scans) {
			auto &scan = scan_ref.get();
			scan.collection = input_table;
		}
	}

	vector<unique_ptr<PipelineExecutor>> &GetCachedExecutors(Pipeline &pipeline, idx_t max_threads) {
		// Ordinary pipelines build PipelineExecutors once and discard them after the query finishes.
		// Recursive CTEs re-enter the same pipelines many times, and correlated recursive invocations can
		// construct several RecursiveCTEState objects for the same physical plan. Keep a state-local cache
		// for cheap per-iteration reuse, and spill back into a shared pool so later states can recycle the
		// already-initialized executors instead of reconstructing them from scratch.
		lock_guard<mutex> guard(cached_executor_lock);
		auto entry = cached_executors.find(pipeline);
		if (entry == cached_executors.end()) {
			entry =
			    cached_executors.emplace(reference<Pipeline>(pipeline), vector<unique_ptr<PipelineExecutor>>()).first;
		}
		auto &executors = entry->second;
		if (!allow_executor_reuse) {
			while (executors.size() < max_threads) {
				executors.push_back(make_uniq<PipelineExecutor>(pipeline.GetClientContext(), pipeline));
			}
			return executors;
		}
		D_ASSERT(executor_pool);
		// Lock order: cached_executor_lock -> executor_pool->lock.
		lock_guard<mutex> pool_guard(executor_pool->lock);
		auto pool_entry = executor_pool->executors.find(pipeline);
		if (pool_entry == executor_pool->executors.end()) {
			pool_entry =
			    executor_pool->executors.emplace(reference<Pipeline>(pipeline), vector<unique_ptr<PipelineExecutor>>())
			        .first;
		}
		auto &shared_executors = pool_entry->second;
		while (executors.size() < max_threads) {
			if (!shared_executors.empty()) {
				executors.push_back(std::move(shared_executors.back()));
				shared_executors.pop_back();
			} else {
				executors.push_back(make_uniq<PipelineExecutor>(pipeline.GetClientContext(), pipeline));
			}
		}
		return executors;
	}

	void ClearCachedExecutors() {
		lock_guard<mutex> guard(cached_executor_lock);
		if (cached_executors.empty()) {
			return;
		}
		if (!allow_executor_reuse) {
			cached_executors.clear();
			return;
		}
		D_ASSERT(executor_pool);
		// Lock order: cached_executor_lock -> executor_pool->lock.
		lock_guard<mutex> pool_guard(executor_pool->lock);
		for (auto &entry : cached_executors) {
			auto pool_entry = executor_pool->executors.find(entry.first.get());
			if (pool_entry == executor_pool->executors.end()) {
				pool_entry =
				    executor_pool->executors.emplace(entry.first, vector<unique_ptr<PipelineExecutor>>()).first;
			}
			auto &shared_executors = pool_entry->second;
			for (auto &executor : entry.second) {
				shared_executors.push_back(std::move(executor));
			}
		}
		cached_executors.clear();
	}

	unique_ptr<GroupedAggregateHashTable> ht;
	const PhysicalRecursiveCTE &op;
	ExpressionExecutor executor;
	DataChunk payload_rows;
	const bool allow_executor_reuse;
	shared_ptr<RecursiveExecutorPool> executor_pool;

	mutex intermediate_table_lock;
	ColumnDataCollection intermediate_table;
	ColumnDataAppendState intermediate_append_state;
	ColumnDataAppendState working_append_state;
	ColumnDataAppendState recurring_append_state;
	ColumnDataScanState scan_state;
	bool initialized = false;
	bool finished_scan = false;
	bool output_is_working = false;
	SelectionVector new_groups;
	//! Cached dummy address vector for ProbeHT (avoids per-chunk VectorBuffer allocation)
	Vector dummy_addresses;
	//! Cached chunk for distinct key extraction in the using_key Sink path
	DataChunk distinct_rows;
	//! Cached chunks for source-side hash table scans and recurring table copy paths
	DataChunk source_result;
	DataChunk source_payload_rows;
	DataChunk source_distinct_rows;
	AggregateHTScanState ht_scan_state;

	//! Cached PipelineExecutors per pipeline for reuse across recursive iterations
	//! When both locks are needed, always acquire cached_executor_lock before executor_pool->lock.
	mutex cached_executor_lock;
	PhysicalRecursiveCTE::executor_cache_t cached_executors;
	//! Cached dependency graph for the single-thread inline recursive fast path
	unique_ptr<RecursiveCTEInlinePlan> inline_plan;
	//! Cached dependency graph after invariant meta-pipelines have been materialized once
	unique_ptr<RecursiveCTEInlinePlan> invariant_inline_plan;
	//! Whether invariant recursive meta-pipelines have already been materialized for this state
	bool invariant_meta_pipelines_materialized = false;
};

//===--------------------------------------------------------------------===//
// Recursive CTE Task and Event for optimized execution
//===--------------------------------------------------------------------===//
//
// The normal pipeline scheduler is optimized for one-shot query execution: build the query-wide
// event graph, allocate PipelineExecutors/tasks, run the pipelines once, then tear that runtime
// state down. Recursive CTEs repeatedly re-enter the recursive member, often with tiny frontiers,
// so paying that setup/teardown cost every iteration quickly dominates the actual data processing.
//
// The helpers below keep the same pipeline/finalize semantics, but split out a recursive-specific
// runtime that can reset and reuse executors, selected operator state, and the recursive dependency
// topology across iterations.

// Recursive iterations often produce small batches. Aim for about half a vector of input per worker
// before increasing parallelism so scheduler overhead stays low on narrow recursive workloads.
static constexpr const idx_t RECURSIVE_ROWS_PER_THREAD = STANDARD_VECTOR_SIZE / 2;

static idx_t GetRecursiveThreadLimit(const RecursiveCTEState &state) {
	idx_t recursive_rows = 0;
	if (state.op.working_table && state.op.recursive_reference_count > 0) {
		recursive_rows += state.CurrentInputTable().Count() * state.op.recursive_reference_count;
	}
	if (state.op.recurring_table && state.op.recurring_reference_count > 0) {
		recursive_rows += state.op.recurring_table->Count() * state.op.recurring_reference_count;
	}
	return MaxValue<idx_t>((recursive_rows + RECURSIVE_ROWS_PER_THREAD - 1) / RECURSIVE_ROWS_PER_THREAD, 1);
}

static idx_t GetRecursivePipelineMaxThreads(RecursiveCTEState &state, Pipeline &pipeline) {
	auto max_threads = pipeline.GetMaxThreads();
	if (max_threads < 1) {
		max_threads = 1;
	}
	return MinValue(max_threads, GetRecursiveThreadLimit(state));
}

static void ExecuteRecursivePipelineInline(PipelineExecutor &pipeline_executor) {
	auto signal_state = make_shared_ptr<InterruptDoneSignalState>();
	pipeline_executor.SetInterruptState(InterruptState(weak_ptr<InterruptDoneSignalState>(signal_state)));
	while (true) {
		auto result = pipeline_executor.Execute();
		switch (result) {
		case PipelineExecuteResult::FINISHED:
			return;
		case PipelineExecuteResult::NOT_FINISHED:
			throw InternalException("Execute without limit should not return NOT_FINISHED");
		case PipelineExecuteResult::INTERRUPTED:
			signal_state->Await();
			break;
		}
	}
}

//! A task that executes a cached PipelineExecutor
class RecursiveCTETask : public ExecutorTask {
public:
	RecursiveCTETask(Pipeline &pipeline_p, shared_ptr<Event> event_p, PipelineExecutor &executor_p)
	    : ExecutorTask(pipeline_p.executor, std::move(event_p)), pipeline(pipeline_p), pipeline_executor(executor_p) {
	}

	Pipeline &pipeline;
	PipelineExecutor &pipeline_executor;

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		pipeline_executor.SetTaskForInterrupts(shared_from_this());

		if (mode == TaskExecutionMode::PROCESS_PARTIAL) {
			auto res = pipeline_executor.Execute(PARTIAL_CHUNK_COUNT);
			switch (res) {
			case PipelineExecuteResult::NOT_FINISHED:
				return TaskExecutionResult::TASK_NOT_FINISHED;
			case PipelineExecuteResult::INTERRUPTED:
				return TaskExecutionResult::TASK_BLOCKED;
			case PipelineExecuteResult::FINISHED:
				break;
			}
		} else {
			auto res = pipeline_executor.Execute();
			switch (res) {
			case PipelineExecuteResult::NOT_FINISHED:
				throw InternalException("Execute without limit should not return NOT_FINISHED");
			case PipelineExecuteResult::INTERRUPTED:
				return TaskExecutionResult::TASK_BLOCKED;
			case PipelineExecuteResult::FINISHED:
				break;
			}
		}

		event->FinishTask();
		return TaskExecutionResult::TASK_FINISHED;
	}

	bool TaskBlockedOnResult() const override {
		return pipeline_executor.RemainingSinkChunk();
	}

private:
	// Keep partial execution reasonably coarse so blocked pipelines make progress without creating tiny tasks.
	static constexpr const idx_t PARTIAL_CHUNK_COUNT = 50;
};

//! Recursive execute event.
//! We still use BasePipelineEvent for dependency tracking and task bookkeeping, but the stock
//! pipeline scheduling path is too expensive here: it assumes a one-shot execution, creates fresh
//! PipelineExecutors/tasks every time, and resets shared pipeline state immediately before launch.
//! Recursive CTEs need the same dependency semantics while reusing cached PipelineExecutors, and
//! root events must sometimes be reset up-front on the main thread to avoid reset-vs-execute races.
class RecursiveCTEPipelineEvent : public BasePipelineEvent {
public:
	RecursiveCTEPipelineEvent(shared_ptr<Pipeline> pipeline_p, RecursiveCTEState &state_p)
	    : BasePipelineEvent(std::move(pipeline_p)), state(state_p) {
	}

	RecursiveCTEState &state;
	bool prepared_for_schedule = false;

	void PrepareForSchedule() {
		// Root recursive pipeline events can be scheduled back-to-back while sharing operator instances.
		// Prepare their global pipeline state up-front on the main thread so later task execution does
		// not race with another root event resetting the same operator state.
		pipeline->ResetForReschedule(false);
		prepared_for_schedule = true;
	}

	void Schedule() override {
		// Sink state is prepared up-front from the main thread. Reinitialize the remaining
		// global state here, reusing existing state objects when operators expose reset hooks.
		// Dependency-free pipeline events can be prepared up-front on the main thread to avoid
		// racing with another root event that shares operator instances.
		if (!prepared_for_schedule) {
			pipeline->ResetForReschedule(false);
		}

		auto max_threads = GetRecursivePipelineMaxThreads(state, *pipeline);

		// Get or create cached executors for this pipeline
		auto &executors = state.GetCachedExecutors(*pipeline, max_threads);

		// Create tasks using cached executors
		vector<shared_ptr<Task>> tasks;
		for (idx_t i = 0; i < max_threads; i++) {
			executors[i]->PrepareForExecution();
			tasks.push_back(make_uniq<RecursiveCTETask>(*pipeline, shared_from_this(), *executors[i]));
		}
		SetTasks(std::move(tasks));
	}
};

//! Inline finish event for the single-thread recursive fast path.
//! PipelineFinishEvent is correct for the general scheduler, but it is intentionally scheduler-centric:
//! it expects finish work to go back through the task/event machinery. In the single-thread recursive
//! path that would mostly bounce control back into the scheduler just to run the finish work on the
//! current thread. This event preserves the same finalize contract, including BLOCKED handling, while
//! keeping the finish step inline with the cached execute/prepare stages.
class RecursiveCTEFinishEvent : public BasePipelineEvent {
public:
	explicit RecursiveCTEFinishEvent(shared_ptr<Pipeline> pipeline_p) : BasePipelineEvent(std::move(pipeline_p)) {
	}

	void Schedule() override {
		// This event is only used by the single-thread inline recursive fast path.
		// Blocking here is safe because the caller runs it synchronously and explicitly waits for completion.
		D_ASSERT(total_tasks == 0);
		total_tasks = 1;

		auto signal_state = make_shared_ptr<InterruptDoneSignalState>();
		InterruptState interrupt_state {weak_ptr<InterruptDoneSignalState>(signal_state)};
		auto sink = pipeline->GetSink();
		auto &operators = pipeline->GetIntermediateOperators();
		idx_t operator_idx = 0;

		while (true) {
			bool blocked = false;
			for (; operator_idx < operators.size(); operator_idx++) {
				auto &op = operators[operator_idx].get();
				if (!op.RequiresOperatorFinalize()) {
					continue;
				}
				OperatorFinalizeInput op_finalize_input {*op.op_state, interrupt_state};
				auto op_state = op.OperatorFinalize(*pipeline, *this, executor.context, op_finalize_input);
				if (op_state == OperatorFinalResultType::BLOCKED) {
					signal_state->Await();
					blocked = true;
					break;
				}
			}
			if (blocked) {
				continue;
			}

			OperatorSinkFinalizeInput finalize_input {*sink->sink_state, interrupt_state};
			auto sink_state = sink->Finalize(*pipeline, *this, executor.context, finalize_input);
			if (sink_state == SinkFinalizeType::BLOCKED) {
				signal_state->Await();
				continue;
			}

			sink->sink_state->state = sink_state;
			FinishTask();
			return;
		}
	}
};

struct RecursiveCTEEventStack {
	RecursiveCTEEventStack(shared_ptr<RecursiveCTEPipelineEvent> pipeline_event_p,
	                       shared_ptr<Event> pipeline_prepare_finish_event_p, shared_ptr<Event> pipeline_finish_event_p,
	                       shared_ptr<Event> pipeline_complete_event_p)
	    : pipeline_event(std::move(pipeline_event_p)),
	      pipeline_prepare_finish_event(std::move(pipeline_prepare_finish_event_p)),
	      pipeline_finish_event(std::move(pipeline_finish_event_p)),
	      pipeline_complete_event(std::move(pipeline_complete_event_p)) {
	}

	shared_ptr<RecursiveCTEPipelineEvent> pipeline_event;
	shared_ptr<Event> pipeline_prepare_finish_event;
	shared_ptr<Event> pipeline_finish_event;
	shared_ptr<Event> pipeline_complete_event;
};

using recursive_cte_event_map_t = reference_map_t<Pipeline, RecursiveCTEEventStack>;

enum class RecursiveCTEInlineStageType : uint8_t { EXECUTE, PREPARE_FINISH, FINISH };

struct RecursiveCTEInlineStage {
	RecursiveCTEInlineStage(RecursiveCTEInlineStageType type_p, Pipeline &pipeline_p)
	    : type(type_p), pipeline(pipeline_p), dependency_count(0) {
	}

	RecursiveCTEInlineStageType type;
	reference<Pipeline> pipeline;
	vector<idx_t> dependents;
	idx_t dependency_count;
};

struct RecursiveCTEInlineStageStack {
	RecursiveCTEInlineStageStack(idx_t execute_stage_p, idx_t prepare_finish_stage_p, idx_t finish_stage_p)
	    : execute_stage(execute_stage_p), prepare_finish_stage(prepare_finish_stage_p), finish_stage(finish_stage_p) {
	}

	idx_t execute_stage;
	idx_t prepare_finish_stage;
	idx_t finish_stage;
};

struct RecursiveCTEInlinePlan {
	vector<RecursiveCTEInlineStage> stages;
	vector<reference<Pipeline>> initialize_on_schedule_pipelines;
};

using recursive_cte_inline_stage_map_t = reference_map_t<Pipeline, RecursiveCTEInlineStageStack>;

static idx_t AddRecursiveInlineStage(RecursiveCTEInlinePlan &plan, RecursiveCTEInlineStageType type,
                                     Pipeline &pipeline) {
	plan.stages.emplace_back(type, pipeline);
	return plan.stages.size() - 1;
}

static void AddRecursiveInlineDependency(RecursiveCTEInlinePlan &plan, idx_t dependent_stage, idx_t dependency_stage) {
	auto &dependent = plan.stages[dependent_stage];
	dependent.dependency_count++;
	plan.stages[dependency_stage].dependents.push_back(dependent_stage);
}

static RecursiveCTEInlineStageStack AddRecursiveInlineStageStack(RecursiveCTEInlinePlan &plan, Pipeline &pipeline) {
	auto execute = AddRecursiveInlineStage(plan, RecursiveCTEInlineStageType::EXECUTE, pipeline);
	auto prepare_finish = AddRecursiveInlineStage(plan, RecursiveCTEInlineStageType::PREPARE_FINISH, pipeline);
	auto finish = AddRecursiveInlineStage(plan, RecursiveCTEInlineStageType::FINISH, pipeline);
	AddRecursiveInlineDependency(plan, prepare_finish, execute);
	AddRecursiveInlineDependency(plan, finish, prepare_finish);
	return RecursiveCTEInlineStageStack(execute, prepare_finish, finish);
}

static RecursiveCTEEventStack CreateRecursiveEventStack(const shared_ptr<Pipeline> &pipeline,
                                                        RecursiveCTEState &state) {
	auto execute = make_shared_ptr<RecursiveCTEPipelineEvent>(pipeline, state);
	auto prepare_finish = make_shared_ptr<PipelinePrepareFinishEvent>(pipeline);
	auto finish = make_shared_ptr<PipelineFinishEvent>(pipeline);
	prepare_finish->AddDependency(*execute);
	finish->AddDependency(*prepare_finish);
	return RecursiveCTEEventStack(execute, prepare_finish, finish, nullptr);
}

enum class RecursiveCTEMetaPipelineEntryType : uint8_t {
	BASE,
	SHARED_FINISH_GROUP,
	HAS_FINISH_EVENT,
	SHARED_BASE_FINISH
};

struct RecursiveCTEMetaPipelinePlanEntry {
	RecursiveCTEMetaPipelinePlanEntry(Pipeline &pipeline_p, RecursiveCTEMetaPipelineEntryType type_p,
	                                  optional_ptr<Pipeline> finish_group_p = nullptr)
	    : pipeline(pipeline_p), type(type_p), finish_group(finish_group_p) {
	}

	reference<Pipeline> pipeline;
	RecursiveCTEMetaPipelineEntryType type;
	optional_ptr<Pipeline> finish_group;
};

struct RecursiveCTEMetaPipelinePlan {
	vector<RecursiveCTEMetaPipelinePlanEntry> entries;
	vector<reference<Pipeline>> initialize_on_schedule_pipelines;
};

static bool RequiresInitializeOnSchedule(Pipeline &pipeline) {
	auto source = pipeline.GetSource();
	if (!source || source->type != PhysicalOperatorType::TABLE_SCAN) {
		return false;
	}
	auto &table_function = source->Cast<PhysicalTableScan>();
	return table_function.function.global_initialization == TableFunctionInitialization::INITIALIZE_ON_SCHEDULE;
}

static RecursiveCTEMetaPipelinePlan BuildRecursiveMetaPipelinePlan(MetaPipeline &meta_pipeline) {
	// MetaPipeline already knows how to classify pipelines for the normal scheduler, but recursive
	// execution needs that classification in two different forms:
	// 1. a cached scheduler-free stage plan for the single-thread inline fast path
	// 2. a per-iteration Event graph for the multi-threaded path
	// Materializing a tiny neutral plan keeps both paths in sync without forcing the inline path to
	// instantiate Event objects it will never schedule.
	RecursiveCTEMetaPipelinePlan result;

	vector<shared_ptr<Pipeline>> pipelines;
	meta_pipeline.GetPipelines(pipelines, false);
	result.entries.reserve(pipelines.size());
	result.entries.emplace_back(*meta_pipeline.GetBasePipeline(), RecursiveCTEMetaPipelineEntryType::BASE);

	for (idx_t i = 1; i < pipelines.size(); i++) {
		auto &pipeline = pipelines[i];
		auto finish_group = meta_pipeline.GetFinishGroup(*pipeline);
		if (finish_group) {
			result.entries.emplace_back(*pipeline, RecursiveCTEMetaPipelineEntryType::SHARED_FINISH_GROUP,
			                            finish_group.get());
			continue;
		}
		if (meta_pipeline.HasFinishEvent(*pipeline)) {
			result.entries.emplace_back(*pipeline, RecursiveCTEMetaPipelineEntryType::HAS_FINISH_EVENT);
			continue;
		}
		result.entries.emplace_back(*pipeline, RecursiveCTEMetaPipelineEntryType::SHARED_BASE_FINISH);
	}

	for (auto &pipeline : pipelines) {
		if (RequiresInitializeOnSchedule(*pipeline)) {
			result.initialize_on_schedule_pipelines.push_back(*pipeline);
		}
	}
	return result;
}

static unique_ptr<RecursiveCTEInlinePlan>
BuildRecursiveInlinePlan(const vector<shared_ptr<MetaPipeline>> &meta_pipelines) {
	// This is the scheduler-free equivalent of the recursive event graph. It is only used when the
	// current iteration is capped to one thread; in that case, preserving the same execute ->
	// prepare-finish -> finish ordering matters, but paying full scheduler/task overhead does not.
	auto plan = make_uniq<RecursiveCTEInlinePlan>();
	recursive_cte_inline_stage_map_t stage_map;
	for (auto &meta_pipeline : meta_pipelines) {
		auto meta_pipeline_plan = BuildRecursiveMetaPipelinePlan(*meta_pipeline);
		RecursiveCTEInlineStageStack base_stack(DConstants::INVALID_INDEX, DConstants::INVALID_INDEX,
		                                        DConstants::INVALID_INDEX);
		for (auto &entry : meta_pipeline_plan.entries) {
			auto &pipeline = entry.pipeline.get();
			if (entry.type != RecursiveCTEMetaPipelineEntryType::BASE) {
				D_ASSERT(base_stack.execute_stage != DConstants::INVALID_INDEX);
			}
			switch (entry.type) {
			case RecursiveCTEMetaPipelineEntryType::BASE: {
				base_stack = AddRecursiveInlineStageStack(*plan, pipeline);
				stage_map.emplace(reference<Pipeline>(pipeline), base_stack);
				break;
			}
			case RecursiveCTEMetaPipelineEntryType::SHARED_FINISH_GROUP: {
				D_ASSERT(entry.finish_group);
				auto group_entry = stage_map.find(*entry.finish_group);
				D_ASSERT(group_entry != stage_map.end());
				auto execute = AddRecursiveInlineStage(*plan, RecursiveCTEInlineStageType::EXECUTE, pipeline);
				AddRecursiveInlineDependency(*plan, execute, base_stack.finish_stage);
				AddRecursiveInlineDependency(*plan, group_entry->second.prepare_finish_stage, execute);
				stage_map.emplace(reference<Pipeline>(pipeline),
				                  RecursiveCTEInlineStageStack(execute, group_entry->second.prepare_finish_stage,
				                                               group_entry->second.finish_stage));
				break;
			}
			case RecursiveCTEMetaPipelineEntryType::HAS_FINISH_EVENT: {
				auto pipeline_stack = AddRecursiveInlineStageStack(*plan, pipeline);
				AddRecursiveInlineDependency(*plan, pipeline_stack.execute_stage, base_stack.finish_stage);
				stage_map.emplace(reference<Pipeline>(pipeline), pipeline_stack);
				break;
			}
			case RecursiveCTEMetaPipelineEntryType::SHARED_BASE_FINISH: {
				auto execute = AddRecursiveInlineStage(*plan, RecursiveCTEInlineStageType::EXECUTE, pipeline);
				AddRecursiveInlineDependency(*plan, base_stack.prepare_finish_stage, execute);
				stage_map.emplace(
				    reference<Pipeline>(pipeline),
				    RecursiveCTEInlineStageStack(execute, base_stack.prepare_finish_stage, base_stack.finish_stage));
				break;
			}
			default:
				throw InternalException("Unsupported recursive meta pipeline plan entry");
			}
		}
		for (auto &pipeline : meta_pipeline_plan.initialize_on_schedule_pipelines) {
			plan->initialize_on_schedule_pipelines.push_back(pipeline);
		}
	}

	for (auto &entry : stage_map) {
		auto &pipeline = entry.first.get();
		for (auto &dependency : pipeline.GetDependencies()) {
			auto dep = dependency.lock();
			D_ASSERT(dep);
			auto stage_entry = stage_map.find(*dep);
			if (stage_entry == stage_map.end()) {
				continue;
			}
			AddRecursiveInlineDependency(*plan, entry.second.execute_stage, stage_entry->second.finish_stage);
		}
	}

	for (auto &meta_pipeline : meta_pipelines) {
		for (auto &entry : meta_pipeline->GetDependencies()) {
			auto pipeline_entry = stage_map.find(entry.first.get());
			if (pipeline_entry == stage_map.end()) {
				continue;
			}
			for (auto &dependency : entry.second) {
				auto dependency_entry = stage_map.find(dependency.get());
				if (dependency_entry == stage_map.end()) {
					continue;
				}
				AddRecursiveInlineDependency(*plan, pipeline_entry->second.execute_stage,
				                             dependency_entry->second.execute_stage);
			}
		}
	}

	for (auto &meta_pipeline : meta_pipelines) {
		vector<shared_ptr<MetaPipeline>> children;
		meta_pipeline->GetMetaPipelines(children, false, true);
		for (auto &child1 : children) {
			if (child1->Type() != MetaPipelineType::JOIN_BUILD) {
				continue;
			}
			auto child1_entry = stage_map.find(*child1->GetBasePipeline());
			if (child1_entry == stage_map.end()) {
				continue;
			}

			for (auto &child2 : children) {
				if (child2->Type() != MetaPipelineType::JOIN_BUILD || child1.get() == child2.get()) {
					continue;
				}
				if (child1->GetParent().get() != child2->GetParent().get()) {
					continue;
				}
				auto child2_entry = stage_map.find(*child2->GetBasePipeline());
				if (child2_entry == stage_map.end()) {
					continue;
				}

				AddRecursiveInlineDependency(*plan, child1_entry->second.prepare_finish_stage,
				                             child2_entry->second.execute_stage);
				AddRecursiveInlineDependency(*plan, child1_entry->second.finish_stage,
				                             child2_entry->second.prepare_finish_stage);
			}
		}
	}
	return plan;
}

static bool OperatorDirectlyDependsOnRecursiveInput(const PhysicalOperator &op, TableIndex cte_index) {
	if (op.type == PhysicalOperatorType::DELIM_SCAN) {
		return true;
	}
	if (op.type == PhysicalOperatorType::RECURSIVE_CTE_SCAN ||
	    op.type == PhysicalOperatorType::RECURSIVE_RECURRING_CTE_SCAN) {
		auto &scan = op.Cast<PhysicalColumnDataScan>();
		return scan.cte_index == cte_index;
	}
	if (op.type == PhysicalOperatorType::LEFT_DELIM_JOIN || op.type == PhysicalOperatorType::RIGHT_DELIM_JOIN) {
		auto &delim_join = op.Cast<PhysicalDelimJoin>();
		for (auto &scan : delim_join.delim_scans) {
			if (OperatorDirectlyDependsOnRecursiveInput(scan.get(), cte_index)) {
				return true;
			}
		}
	}
	return false;
}

static bool PipelineDirectlyDependsOnRecursiveInput(Pipeline &pipeline, TableIndex cte_index) {
	auto source = pipeline.GetSource();
	if (source && OperatorDirectlyDependsOnRecursiveInput(*source, cte_index)) {
		return true;
	}
	for (auto &op : pipeline.GetIntermediateOperators()) {
		if (OperatorDirectlyDependsOnRecursiveInput(op.get(), cte_index)) {
			return true;
		}
	}
	return false;
}

static reference_set_t<const MetaPipeline>
FindInvariantRecursiveMetaPipelines(const vector<shared_ptr<MetaPipeline>> &meta_pipelines, TableIndex cte_index) {
	// By default the recursive executor reruns every meta-pipeline in the recursive member each
	// iteration because the generic scheduling infrastructure does not know which build-side subplans
	// are independent of the current recursive frontier. For small recursive workloads that repeats
	// expensive setup for static subplans. Classify only JOIN_BUILD meta-pipelines that are
	// transitively independent from the recursive input and whose build side does not propagate rows
	// back into the recursive result, so their materialized state can safely survive later iterations.
	reference_map_t<const Pipeline, reference<const MetaPipeline>> pipeline_to_meta_pipeline;
	reference_set_t<const MetaPipeline> variant_meta_pipelines;

	for (auto &meta_pipeline : meta_pipelines) {
		vector<shared_ptr<Pipeline>> pipelines;
		meta_pipeline->GetPipelines(pipelines, false);
		for (auto &pipeline : pipelines) {
			pipeline_to_meta_pipeline.emplace(*pipeline, *meta_pipeline);
			if (PipelineDirectlyDependsOnRecursiveInput(*pipeline, cte_index)) {
				variant_meta_pipelines.insert(*meta_pipeline);
			}
		}
	}

	bool changed = true;
	while (changed) {
		changed = false;
		for (auto &meta_pipeline : meta_pipelines) {
			if (variant_meta_pipelines.find(*meta_pipeline) != variant_meta_pipelines.end()) {
				continue;
			}

			bool depends_on_variant = false;
			vector<shared_ptr<Pipeline>> pipelines;
			meta_pipeline->GetPipelines(pipelines, false);
			for (auto &pipeline : pipelines) {
				for (auto &dependency : pipeline->GetDependencies()) {
					auto dep = dependency.lock();
					if (!dep) {
						continue;
					}
					auto dep_entry = pipeline_to_meta_pipeline.find(*dep);
					if (dep_entry == pipeline_to_meta_pipeline.end()) {
						continue;
					}
					if (variant_meta_pipelines.find(dep_entry->second) != variant_meta_pipelines.end()) {
						depends_on_variant = true;
						break;
					}
				}
				if (depends_on_variant) {
					break;
				}
			}
			if (!depends_on_variant) {
				for (auto &entry : meta_pipeline->GetDependencies()) {
					for (auto &dependency : entry.second) {
						auto dep_entry = pipeline_to_meta_pipeline.find(dependency.get());
						if (dep_entry == pipeline_to_meta_pipeline.end()) {
							continue;
						}
						if (variant_meta_pipelines.find(dep_entry->second) != variant_meta_pipelines.end()) {
							depends_on_variant = true;
							break;
						}
					}
					if (depends_on_variant) {
						break;
					}
				}
			}
			if (depends_on_variant) {
				variant_meta_pipelines.insert(*meta_pipeline);
				changed = true;
			}
		}
	}

	reference_set_t<const MetaPipeline> result;
	for (auto &meta_pipeline : meta_pipelines) {
		if (meta_pipeline->Type() != MetaPipelineType::JOIN_BUILD) {
			continue;
		}
		auto sink = meta_pipeline->GetSink();
		if (!sink) {
			continue;
		}

		bool can_cache_build = false;
		switch (sink->type) {
		case PhysicalOperatorType::HASH_JOIN: {
			auto &hash_join = sink->Cast<PhysicalHashJoin>();
			can_cache_build = !PropagatesBuildSide(hash_join.join_type);
			break;
		}
		case PhysicalOperatorType::NESTED_LOOP_JOIN: {
			auto &nested_loop_join = sink->Cast<PhysicalNestedLoopJoin>();
			can_cache_build = !PropagatesBuildSide(nested_loop_join.join_type);
			break;
		}
		case PhysicalOperatorType::BLOCKWISE_NL_JOIN: {
			auto &blockwise_nl_join = sink->Cast<PhysicalBlockwiseNLJoin>();
			can_cache_build = !PropagatesBuildSide(blockwise_nl_join.join_type);
			break;
		}
		case PhysicalOperatorType::CROSS_PRODUCT:
			can_cache_build = true;
			break;
		default:
			break;
		}
		if (!can_cache_build) {
			continue;
		}
		if (variant_meta_pipelines.find(*meta_pipeline) == variant_meta_pipelines.end()) {
			result.insert(*meta_pipeline);
		}
	}
	return result;
}

static vector<shared_ptr<MetaPipeline>> GetActiveRecursiveMetaPipelines(const PhysicalRecursiveCTE &op,
                                                                        RecursiveCTEState &state) {
	vector<shared_ptr<MetaPipeline>> meta_pipelines;
	op.recursive_meta_pipeline->GetMetaPipelines(meta_pipelines, true, false);
	if (!state.allow_executor_reuse || !state.invariant_meta_pipelines_materialized ||
	    op.invariant_meta_pipelines.empty()) {
		return meta_pipelines;
	}

	vector<shared_ptr<MetaPipeline>> active_meta_pipelines;
	active_meta_pipelines.reserve(meta_pipelines.size());
	for (auto &meta_pipeline : meta_pipelines) {
		if (op.invariant_meta_pipelines.find(*meta_pipeline) == op.invariant_meta_pipelines.end()) {
			active_meta_pipelines.push_back(meta_pipeline);
		}
	}
	return active_meta_pipelines;
}

static void ConfigureInvariantRecursiveBuildReuse(const PhysicalRecursiveCTE &op, bool preserve_build) {
	for (auto &meta_pipeline_ref : op.invariant_meta_pipelines) {
		auto sink = meta_pipeline_ref.get().GetSink();
		if (!sink || sink->type != PhysicalOperatorType::HASH_JOIN) {
			continue;
		}
		sink->Cast<PhysicalHashJoin>().SetPreserveBuildForRecursiveReuse(preserve_build);
	}
}

static void ProcessRecursiveExecutorTasks(Executor &executor) {
	// Only drain the recursive executor here. Re-entering the outer query executor while waiting for
	// recursive work can run unrelated tasks and used to break recursive completion tracking.
	if (!executor.WorkOnTasks()) {
		executor.WaitForTask();
	}
	if (executor.HasError()) {
		executor.ThrowException();
	}
}

static void WaitForRecursiveEvents(Executor &executor, vector<shared_ptr<Event>> &events) {
	while (true) {
		ProcessRecursiveExecutorTasks(executor);
		bool finished = true;
		for (auto &event : events) {
			if (!event->IsFinished()) {
				finished = false;
				break;
			}
		}
		if (finished) {
			break;
		}
	}
}

static void WaitForRecursiveEvent(Executor &executor, Event &event) {
	while (!event.IsFinished()) {
		ProcessRecursiveExecutorTasks(executor);
	}
}

// Build the recursive Event chain for one MetaPipeline using the neutral classification from
// BuildRecursiveMetaPipelinePlan().
//
// Why this exists instead of letting MetaPipeline schedule itself directly:
// - the normal scheduler builds a query-wide, one-shot Event graph; recursive execution only wants
//   to rebuild the recursive subset for the current iteration
// - recursive execution may omit already-materialized invariant build pipelines, so the event graph
//   is not just a replay of the original query schedule
// - the recursive path needs RecursiveCTEPipelineEvent at the execute edge so it can reuse cached
//   PipelineExecutors instead of constructing fresh ones every iteration
//
// The BASE pipeline anchors the chain for the whole meta-pipeline. Other pipelines either share the
// BASE finish event, own their own finish chain, or share another pipeline's finish group, mirroring
// the same finish/finalize semantics as the normal scheduler with less per-iteration setup work.
static void ScheduleRecursiveMetaPipeline(const shared_ptr<MetaPipeline> &meta_pipeline, RecursiveCTEState &state,
                                          Executor &executor, recursive_cte_event_map_t &event_map,
                                          vector<shared_ptr<Event>> &events) {
	auto meta_pipeline_plan = BuildRecursiveMetaPipelinePlan(*meta_pipeline);
	RecursiveCTEEventStack base_stack(nullptr, nullptr, nullptr, nullptr);
	for (auto &entry : meta_pipeline_plan.entries) {
		auto &pipeline = entry.pipeline.get();
		if (entry.type != RecursiveCTEMetaPipelineEntryType::BASE) {
			D_ASSERT(base_stack.pipeline_event);
			D_ASSERT(base_stack.pipeline_prepare_finish_event);
			D_ASSERT(base_stack.pipeline_finish_event);
			D_ASSERT(base_stack.pipeline_complete_event);
		}
		switch (entry.type) {
		case RecursiveCTEMetaPipelineEntryType::BASE: {
			// The base pipeline owns the full execute -> prepare-finish -> finish -> complete chain.
			base_stack = CreateRecursiveEventStack(pipeline.shared_from_this(), state);
			auto completion = make_shared_ptr<PipelineCompleteEvent>(executor, false);
			completion->AddDependency(*base_stack.pipeline_finish_event);
			base_stack.pipeline_complete_event = completion;
			event_map.emplace(reference<Pipeline>(pipeline), base_stack);
			events.push_back(base_stack.pipeline_event);
			events.push_back(base_stack.pipeline_prepare_finish_event);
			events.push_back(base_stack.pipeline_finish_event);
			events.push_back(completion);
			break;
		}
		case RecursiveCTEMetaPipelineEntryType::SHARED_FINISH_GROUP: {
			// This pipeline executes independently, but its finalize work is merged into an existing
			// finish group selected by MetaPipeline.
			D_ASSERT(entry.finish_group);
			auto group_entry = event_map.find(*entry.finish_group);
			D_ASSERT(group_entry != event_map.end());
			auto execute = make_shared_ptr<RecursiveCTEPipelineEvent>(pipeline.shared_from_this(), state);
			execute->AddDependency(*base_stack.pipeline_finish_event);
			group_entry->second.pipeline_prepare_finish_event->AddDependency(*execute);
			event_map.emplace(reference<Pipeline>(pipeline),
			                  RecursiveCTEEventStack(execute, group_entry->second.pipeline_prepare_finish_event,
			                                         group_entry->second.pipeline_finish_event,
			                                         base_stack.pipeline_complete_event));
			events.push_back(execute);
			break;
		}
		case RecursiveCTEMetaPipelineEntryType::HAS_FINISH_EVENT: {
			// This pipeline needs its own prepare/finish chain, but still joins the BASE completion edge
			// so the whole meta-pipeline reports done only after every finish stage completed.
			auto pipeline_stack = CreateRecursiveEventStack(pipeline.shared_from_this(), state);
			pipeline_stack.pipeline_event->AddDependency(*base_stack.pipeline_finish_event);
			base_stack.pipeline_complete_event->AddDependency(*pipeline_stack.pipeline_finish_event);
			pipeline_stack.pipeline_complete_event = base_stack.pipeline_complete_event;
			event_map.emplace(reference<Pipeline>(pipeline), pipeline_stack);
			events.push_back(pipeline_stack.pipeline_event);
			events.push_back(pipeline_stack.pipeline_prepare_finish_event);
			events.push_back(pipeline_stack.pipeline_finish_event);
			break;
		}
		case RecursiveCTEMetaPipelineEntryType::SHARED_BASE_FINISH: {
			// This is the cheapest case: only the execute stage is private, while prepare/finish reuse
			// the BASE pipeline chain.
			auto execute = make_shared_ptr<RecursiveCTEPipelineEvent>(pipeline.shared_from_this(), state);
			base_stack.pipeline_prepare_finish_event->AddDependency(*execute);
			event_map.emplace(reference<Pipeline>(pipeline),
			                  RecursiveCTEEventStack(execute, base_stack.pipeline_prepare_finish_event,
			                                         base_stack.pipeline_finish_event,
			                                         base_stack.pipeline_complete_event));
			events.push_back(execute);
			break;
		}
		default:
			throw InternalException("Unsupported recursive meta pipeline plan entry");
		}
	}

	for (auto &pipeline : meta_pipeline_plan.initialize_on_schedule_pipelines) {
		pipeline.get().ResetSource(true);
	}
}

static void ScheduleRecursivePipelines(const vector<shared_ptr<MetaPipeline>> &meta_pipelines, RecursiveCTEState &state,
                                       Executor &executor, vector<shared_ptr<Event>> &events) {
	// Rebuild only the recursive subset of the event graph for this iteration. Reusing the query's
	// original scheduler wiring is not enough here: recursive iterations may omit already-materialized
	// invariant build pipelines, and root execute events need a dedicated prepare step before any task
	// starts so shared pipeline state is not reset concurrently with active execution.
	recursive_cte_event_map_t event_map;
	for (auto &meta_pipeline : meta_pipelines) {
		ScheduleRecursiveMetaPipeline(meta_pipeline, state, executor, event_map, events);
	}

	for (auto &entry : event_map) {
		auto &pipeline = entry.first.get();
		for (auto &dependency : pipeline.GetDependencies()) {
			auto dep = dependency.lock();
			D_ASSERT(dep);
			auto event_entry = event_map.find(*dep);
			if (event_entry == event_map.end()) {
				continue;
			}
			entry.second.pipeline_event->AddDependency(*event_entry->second.pipeline_complete_event);
		}
	}

	for (auto &meta_pipeline : meta_pipelines) {
		for (auto &entry : meta_pipeline->GetDependencies()) {
			auto pipeline_entry = event_map.find(entry.first.get());
			if (pipeline_entry == event_map.end()) {
				continue;
			}
			for (auto &dependency : entry.second) {
				auto dependency_entry = event_map.find(dependency.get());
				if (dependency_entry == event_map.end()) {
					continue;
				}
				pipeline_entry->second.pipeline_event->AddDependency(*dependency_entry->second.pipeline_event);
			}
		}
	}

	for (auto &meta_pipeline : meta_pipelines) {
		vector<shared_ptr<MetaPipeline>> children;
		meta_pipeline->GetMetaPipelines(children, false, true);
		for (auto &child1 : children) {
			if (child1->Type() != MetaPipelineType::JOIN_BUILD) {
				continue;
			}
			auto child1_entry = event_map.find(*child1->GetBasePipeline());
			if (child1_entry == event_map.end()) {
				continue;
			}

			for (auto &child2 : children) {
				if (child2->Type() != MetaPipelineType::JOIN_BUILD || child1.get() == child2.get()) {
					continue;
				}
				if (child1->GetParent().get() != child2->GetParent().get()) {
					continue;
				}
				auto child2_entry = event_map.find(*child2->GetBasePipeline());
				if (child2_entry == event_map.end()) {
					continue;
				}

				child1_entry->second.pipeline_prepare_finish_event->AddDependency(*child2_entry->second.pipeline_event);
				child1_entry->second.pipeline_finish_event->AddDependency(
				    *child2_entry->second.pipeline_prepare_finish_event);
			}
		}
	}

	for (auto &entry : event_map) {
		if (!entry.second.pipeline_event->HasDependencies()) {
			entry.second.pipeline_event->PrepareForSchedule();
		}
	}

	for (auto &event : events) {
		if (!event->HasDependencies()) {
			event->Schedule();
		}
	}
}

static void ExecuteRecursivePipelineFinishInline(Pipeline &pipeline, Executor &executor) {
	auto finish_event = make_shared_ptr<RecursiveCTEFinishEvent>(pipeline.shared_from_this());
	auto complete_event = make_shared_ptr<PipelineCompleteEvent>(executor, false);
	complete_event->AddDependency(*finish_event);
	finish_event->Schedule();
	if (!complete_event->IsFinished()) {
		WaitForRecursiveEvent(executor, *complete_event);
	}
}

static void ExecuteRecursiveInlinePlan(RecursiveCTEState &state, Executor &executor,
                                       const RecursiveCTEInlinePlan &plan) {
	for (auto &pipeline : plan.initialize_on_schedule_pipelines) {
		pipeline.get().ResetSource(true);
	}

	vector<idx_t> remaining_dependencies;
	remaining_dependencies.reserve(plan.stages.size());
	vector<idx_t> ready_stages;
	ready_stages.reserve(plan.stages.size());
	for (idx_t stage_idx = 0; stage_idx < plan.stages.size(); stage_idx++) {
		auto dependency_count = plan.stages[stage_idx].dependency_count;
		remaining_dependencies.push_back(dependency_count);
		if (dependency_count == 0) {
			ready_stages.push_back(stage_idx);
		}
	}

	for (idx_t ready_idx = 0; ready_idx < ready_stages.size(); ready_idx++) {
		auto stage_idx = ready_stages[ready_idx];
		auto &stage = plan.stages[stage_idx];
		auto &pipeline = stage.pipeline.get();
		switch (stage.type) {
		case RecursiveCTEInlineStageType::EXECUTE: {
			pipeline.ResetForReschedule(false);
			auto max_threads = GetRecursivePipelineMaxThreads(state, pipeline);
			D_ASSERT(max_threads == 1);
			auto &executors = state.GetCachedExecutors(pipeline, max_threads);
			executors[0]->PrepareForExecution();
			ExecuteRecursivePipelineInline(*executors[0]);
			break;
		}
		case RecursiveCTEInlineStageType::PREPARE_FINISH:
			pipeline.PrepareFinalize();
			break;
		case RecursiveCTEInlineStageType::FINISH:
			ExecuteRecursivePipelineFinishInline(pipeline, executor);
			break;
		default:
			throw InternalException("Unsupported recursive inline stage");
		}

		for (auto dependent_stage : stage.dependents) {
			auto &remaining = remaining_dependencies[dependent_stage];
			D_ASSERT(remaining > 0);
			remaining--;
			if (remaining == 0) {
				ready_stages.push_back(dependent_stage);
			}
		}
	}

	if (ready_stages.size() != plan.stages.size()) {
		throw InternalException("Recursive inline plan did not schedule every stage");
	}
}

unique_ptr<GlobalSinkState> PhysicalRecursiveCTE::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<RecursiveCTEState>(context, *this);
}

idx_t PhysicalRecursiveCTE::ProbeHT(DataChunk &chunk, RecursiveCTEState &state) const {
	// Use the HT to eliminate duplicate rows
	idx_t new_group_count = state.ht->FindOrCreateGroups(chunk, state.dummy_addresses, state.new_groups);

	// we only return entries we have not seen before (i.e. new groups)
	chunk.Slice(state.new_groups, new_group_count);

	return new_group_count;
}

static void GatherChunk(DataChunk &output_chunk, DataChunk &input_chunk, const vector<idx_t> &idx_set) {
	idx_t chunk_index = 0;
	for (auto &group_idx : idx_set) {
		output_chunk.data[chunk_index++].Reference(input_chunk.data[group_idx]);
	}
	output_chunk.SetCardinality(input_chunk.size());
}

static void ScatterChunk(DataChunk &output_chunk, DataChunk &input_chunk, const vector<idx_t> &idx_set) {
	idx_t chunk_index = 0;
	for (auto &group_idx : idx_set) {
		output_chunk.data[group_idx].Reference(input_chunk.data[chunk_index++]);
	}
	output_chunk.SetCardinality(input_chunk.size());
}

SinkResultType PhysicalRecursiveCTE::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<RecursiveCTEState>();

	lock_guard<mutex> guard(gstate.intermediate_table_lock);
	if (!using_key) {
		auto &output = gstate.CurrentOutputTable();
		auto &append_state = gstate.CurrentOutputAppendState();
		if (!union_all) {
			idx_t match_count = ProbeHT(chunk, gstate);
			if (match_count > 0) {
				output.Append(append_state, chunk);
			}
		} else {
			output.Append(append_state, chunk);
		}
	} else {
		// Split incoming DataChunk into payload and keys using the cached distinct_rows chunk
		gstate.distinct_rows.Reset();
		GatherChunk(gstate.distinct_rows, chunk, distinct_idx);

		// Add result of recursive anchor to intermediate table
		gstate.intermediate_table.Append(gstate.intermediate_append_state, chunk);

		// Execute aggregate expressions on chunk if any
		if (!gstate.executor.expressions.empty()) {
			gstate.payload_rows.Reset();
			gstate.executor.Execute(chunk, gstate.payload_rows);
		}

		// Add the result of the executed expressions to the hash table
		gstate.ht->AddChunk(gstate.distinct_rows, gstate.payload_rows, AggregateType::NON_DISTINCT);
	}

	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
SourceResultType PhysicalRecursiveCTE::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                       OperatorSourceInput &input) const {
	auto &gstate = sink_state->Cast<RecursiveCTEState>();
	if (!gstate.initialized) {
		if (!using_key) {
			gstate.CurrentOutputTable().InitializeScan(gstate.scan_state);
		} else {
			gstate.ht->InitializeScan(gstate.ht_scan_state);
			recurring_table->InitializeScan(gstate.scan_state);
		}
		gstate.finished_scan = false;
		gstate.initialized = true;
	}
	while (chunk.size() == 0) {
		if (!gstate.finished_scan) {
			if (!using_key) {
				// scan any chunks we have collected so far
				gstate.CurrentOutputTable().Scan(gstate.scan_state, chunk);
			}
			if (chunk.size() == 0) {
				gstate.finished_scan = true;
			} else {
				break;
			}
		} else {
			// we have run out of chunks
			// now we need to recurse
			// we set up the working table as the data we gathered in this iteration of the recursion
			auto &current_output = gstate.CurrentOutputTable();

			// After an iteration, we reset the recurring table
			// and fill it up with the new hash table rows for the next iteration.
			if (using_key && ref_recurring && current_output.Count() != 0) {
				gstate.ResetRecurringTable();
				AggregateHTScanState scan_state;
				gstate.ht->InitializeScan(scan_state);
				auto &result = gstate.source_result;
				auto &payload_rows = gstate.source_payload_rows;
				auto &distinct_rows = gstate.source_distinct_rows;

				while (gstate.ht->Scan(scan_state, distinct_rows, payload_rows)) {
					result.Reset();
					// Populate the result DataChunk with the keys and the payload.
					ScatterChunk(result, distinct_rows, distinct_idx);
					ScatterChunk(result, payload_rows, payload_idx);
					// Append the result to the recurring table.
					recurring_table->Append(gstate.recurring_append_state, result);
				}
			} else if (ref_recurring && current_output.Count() != 0) {
				// we need to populate the recurring table from the intermediate table
				// careful: we can not just use Combine here, because this destroys the intermediate table
				// instead we need to scan and append to create a copy
				// Note: as we are in the "normal" recursion case here, not the USING KEY case,
				// we can just scan the intermediate table directly, instead of going through the HT
				ColumnDataScanState scan_state;
				current_output.InitializeScan(scan_state);
				while (current_output.Scan(scan_state, gstate.source_result)) {
					recurring_table->Append(gstate.recurring_append_state, gstate.source_result);
				}
			}

			gstate.finished_scan = false;
			if (!using_key) {
				gstate.AdvanceIterationBuffers();
				gstate.ResetCurrentOutputTableForReuse();
				gstate.RebindRecursiveScans();
			} else {
				working_table->Reset();
				working_table->Combine(gstate.intermediate_table);
				gstate.InitializeIntermediateAppend();
			}

			// Pre-grow the dedup HT to avoid costly Resize + ReinsertTuples during the next Sink phase.
			// current_output.Count() is the count of rows output in the previous iteration — an upper bound
			// on the number of new unique rows the next iteration can add (since the recursion is converging).
			if (!union_all) {
				const idx_t expected_new = current_output.Count();
				if (expected_new > 0) {
					const idx_t desired_capacity =
					    GroupedAggregateHashTable::GetCapacityForCount(gstate.ht->Count() + expected_new);
					if (desired_capacity > gstate.ht->Capacity()) {
						gstate.ht->Resize(desired_capacity);
					}
				}
			}

			// now we need to re-execute all of the pipelines that depend on the recursion
			ExecuteRecursivePipelines(context);

			// check if we obtained any results
			// if not, we are done
			if (gstate.CurrentOutputTable().Count() == 0) {
				gstate.finished_scan = true;
				if (using_key) {
					auto &payload_rows = gstate.source_payload_rows;
					auto &distinct_rows = gstate.source_distinct_rows;
					distinct_rows.Reset();
					payload_rows.Reset();
					gstate.ht->Scan(gstate.ht_scan_state, distinct_rows, payload_rows);
					ScatterChunk(chunk, distinct_rows, distinct_idx);
					ScatterChunk(chunk, payload_rows, payload_idx);
				}
				break;
			}
			if (!using_key) {
				// set up the scan again
				gstate.CurrentOutputTable().InitializeScan(gstate.scan_state);
			}
		}
	}

	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}

void PhysicalRecursiveCTE::ExecuteRecursivePipelines(ExecutionContext &context) const {
	if (!recursive_meta_pipeline) {
		throw InternalException("Missing meta pipeline for recursive CTE");
	}
	D_ASSERT(recursive_meta_pipeline->HasRecursiveCTE());

	auto &gstate = sink_state->Cast<RecursiveCTEState>();
	auto &executor = recursive_meta_pipeline->GetExecutor();
	auto allow_reuse = context.client.config.enable_caching_operators;
	auto active_meta_pipelines = GetActiveRecursiveMetaPipelines(*this, gstate);
	auto can_cache_invariant_meta_pipelines = allow_reuse && !invariant_meta_pipelines.empty();

	// The generic executor path would rebuild the recursive event graph and allocate fresh
	// PipelineExecutors/tasks every iteration. Recursive execution keeps the already-built recursive
	// MetaPipeline, resets only the state that must change, optionally skips invariant build pipelines
	// after they have been materialized once, and then picks between:
	// - a cached inline dependency plan when the iteration is effectively single-threaded
	// - a custom recursive Event graph when the iteration still benefits from parallel execution

	// Reset sink state from the main thread so recursive iterations can reuse or recreate
	// pipeline-local global sinks without tearing down the rest of the runtime state graph.
	for (auto &meta_pipeline : active_meta_pipelines) {
		vector<shared_ptr<Pipeline>> pipelines;
		meta_pipeline->GetPipelines(pipelines, false);
		for (auto &pipeline : pipelines) {
			auto sink = pipeline->GetSink();
			if (sink.get() != this) {
				pipeline->ResetSinkForReschedule();
			}
		}
	}

	ConfigureInvariantRecursiveBuildReuse(*this, can_cache_invariant_meta_pipelines);

	if (!allow_reuse) {
		gstate.ClearCachedExecutors();
	}

	auto inline_execution = allow_reuse && GetRecursiveThreadLimit(gstate) == 1;

	if (inline_execution) {
		auto &inline_plan =
		    gstate.invariant_meta_pipelines_materialized ? gstate.invariant_inline_plan : gstate.inline_plan;
		if (!inline_plan) {
			inline_plan = BuildRecursiveInlinePlan(active_meta_pipelines);
		}
		ExecuteRecursiveInlinePlan(gstate, executor, *inline_plan);
		if (can_cache_invariant_meta_pipelines) {
			gstate.invariant_meta_pipelines_materialized = true;
		}
		return;
	}

	vector<shared_ptr<Event>> events;
	ScheduleRecursivePipelines(active_meta_pipelines, gstate, executor, events);
	WaitForRecursiveEvents(executor, events);
	if (can_cache_invariant_meta_pipelines) {
		gstate.invariant_meta_pipelines_materialized = true;
	}
}

//===--------------------------------------------------------------------===//
// Pipeline Construction
//===--------------------------------------------------------------------===//

static void GatherColumnDataScans(const PhysicalOperator &op, vector<const_reference<PhysicalOperator>> &delim_scans) {
	if (op.type == PhysicalOperatorType::DELIM_SCAN || op.type == PhysicalOperatorType::CTE_SCAN) {
		delim_scans.push_back(op);
	}
	for (auto child : op.GetChildren()) {
		GatherColumnDataScans(child.get(), delim_scans);
	}
}

static void GatherRecursiveScansInternal(PhysicalOperator &op, TableIndex cte_index,
                                         vector<reference<PhysicalColumnDataScan>> &recursive_scans,
                                         reference_set_t<const PhysicalOperator> &visited) {
	if (!visited.insert(op).second) {
		return;
	}
	if (op.type == PhysicalOperatorType::RECURSIVE_CTE_SCAN) {
		auto &scan = op.Cast<PhysicalColumnDataScan>();
		if (scan.cte_index == cte_index) {
			recursive_scans.push_back(scan);
		}
	}
	for (auto &child : op.children) {
		GatherRecursiveScansInternal(child.get(), cte_index, recursive_scans, visited);
	}
	if (op.type == PhysicalOperatorType::LEFT_DELIM_JOIN || op.type == PhysicalOperatorType::RIGHT_DELIM_JOIN) {
		auto &delim_join = op.Cast<PhysicalDelimJoin>();
		GatherRecursiveScansInternal(delim_join.join, cte_index, recursive_scans, visited);
		GatherRecursiveScansInternal(delim_join.distinct, cte_index, recursive_scans, visited);
	}
}

static void GatherRecursiveScans(PhysicalOperator &op, TableIndex cte_index,
                                 vector<reference<PhysicalColumnDataScan>> &recursive_scans) {
	reference_set_t<const PhysicalOperator> visited;
	GatherRecursiveScansInternal(op, cte_index, recursive_scans, visited);
}

static void CountRecursiveReferencesInternal(const PhysicalOperator &op, TableIndex cte_index,
                                             idx_t &recursive_reference_count, idx_t &recurring_reference_count,
                                             reference_set_t<const PhysicalOperator> &visited) {
	if (!visited.insert(op).second) {
		return;
	}
	if (op.type == PhysicalOperatorType::RECURSIVE_CTE_SCAN ||
	    op.type == PhysicalOperatorType::RECURSIVE_RECURRING_CTE_SCAN) {
		auto &scan = op.Cast<PhysicalColumnDataScan>();
		if (scan.cte_index == cte_index) {
			if (op.type == PhysicalOperatorType::RECURSIVE_CTE_SCAN) {
				recursive_reference_count++;
			} else {
				recurring_reference_count++;
			}
		}
	}
	for (auto child : op.GetChildren()) {
		CountRecursiveReferencesInternal(child.get(), cte_index, recursive_reference_count, recurring_reference_count,
		                                 visited);
	}
}

static void CountRecursiveReferences(const PhysicalOperator &op, TableIndex cte_index, idx_t &recursive_reference_count,
                                     idx_t &recurring_reference_count) {
	reference_set_t<const PhysicalOperator> visited;
	CountRecursiveReferencesInternal(op, cte_index, recursive_reference_count, recurring_reference_count, visited);
}

void PhysicalRecursiveCTE::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	op_state.reset();
	sink_state.reset();
	recursive_meta_pipeline.reset();
	{
		D_ASSERT(shared_executor_pool);
		lock_guard<mutex> guard(shared_executor_pool->lock);
		shared_executor_pool->executors.clear();
	}
	recursive_reference_count = 0;
	recurring_reference_count = 0;
	recursive_scans.clear();
	invariant_meta_pipelines.clear();

	auto &state = meta_pipeline.GetState();
	state.SetPipelineSource(current, *this);

	auto &executor = meta_pipeline.GetExecutor();
	executor.AddRecursiveCTE(*this);

	// the LHS of the recursive CTE is our initial state
	auto &initial_state_pipeline = meta_pipeline.CreateChildMetaPipeline(current, *this);
	initial_state_pipeline.Build(children[0]);

	// the RHS is the recursive pipeline
	recursive_meta_pipeline = make_shared_ptr<MetaPipeline>(executor, state, this);
	recursive_meta_pipeline->SetRecursiveCTE();
	recursive_meta_pipeline->Build(children[1]);
	CountRecursiveReferences(children[1], table_index, recursive_reference_count, recurring_reference_count);
	GatherRecursiveScans(children[1], table_index, recursive_scans);

	vector<const_reference<PhysicalOperator>> ops;
	GatherColumnDataScans(children[1], ops);
	bool has_delim_scan = false;
	for (auto op : ops) {
		if (op.get().type == PhysicalOperatorType::DELIM_SCAN) {
			has_delim_scan = true;
			break;
		}
	}
	if (!has_delim_scan) {
		vector<shared_ptr<MetaPipeline>> recursive_meta_pipelines;
		recursive_meta_pipeline->GetMetaPipelines(recursive_meta_pipelines, true, false);
		invariant_meta_pipelines = FindInvariantRecursiveMetaPipelines(recursive_meta_pipelines, table_index);
	}

	for (auto op : ops) {
		auto entry = state.cte_dependencies.find(op);
		if (entry == state.cte_dependencies.end()) {
			continue;
		}
		// this chunk scan introduces a dependency to the current pipeline
		// namely a dependency on the CTE pipeline to finish
		auto cte_dependency = entry->second.get().shared_from_this();
		current.AddDependency(cte_dependency);
	}
}

vector<const_reference<PhysicalOperator>> PhysicalRecursiveCTE::GetSources() const {
	return {*this};
}

InsertionOrderPreservingMap<string> PhysicalRecursiveCTE::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["CTE Name"] = ctename;
	result["Table Index"] = StringUtil::Format("%llu", table_index.index);
	SetEstimatedCardinality(result, estimated_cardinality);
	return result;
}

} // namespace duckdb
