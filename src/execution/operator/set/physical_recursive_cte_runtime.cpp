#include "duckdb/execution/operator/set/physical_recursive_cte_state.hpp"

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
#include "duckdb/parallel/pipeline_complete_event.hpp"
#include "duckdb/parallel/pipeline_executor.hpp"
#include "duckdb/parallel/pipeline_finish_event.hpp"
#include "duckdb/parallel/pipeline_prepare_finish_event.hpp"
#include "duckdb/parallel/task_scheduler.hpp"

#include "duckdb/main/settings.hpp"
#include "duckdb/common/atomic.hpp"

namespace duckdb {

struct RecursiveCTEInlineStageStack {
	RecursiveCTEInlineStageStack(idx_t execute_stage_p, idx_t prepare_finish_stage_p, idx_t finish_stage_p)
	    : execute_stage(execute_stage_p), prepare_finish_stage(prepare_finish_stage_p), finish_stage(finish_stage_p) {
	}

	idx_t execute_stage;
	idx_t prepare_finish_stage;
	idx_t finish_stage;
};

using recursive_cte_inline_stage_map_t = reference_map_t<Pipeline, RecursiveCTEInlineStageStack>;

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

static idx_t GetRecursiveWorkUnits(const RecursiveCTEState &state) {
	idx_t work_units = 0;
	if (state.op.working_table && state.op.recursive_reference_count > 0) {
		work_units += state.CurrentInputTable().ChunkCount() * state.op.recursive_reference_count;
	}
	if (state.op.recurring_table && state.op.recurring_reference_count > 0) {
		const auto recurring_chunks = state.op.using_key
		                                  ? (state.ht->Count() + STANDARD_VECTOR_SIZE - 1) / STANDARD_VECTOR_SIZE
		                                  : state.op.recurring_table->ChunkCount();
		work_units += recurring_chunks * state.op.recurring_reference_count;
	}
	return MaxValue<idx_t>(work_units, 1);
}

static idx_t GetRecursiveFrontierRows(const RecursiveCTEState &state) {
	idx_t recursive_rows = 0;
	if (state.op.working_table && state.op.recursive_reference_count > 0) {
		recursive_rows += state.CurrentInputTable().Count() * state.op.recursive_reference_count;
	}
	if (state.op.recurring_table && state.op.recurring_reference_count > 0) {
		const auto recurring_rows = state.op.using_key ? state.ht->Count() : state.op.recurring_table->Count();
		recursive_rows += recurring_rows * state.op.recurring_reference_count;
	}
	return recursive_rows;
}

static idx_t GetRecursiveFrontierStorageBytes(const RecursiveCTEState &state) {
	idx_t recursive_bytes = 0;
	if (state.op.working_table && state.op.recursive_reference_count > 0) {
		recursive_bytes += state.CurrentInputTable().SizeInBytes() * state.op.recursive_reference_count;
	}
	if (state.op.recurring_table && state.op.recurring_reference_count > 0 && !state.op.using_key) {
		recursive_bytes += state.op.recurring_table->SizeInBytes() * state.op.recurring_reference_count;
	}
	return recursive_bytes;
}

static idx_t GetRecursiveThreadLimit(const RecursiveCTEState &state, idx_t work_units, idx_t frontier_rows) {
	const auto row_limit =
	    MaxValue<idx_t>((frontier_rows + RECURSIVE_ROWS_PER_THREAD - 1) / RECURSIVE_ROWS_PER_THREAD, 1);
	const auto configured_threads =
	    TaskScheduler::GetScheduler(state.op.recursive_meta_pipeline->GetExecutor().context).NumberOfThreads();
	return MinValue(MinValue(MinValue(row_limit, work_units), state.recursive_thread_limit), configured_threads);
}

static void UpdateRecursiveThreadLimit(RecursiveCTEState &state, idx_t elapsed_us, idx_t worker_count, idx_t work_units,
                                       idx_t frontier_rows, idx_t frontier_storage_bytes) {
	// Aim for several milliseconds of measured serial work per worker. This deliberately leaves cheap
	// broad epochs inline: chunk/row width alone does not justify scheduler and sink contention.
	static constexpr idx_t TARGET_WORK_PER_THREAD_US = 5000;
	static constexpr idx_t REQUIRED_CANDIDATE_EPOCHS = 2;
	static constexpr double SERIAL_EWMA_ALPHA = 0.25;
	if (state.collect_runtime_metrics) {
		state.cumulative_epoch_count++;
		state.cumulative_elapsed_us += elapsed_us;
		state.cumulative_frontier_rows += frontier_rows;
		state.cumulative_frontier_chunks += work_units;
		state.cumulative_frontier_storage_bytes += frontier_storage_bytes;
	}

	const auto first_serial_measurement = worker_count == 1 && state.serial_cost_per_work_unit_us == 0;
	const auto cost_per_work_unit = static_cast<double>(elapsed_us) / static_cast<double>(work_units);
	if (worker_count == 1) {
		if (state.serial_cost_per_work_unit_us == 0) {
			state.serial_cost_per_work_unit_us = cost_per_work_unit;
		} else {
			state.serial_cost_per_work_unit_us =
			    state.serial_cost_per_work_unit_us * (1 - SERIAL_EWMA_ALPHA) + cost_per_work_unit * SERIAL_EWMA_ALPHA;
		}
	}

	const auto estimated_serial_us =
	    state.serial_cost_per_work_unit_us == 0
	        ? elapsed_us * worker_count
	        : LossyNumericCast<idx_t>(state.serial_cost_per_work_unit_us * static_cast<double>(work_units));
	idx_t candidate = 1;
	if (work_units > 1 && estimated_serial_us >= TARGET_WORK_PER_THREAD_US) {
		const auto configured_threads =
		    TaskScheduler::GetScheduler(state.op.recursive_meta_pipeline->GetExecutor().context).NumberOfThreads();
		candidate = MinValue<idx_t>(
		    MinValue(work_units, configured_threads),
		    MaxValue<idx_t>((estimated_serial_us + TARGET_WORK_PER_THREAD_US - 1) / TARGET_WORK_PER_THREAD_US, 2));
	}
	if (worker_count > 1 && state.serial_cost_per_work_unit_us != 0 && elapsed_us * 10 >= estimated_serial_us * 9) {
		candidate = 1;
	}
	if (first_serial_measurement && state.recursive_thread_limit == 1 && candidate > 1) {
		const auto previous_limit = state.recursive_thread_limit;
		state.recursive_thread_limit = candidate;
		state.recursive_thread_candidate = candidate;
		state.recursive_thread_candidate_votes = 0;
		state.LogThreadLimitChanged(previous_limit, candidate, elapsed_us, work_units, frontier_rows);
		return;
	}

	if (candidate == state.recursive_thread_limit) {
		state.recursive_thread_candidate = candidate;
		state.recursive_thread_candidate_votes = 0;
		return;
	}
	if (candidate != state.recursive_thread_candidate) {
		state.recursive_thread_candidate = candidate;
		state.recursive_thread_candidate_votes = 1;
		return;
	}
	if (++state.recursive_thread_candidate_votes >= REQUIRED_CANDIDATE_EPOCHS) {
		const auto previous_limit = state.recursive_thread_limit;
		state.recursive_thread_limit = candidate;
		state.recursive_thread_candidate_votes = 0;
		state.LogThreadLimitChanged(previous_limit, candidate, elapsed_us, work_units, frontier_rows);
	}
}

static idx_t GetRecursivePipelineMaxThreads(RecursiveCTEState &state, Pipeline &pipeline) {
	auto max_threads = pipeline.GetMaxThreads();
	if (max_threads < 1) {
		max_threads = 1;
	}
	return MinValue(max_threads, state.recursive_epoch_thread_limit);
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
		if (state.collect_runtime_metrics) {
			state.cumulative_task_count.fetch_add(max_threads);
		}
		state.PrepareCachedExecutors(*pipeline, max_threads);
		auto &executors = state.GetCachedExecutors(*pipeline);
		D_ASSERT(executors.size() >= max_threads);

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

static idx_t AddRecursiveInlineStage(RecursiveCTEPipelineSchedulePlan &plan, RecursiveCTEInlineStageType type,
                                     Pipeline &pipeline) {
	plan.stages.emplace_back(type, pipeline);
	return plan.stages.size() - 1;
}

static void AddRecursiveInlineDependency(RecursiveCTEPipelineSchedulePlan &plan, idx_t dependent_stage,
                                         idx_t dependency_stage) {
	auto &dependent = plan.stages[dependent_stage];
	dependent.dependency_count++;
	plan.stages[dependency_stage].dependents.push_back(dependent_stage);
}

static RecursiveCTEInlineStageStack AddRecursiveInlineStageStack(RecursiveCTEPipelineSchedulePlan &plan,
                                                                 Pipeline &pipeline) {
	auto execute = AddRecursiveInlineStage(plan, RecursiveCTEInlineStageType::EXECUTE, pipeline);
	auto prepare_finish = AddRecursiveInlineStage(plan, RecursiveCTEInlineStageType::PREPARE_FINISH, pipeline);
	auto finish = AddRecursiveInlineStage(plan, RecursiveCTEInlineStageType::FINISH, pipeline);
	AddRecursiveInlineDependency(plan, prepare_finish, execute);
	AddRecursiveInlineDependency(plan, finish, prepare_finish);
	return RecursiveCTEInlineStageStack(execute, prepare_finish, finish);
}

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

static unique_ptr<RecursiveCTEPipelineSchedulePlan>
BuildRecursivePipelineSchedulePlan(const vector<shared_ptr<MetaPipeline>> &meta_pipelines) {
	// Build immutable execute -> prepare-finish -> finish topology once. Both the scheduler-free
	// single-thread path and the event path consume this plan, so their dependency semantics cannot drift.
	auto plan = make_uniq<RecursiveCTEPipelineSchedulePlan>();
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

static bool PipelineIsRepeatable(Pipeline &pipeline,
                                 const reference_set_t<const PhysicalOperator> &non_repeatable_operators) {
	auto source = pipeline.GetSource();
	if (source && non_repeatable_operators.find(*source) != non_repeatable_operators.end()) {
		return false;
	}
	for (auto &op : pipeline.GetIntermediateOperators()) {
		if (non_repeatable_operators.find(op) != non_repeatable_operators.end()) {
			return false;
		}
	}
	auto sink = pipeline.GetSink();
	return !sink || non_repeatable_operators.find(*sink) == non_repeatable_operators.end();
}

static reference_set_t<const MetaPipeline>
FindInvariantRecursiveMetaPipelines(const vector<shared_ptr<MetaPipeline>> &meta_pipelines, TableIndex cte_index,
                                    const reference_set_t<const PhysicalOperator> &non_repeatable_operators) {
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
			if (PipelineDirectlyDependsOnRecursiveInput(*pipeline, cte_index) ||
			    !PipelineIsRepeatable(*pipeline, non_repeatable_operators)) {
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

static bool InvariantRecursiveBuildsRemainReusable(const PhysicalRecursiveCTE &op) {
	// Recursive invariant caching may omit these meta-pipelines entirely after the first materialization.
	// That is only safe if every invariant HASH_JOIN actually kept a reusable in-memory build side.
	// External/spilled hash joins run through mutable partition/probe-spill rounds and must therefore
	// stay in the active recursive schedule instead of being treated as "materialized once".
	for (auto &meta_pipeline_ref : op.invariant_meta_pipelines) {
		auto sink = meta_pipeline_ref.get().GetSink();
		if (!sink || sink->type != PhysicalOperatorType::HASH_JOIN) {
			continue;
		}
		if (!sink->Cast<PhysicalHashJoin>().CanPreserveBuildForRecursiveReuse()) {
			return false;
		}
	}
	return true;
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

static void ScheduleRecursivePlan(const RecursiveCTEPipelineSchedulePlan &plan, RecursiveCTEState &state,
                                  vector<shared_ptr<Event>> &events) {
	for (auto &pipeline : plan.initialize_on_schedule_pipelines) {
		pipeline.get().ResetSource(true);
	}

	events.reserve(plan.stages.size());
	for (auto &stage : plan.stages) {
		auto pipeline = stage.pipeline.get().shared_from_this();
		switch (stage.type) {
		case RecursiveCTEInlineStageType::EXECUTE:
			events.push_back(make_shared_ptr<RecursiveCTEPipelineEvent>(std::move(pipeline), state));
			break;
		case RecursiveCTEInlineStageType::PREPARE_FINISH:
			events.push_back(make_shared_ptr<PipelinePrepareFinishEvent>(std::move(pipeline)));
			break;
		case RecursiveCTEInlineStageType::FINISH:
			events.push_back(make_shared_ptr<PipelineFinishEvent>(std::move(pipeline)));
			break;
		default:
			throw InternalException("Unsupported recursive schedule stage");
		}
	}

	for (idx_t stage_idx = 0; stage_idx < plan.stages.size(); stage_idx++) {
		for (auto dependent_stage : plan.stages[stage_idx].dependents) {
			events[dependent_stage]->AddDependency(*events[stage_idx]);
		}
	}

	for (idx_t stage_idx = 0; stage_idx < plan.stages.size(); stage_idx++) {
		auto &event = events[stage_idx];
		if (event->HasDependencies()) {
			continue;
		}
		if (plan.stages[stage_idx].type == RecursiveCTEInlineStageType::EXECUTE) {
			event->Cast<RecursiveCTEPipelineEvent>().PrepareForSchedule();
		}
		event->Schedule();
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
                                       const RecursiveCTEPipelineSchedulePlan &plan) {
	for (auto &pipeline : plan.initialize_on_schedule_pipelines) {
		pipeline.get().ResetSource(true);
	}

	auto &remaining_dependencies = state.remaining_schedule_dependencies;
	remaining_dependencies.clear();
	remaining_dependencies.reserve(plan.stages.size());
	auto &ready_stages = state.ready_schedule_stages;
	ready_stages.clear();
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
			state.PrepareCachedExecutors(pipeline, max_threads);
			auto &executors = state.GetCachedExecutors(pipeline);
			D_ASSERT(executors.size() >= max_threads);
			executors[0]->PrepareForExecution();
			if (state.collect_runtime_metrics) {
				state.cumulative_task_count.fetch_add(1);
			}
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

void PhysicalRecursiveCTE::ExecuteRecursivePipelines(ExecutionContext &context) const {
	if (!recursive_meta_pipeline) {
		throw InternalException("Missing meta pipeline for recursive CTE");
	}
	D_ASSERT(recursive_meta_pipeline->HasRecursiveCTE());

	auto &gstate = sink_state->Cast<RecursiveCTEState>();
	auto &executor = recursive_meta_pipeline->GetExecutor();
	auto allow_reuse = Settings::Get<EnableCachingOperatorsSetting>(context.client);
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

	auto &schedule_plan =
	    gstate.invariant_meta_pipelines_materialized ? gstate.invariant_schedule_plan : gstate.schedule_plan;
	const auto prepare_cached_executor_entries = !allow_reuse || !schedule_plan;
	if (!schedule_plan) {
		schedule_plan = BuildRecursivePipelineSchedulePlan(active_meta_pipelines);
	}

	// Materialize every state-local cache entry before events can run concurrently. Individual
	// pipelines grow their stable executor vector after their source state has been reset. Reusable
	// entries only need to be registered when their immutable schedule plan is first constructed.
	if (prepare_cached_executor_entries) {
		reference_set_t<Pipeline> prepared_pipelines;
		for (auto &meta_pipeline : active_meta_pipelines) {
			vector<shared_ptr<Pipeline>> pipelines;
			meta_pipeline->GetPipelines(pipelines, false);
			for (auto &pipeline : pipelines) {
				if (prepared_pipelines.insert(*pipeline).second) {
					gstate.PrepareCachedExecutorEntry(*pipeline);
				}
			}
		}
	}

	const auto work_units = GetRecursiveWorkUnits(gstate);
	const auto frontier_rows = GetRecursiveFrontierRows(gstate);
	const auto frontier_storage_bytes = gstate.collect_runtime_metrics ? GetRecursiveFrontierStorageBytes(gstate) : 0;
	if (!gstate.recursive_thread_limit_initialized) {
		gstate.recursive_thread_limit_initialized = true;
		if (!using_key && union_all) {
			gstate.recursive_thread_limit =
			    TaskScheduler::GetScheduler(recursive_meta_pipeline->GetExecutor().context).NumberOfThreads();
		}
	}
	const auto worker_count = GetRecursiveThreadLimit(gstate, work_units, frontier_rows);
	gstate.recursive_epoch_thread_limit = worker_count;
	if (worker_count > 1 && !using_key && !union_all) {
		const auto partition_count = MinValue<idx_t>(NextPowerOfTwo(worker_count), 4);
		gstate.PromoteDistinctState(context.client, partition_count);
	}
	const auto epoch_start = std::chrono::steady_clock::now();
	auto inline_execution = allow_reuse && worker_count == 1;

	if (inline_execution) {
		ExecuteRecursiveInlinePlan(gstate, executor, *schedule_plan);
	} else {
		vector<shared_ptr<Event>> events;
		ScheduleRecursivePlan(*schedule_plan, gstate, events);
		WaitForRecursiveEvents(executor, events);
	}

	const auto epoch_end = std::chrono::steady_clock::now();
	const auto elapsed_us =
	    NumericCast<idx_t>(std::chrono::duration_cast<std::chrono::microseconds>(epoch_end - epoch_start).count());
	if (gstate.collect_runtime_metrics) {
		gstate.cumulative_worker_count += worker_count;
	}
	UpdateRecursiveThreadLimit(gstate, elapsed_us, worker_count, work_units, frontier_rows, frontier_storage_bytes);
	if (can_cache_invariant_meta_pipelines && InvariantRecursiveBuildsRemainReusable(*this)) {
		if (gstate.collect_runtime_metrics && !gstate.invariant_meta_pipelines_materialized) {
			gstate.retained_build_executions += invariant_meta_pipelines.size();
		}
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
		invariant_meta_pipelines =
		    FindInvariantRecursiveMetaPipelines(recursive_meta_pipelines, table_index, non_repeatable_operators);
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

} // namespace duckdb
