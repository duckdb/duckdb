#include "duckdb/execution/executor.hpp"

#include "duckdb/execution/execution_context.hpp"
#include "duckdb/execution/operator/helper/physical_result_collector.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/execution/operator/set/physical_cte.hpp"
#include "duckdb/execution/operator/set/physical_recursive_cte.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/pipeline_complete_event.hpp"
#include "duckdb/parallel/pipeline_event.hpp"
#include "duckdb/parallel/pipeline_executor.hpp"
#include "duckdb/parallel/pipeline_finish_event.hpp"
#include "duckdb/parallel/pipeline_initialize_event.hpp"
#include "duckdb/parallel/pipeline_prepare_finish_event.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/parallel/thread_context.hpp"

#include <algorithm>
#include <chrono>

namespace duckdb {

Executor::Executor(ClientContext &context) : context(context), executor_tasks(0), blocked_thread_time(0) {
}

Executor::~Executor() {
	D_ASSERT(Exception::UncaughtException() || executor_tasks == 0);
}

Executor &Executor::Get(ClientContext &context) {
	return context.GetExecutor();
}

void Executor::AddEvent(shared_ptr<Event> event) {
	lock_guard<mutex> elock(executor_lock);
	if (cancelled) {
		return;
	}
	events.push_back(std::move(event));
}

struct PipelineEventStack {
	PipelineEventStack(Event &pipeline_initialize_event, Event &pipeline_event, Event &pipeline_prepare_finish_event,
	                   Event &pipeline_finish_event, Event &pipeline_complete_event)
	    : pipeline_initialize_event(pipeline_initialize_event), pipeline_event(pipeline_event),
	      pipeline_prepare_finish_event(pipeline_prepare_finish_event), pipeline_finish_event(pipeline_finish_event),
	      pipeline_complete_event(pipeline_complete_event) {
	}

	Event &pipeline_initialize_event;
	Event &pipeline_event;
	Event &pipeline_prepare_finish_event;
	Event &pipeline_finish_event;
	Event &pipeline_complete_event;
};

using event_map_t = reference_map_t<Pipeline, PipelineEventStack>;

struct ScheduleEventData {
	ScheduleEventData(const vector<shared_ptr<MetaPipeline>> &meta_pipelines, vector<shared_ptr<Event>> &events,
	                  bool initial_schedule)
	    : meta_pipelines(meta_pipelines), events(events), initial_schedule(initial_schedule) {
	}

	const vector<shared_ptr<MetaPipeline>> &meta_pipelines;
	vector<shared_ptr<Event>> &events;
	bool initial_schedule;
	event_map_t event_map;
};

void Executor::SchedulePipeline(const shared_ptr<MetaPipeline> &meta_pipeline, ScheduleEventData &event_data) {
	D_ASSERT(meta_pipeline);
	auto &events = event_data.events;
	auto &event_map = event_data.event_map;

	// create events/stack for the base pipeline
	auto base_pipeline = meta_pipeline->GetBasePipeline();
	auto base_initialize_event = make_shared_ptr<PipelineInitializeEvent>(base_pipeline);
	auto base_event = make_shared_ptr<PipelineEvent>(base_pipeline);
	auto base_prepare_finish_event = make_shared_ptr<PipelinePrepareFinishEvent>(base_pipeline);
	auto base_finish_event = make_shared_ptr<PipelineFinishEvent>(base_pipeline);
	auto base_complete_event =
	    make_shared_ptr<PipelineCompleteEvent>(base_pipeline->executor, event_data.initial_schedule);
	PipelineEventStack base_stack(*base_initialize_event, *base_event, *base_prepare_finish_event, *base_finish_event,
	                              *base_complete_event);
	events.push_back(std::move(base_initialize_event));
	events.push_back(std::move(base_event));
	events.push_back(std::move(base_prepare_finish_event));
	events.push_back(std::move(base_finish_event));
	events.push_back(std::move(base_complete_event));

	// dependencies: initialize -> event -> prepare finish -> finish -> complete
	base_stack.pipeline_event.AddDependency(base_stack.pipeline_initialize_event);
	base_stack.pipeline_prepare_finish_event.AddDependency(base_stack.pipeline_event);
	base_stack.pipeline_finish_event.AddDependency(base_stack.pipeline_prepare_finish_event);
	base_stack.pipeline_complete_event.AddDependency(base_stack.pipeline_finish_event);

	// create an event and stack for all pipelines in the MetaPipeline
	vector<shared_ptr<Pipeline>> pipelines;
	meta_pipeline->GetPipelines(pipelines, false);
	for (idx_t i = 1; i < pipelines.size(); i++) { // loop starts at 1 because 0 is the base pipeline
		auto &pipeline = pipelines[i];
		D_ASSERT(pipeline);

		// create events/stack for this pipeline
		auto pipeline_event = make_shared_ptr<PipelineEvent>(pipeline);

		auto finish_group = meta_pipeline->GetFinishGroup(*pipeline);
		if (finish_group) {
			// this pipeline is part of a finish group
			const auto group_entry = event_map.find(*finish_group.get());
			D_ASSERT(group_entry != event_map.end());
			auto &group_stack = group_entry->second;
			PipelineEventStack pipeline_stack(base_stack.pipeline_initialize_event, *pipeline_event,
			                                  group_stack.pipeline_prepare_finish_event,
			                                  group_stack.pipeline_finish_event, base_stack.pipeline_complete_event);

			// dependencies: base_finish -> pipeline_event -> group_prepare_finish
			pipeline_stack.pipeline_event.AddDependency(base_stack.pipeline_finish_event);
			group_stack.pipeline_prepare_finish_event.AddDependency(pipeline_stack.pipeline_event);

			// add pipeline stack to event map
			event_map.insert(make_pair(reference<Pipeline>(*pipeline), pipeline_stack));
		} else if (meta_pipeline->HasFinishEvent(*pipeline)) {
			// this pipeline has its own finish event (despite going into the same sink - Finalize twice!)
			auto pipeline_prepare_finish_event = make_shared_ptr<PipelinePrepareFinishEvent>(pipeline);
			auto pipeline_finish_event = make_shared_ptr<PipelineFinishEvent>(pipeline);
			PipelineEventStack pipeline_stack(base_stack.pipeline_initialize_event, *pipeline_event,
			                                  *pipeline_prepare_finish_event, *pipeline_finish_event,
			                                  base_stack.pipeline_complete_event);
			events.push_back(std::move(pipeline_prepare_finish_event));
			events.push_back(std::move(pipeline_finish_event));

			// dependencies:
			// base_finish -> pipeline_event -> pipeline_prepare_finish -> pipeline_finish -> base_complete
			pipeline_stack.pipeline_event.AddDependency(base_stack.pipeline_finish_event);
			pipeline_stack.pipeline_prepare_finish_event.AddDependency(pipeline_stack.pipeline_event);
			pipeline_stack.pipeline_finish_event.AddDependency(pipeline_stack.pipeline_prepare_finish_event);
			base_stack.pipeline_complete_event.AddDependency(pipeline_stack.pipeline_finish_event);

			// add pipeline stack to event map
			event_map.insert(make_pair(reference<Pipeline>(*pipeline), pipeline_stack));
		} else {
			// no additional finish event
			PipelineEventStack pipeline_stack(base_stack.pipeline_initialize_event, *pipeline_event,
			                                  base_stack.pipeline_prepare_finish_event,
			                                  base_stack.pipeline_finish_event, base_stack.pipeline_complete_event);

			// dependencies: base_initialize -> pipeline_event -> base_prepare_finish
			pipeline_stack.pipeline_event.AddDependency(base_stack.pipeline_initialize_event);
			base_stack.pipeline_prepare_finish_event.AddDependency(pipeline_stack.pipeline_event);

			// add pipeline stack to event map
			event_map.insert(make_pair(reference<Pipeline>(*pipeline), pipeline_stack));
		}
		events.push_back(std::move(pipeline_event));
	}

	// add base stack to the event data too
	event_map.insert(make_pair(reference<Pipeline>(*base_pipeline), base_stack));

	for (auto &pipeline : pipelines) {
		auto source = pipeline->GetSource();
		if (source->type == PhysicalOperatorType::TABLE_SCAN) {
			auto &table_function = source->Cast<PhysicalTableScan>();
			if (table_function.function.global_initialization == TableFunctionInitialization::INITIALIZE_ON_SCHEDULE) {
				// certain functions have to be eagerly initialized during scheduling
				// if that is the case - initialize the function here
				pipeline->ResetSource(true);
			}
		}
	}
}

void Executor::ScheduleEventsInternal(ScheduleEventData &event_data) {
	auto &events = event_data.events;
	D_ASSERT(events.empty());

	// create all the required pipeline events
	for (auto &meta_pipeline : event_data.meta_pipelines) {
		SchedulePipeline(meta_pipeline, event_data);
	}

	// set up the dependencies for complete event
	auto &event_map = event_data.event_map;
	for (auto &entry : event_map) {
		auto &pipeline = entry.first.get();
		for (auto &dependency : pipeline.dependencies) {
			auto dep = dependency.lock();
			D_ASSERT(dep);
			auto event_map_entry = event_map.find(*dep);
			if (event_map_entry == event_map.end()) {
				continue;
			}
			D_ASSERT(event_map_entry != event_map.end());
			auto &dep_entry = event_map_entry->second;
			entry.second.pipeline_event.AddDependency(dep_entry.pipeline_complete_event);
		}
	}

	// set the dependencies for pipeline event
	for (auto &meta_pipeline : event_data.meta_pipelines) {
		for (auto &entry : meta_pipeline->GetDependencies()) {
			auto &pipeline = entry.first.get();
			auto root_entry = event_map.find(pipeline);
			D_ASSERT(root_entry != event_map.end());
			auto &pipeline_stack = root_entry->second;
			for (auto &dependency : entry.second) {
				auto event_entry = event_map.find(dependency);
				D_ASSERT(event_entry != event_map.end());
				auto &dependency_stack = event_entry->second;
				pipeline_stack.pipeline_event.AddDependency(dependency_stack.pipeline_event);
			}
		}
	}

	// these dependencies make it so that things happen in this order:
	// 1. all join build child pipelines run until Combine
	// 2. all join build child pipeline PrepareFinalize
	// 3. all join build child pipelines Finalize
	// operators communicate their memory usage through the TemporaryMemoryManger (TMM) in PrepareFinalize
	// then, when the child pipelines Finalize, all required memory is known, and TMM can make an informed decision
	for (auto &meta_pipeline : event_data.meta_pipelines) {
		vector<shared_ptr<MetaPipeline>> children;
		meta_pipeline->GetMetaPipelines(children, false, true);
		for (auto &child1 : children) {
			if (child1->Type() != MetaPipelineType::JOIN_BUILD) {
				continue; // We only want to do this for join builds
			}
			auto &child1_base = *child1->GetBasePipeline();
			auto child1_entry = event_map.find(child1_base);
			D_ASSERT(child1_entry != event_map.end());

			for (auto &child2 : children) {
				if (child2->Type() != MetaPipelineType::JOIN_BUILD || RefersToSameObject(*child1, *child2)) {
					continue; // We don't want to depend on itself
				}
				if (!RefersToSameObject(*child1->GetParent(), *child2->GetParent())) {
					continue; // Different parents, skip
				}

				auto &child2_base = *child2->GetBasePipeline();
				auto child2_entry = event_map.find(child2_base);
				D_ASSERT(child2_entry != event_map.end());

				// all children PrepareFinalize must wait until all Combine
				child1_entry->second.pipeline_prepare_finish_event.AddDependency(child2_entry->second.pipeline_event);
				// all children Finalize must wait until all PrepareFinalize
				child1_entry->second.pipeline_finish_event.AddDependency(
				    child2_entry->second.pipeline_prepare_finish_event);
			}
		}
	}

	// verify that we have no cyclic dependencies
	VerifyScheduledEvents(event_data);

	// schedule the pipelines that do not have dependencies
	for (auto &event : events) {
		if (!event->HasDependencies()) {
			event->Schedule();
		}
	}
}

void Executor::ScheduleEvents(const vector<shared_ptr<MetaPipeline>> &meta_pipelines) {
	ScheduleEventData event_data(meta_pipelines, events, true);
	ScheduleEventsInternal(event_data);
}

void Executor::VerifyScheduledEvents(const ScheduleEventData &event_data) {
#ifdef DEBUG
	const idx_t count = event_data.events.size();
	vector<reference<Event>> vertices;
	vertices.reserve(count);
	for (const auto &event : event_data.events) {
		vertices.push_back(*event);
	}
	vector<bool> visited(count, false);
	vector<bool> recursion_stack(count, false);
	for (idx_t i = 0; i < count; i++) {
		VerifyScheduledEventsInternal(i, vertices, visited, recursion_stack);
	}
#endif
}

void Executor::VerifyScheduledEventsInternal(const idx_t vertex, const vector<reference<Event>> &vertices,
                                             vector<bool> &visited, vector<bool> &recursion_stack) {
	D_ASSERT(!recursion_stack[vertex]); // this vertex is in the recursion stack: circular dependency!
	if (visited[vertex]) {
		return; // early out: we already visited this vertex
	}

	auto &parents = vertices[vertex].get().GetParentsVerification();
	if (parents.empty()) {
		return; // early out: outgoing edges
	}

	// create a vector the indices of the adjacent events
	vector<idx_t> adjacent;
	const idx_t count = vertices.size();
	for (auto parent : parents) {
		idx_t i;
		for (i = 0; i < count; i++) {
			if (RefersToSameObject(vertices[i], parent)) {
				adjacent.push_back(i);
				break;
			}
		}
		D_ASSERT(i != count); // dependency must be in there somewhere
	}

	// mark vertex as visited and add to recursion stack
	visited[vertex] = true;
	recursion_stack[vertex] = true;

	// recurse into adjacent vertices
	for (const auto &i : adjacent) {
		VerifyScheduledEventsInternal(i, vertices, visited, recursion_stack);
	}

	// remove vertex from recursion stack
	recursion_stack[vertex] = false;
}

void Executor::AddRecursiveCTE(PhysicalOperator &rec_cte) {
	recursive_ctes.push_back(rec_cte);
}

void Executor::ReschedulePipelines(const vector<shared_ptr<MetaPipeline>> &pipelines_p,
                                   vector<shared_ptr<Event>> &events_p) {
	ScheduleEventData event_data(pipelines_p, events_p, false);
	ScheduleEventsInternal(event_data);
}

bool Executor::NextExecutor() {
	if (root_pipeline_idx >= root_pipelines.size()) {
		return false;
	}
	root_pipelines[root_pipeline_idx]->Reset();
	root_executor = make_uniq<PipelineExecutor>(context, *root_pipelines[root_pipeline_idx]);
	root_pipeline_idx++;
	return true;
}

void Executor::VerifyPipeline(Pipeline &pipeline) {
	D_ASSERT(!pipeline.ToString().empty());
	auto operators = pipeline.GetOperators();
	for (auto &other_pipeline : pipelines) {
		auto other_operators = other_pipeline->GetOperators();
		for (idx_t op_idx = 0; op_idx < operators.size(); op_idx++) {
			for (idx_t other_idx = 0; other_idx < other_operators.size(); other_idx++) {
				auto &left = operators[op_idx].get();
				auto &right = other_operators[other_idx].get();
				if (left.Equals(right)) {
					D_ASSERT(right.Equals(left));
				} else {
					D_ASSERT(!right.Equals(left));
				}
			}
		}
	}
}

void Executor::VerifyPipelines() {
#ifdef DEBUG
	for (auto &pipeline : pipelines) {
		VerifyPipeline(*pipeline);
	}
#endif
}

void Executor::Initialize(unique_ptr<PhysicalOperator> physical_plan_p) {
	Reset();
	owned_plan = std::move(physical_plan_p);
	InitializeInternal(*owned_plan);
}

void Executor::Initialize(PhysicalOperator &plan) {
	Reset();
	InitializeInternal(plan);
}

void Executor::InitializeInternal(PhysicalOperator &plan) {

	auto &scheduler = TaskScheduler::GetScheduler(context);
	{
		lock_guard<mutex> elock(executor_lock);
		physical_plan = &plan;

		this->profiler = ClientData::Get(context).profiler;
		profiler->Initialize(plan);
		this->producer = scheduler.CreateProducer();

		// build and ready the pipelines
		PipelineBuildState state;
		auto root_pipeline = make_shared_ptr<MetaPipeline>(*this, state, nullptr);
		root_pipeline->Build(*physical_plan);
		root_pipeline->Ready();

		// ready recursive cte pipelines too
		for (auto &rec_cte_ref : recursive_ctes) {
			auto &rec_cte = rec_cte_ref.get().Cast<PhysicalRecursiveCTE>();
			rec_cte.recursive_meta_pipeline->Ready();
		}

		// set root pipelines, i.e., all pipelines that end in the final sink
		root_pipeline->GetPipelines(root_pipelines, false);
		root_pipeline_idx = 0;

		// collect all meta-pipelines from the root pipeline
		vector<shared_ptr<MetaPipeline>> to_schedule;
		root_pipeline->GetMetaPipelines(to_schedule, true, true);

		// number of 'PipelineCompleteEvent's is equal to the number of meta pipelines, so we have to set it here
		total_pipelines = to_schedule.size();

		// collect all pipelines from the root pipelines (recursively) for the progress bar and verify them
		root_pipeline->GetPipelines(pipelines, true);

		// finally, verify and schedule
		VerifyPipelines();
		ScheduleEvents(to_schedule);
	}
}

void Executor::CancelTasks() {
	task.reset();

	{
		lock_guard<mutex> elock(executor_lock);
		// mark the query as cancelled so tasks will early-out
		cancelled = true;
		// destroy all pipelines, events and states
		for (auto &rec_cte_ref : recursive_ctes) {
			auto &rec_cte = rec_cte_ref.get().Cast<PhysicalRecursiveCTE>();
			rec_cte.recursive_meta_pipeline.reset();
		}
		pipelines.clear();
		root_pipelines.clear();
		to_be_rescheduled_tasks.clear();
		events.clear();
	}
	// Take all pending tasks and execute them until they cancel
	while (executor_tasks > 0) {
		WorkOnTasks();
	}
}

void Executor::WorkOnTasks() {
	auto &scheduler = TaskScheduler::GetScheduler(context);

	shared_ptr<Task> task_from_producer;
	while (scheduler.GetTaskFromProducer(*producer, task_from_producer)) {
		auto res = task_from_producer->Execute(TaskExecutionMode::PROCESS_ALL);
		if (res == TaskExecutionResult::TASK_BLOCKED) {
			task_from_producer->Deschedule();
		}
		task_from_producer.reset();
	}
}

void Executor::SignalTaskRescheduled(lock_guard<mutex> &) {
	task_reschedule.notify_one();
}

void Executor::WaitForTask() {
#ifndef DUCKDB_NO_THREADS
	static constexpr std::chrono::milliseconds WAIT_TIME_MS = std::chrono::milliseconds(WAIT_TIME);
	std::unique_lock<mutex> l(executor_lock);
	if (to_be_rescheduled_tasks.empty()) {
		return;
	}
	if (ResultCollectorIsBlocked()) {
		// If the result collector is blocked, it won't get unblocked until the connection calls Fetch
		return;
	}

	blocked_thread_time++;
	task_reschedule.wait_for(l, WAIT_TIME_MS);
#endif
}

void Executor::RescheduleTask(shared_ptr<Task> &task_p) {
	// This function will spin lock until the task provided is added to the to_be_rescheduled_tasks
	while (true) {
		lock_guard<mutex> l(executor_lock);
		if (cancelled) {
			return;
		}
		auto entry = to_be_rescheduled_tasks.find(task_p.get());
		if (entry != to_be_rescheduled_tasks.end()) {
			auto &scheduler = TaskScheduler::GetScheduler(context);
			to_be_rescheduled_tasks.erase(task_p.get());
			scheduler.ScheduleTask(GetToken(), task_p);
			SignalTaskRescheduled(l);
			break;
		}
	}
}

bool Executor::ResultCollectorIsBlocked() {
	if (!HasStreamingResultCollector()) {
		return false;
	}
	if (completed_pipelines + 1 != total_pipelines) {
		// The result collector is always in the last pipeline
		return false;
	}
	if (to_be_rescheduled_tasks.empty()) {
		return false;
	}
	for (auto &kv : to_be_rescheduled_tasks) {
		auto &task = kv.second;
		if (task->TaskBlockedOnResult()) {
			// At least one of the blocked tasks is connected to a result collector
			// This task could be the only task that could unblock the other non-result-collector tasks
			// To prevent a scenario where we halt indefinitely, we return here so it can be unblocked by a call to
			// Fetch
			return true;
		}
	}
	return false;
}

void Executor::AddToBeRescheduled(shared_ptr<Task> &task_p) {
	lock_guard<mutex> l(executor_lock);
	if (cancelled) {
		return;
	}
	if (to_be_rescheduled_tasks.find(task_p.get()) != to_be_rescheduled_tasks.end()) {
		return;
	}
	to_be_rescheduled_tasks[task_p.get()] = std::move(task_p);
}

bool Executor::ExecutionIsFinished() {
	return completed_pipelines >= total_pipelines || HasError();
}

PendingExecutionResult Executor::ExecuteTask(bool dry_run) {
	// Only executor should return NO_TASKS_AVAILABLE
	D_ASSERT(execution_result != PendingExecutionResult::NO_TASKS_AVAILABLE);
	if (execution_result != PendingExecutionResult::RESULT_NOT_READY && ExecutionIsFinished()) {
		return execution_result;
	}
	// check if there are any incomplete pipelines
	auto &scheduler = TaskScheduler::GetScheduler(context);
	if (completed_pipelines < total_pipelines) {
		// there are! if we don't already have a task, fetch one
		auto current_task = task.get();
		if (dry_run) {
			// Pretend we have no task, we don't want to execute anything
			current_task = nullptr;
		} else {
			if (!task) {
				scheduler.GetTaskFromProducer(*producer, task);
			}
			current_task = task.get();
		}

		if (!current_task && !HasError()) {
			// there are no tasks to be scheduled and there are tasks blocked
			lock_guard<mutex> l(executor_lock);
			if (to_be_rescheduled_tasks.empty()) {
				return PendingExecutionResult::NO_TASKS_AVAILABLE;
			}
			// At least one task is blocked
			if (ResultCollectorIsBlocked()) {
				return PendingExecutionResult::RESULT_READY;
			}
			return PendingExecutionResult::BLOCKED;
		}

		if (current_task) {
			// if we have a task, partially process it
			auto result = task->Execute(TaskExecutionMode::PROCESS_PARTIAL);
			if (result == TaskExecutionResult::TASK_BLOCKED) {
				task->Deschedule();
				task.reset();
			} else if (result == TaskExecutionResult::TASK_FINISHED) {
				// if the task is finished, clean it up
				task.reset();
			}
		}
		if (!HasError()) {
			// we (partially) processed a task and no exceptions were thrown
			// give back control to the caller
			return PendingExecutionResult::RESULT_NOT_READY;
		}
		execution_result = PendingExecutionResult::EXECUTION_ERROR;

		// an exception has occurred executing one of the pipelines
		// we need to cancel all tasks associated with this executor
		CancelTasks();
		ThrowException();
	}
	D_ASSERT(!task);

	lock_guard<mutex> elock(executor_lock);
	pipelines.clear();
	NextExecutor();
	if (HasError()) { // LCOV_EXCL_START
		// an exception has occurred executing one of the pipelines
		execution_result = PendingExecutionResult::EXECUTION_ERROR;
		ThrowException();
	} // LCOV_EXCL_STOP
	execution_result = PendingExecutionResult::EXECUTION_FINISHED;
	return execution_result;
}

void Executor::Reset() {
	lock_guard<mutex> elock(executor_lock);
	physical_plan = nullptr;
	cancelled = false;
	owned_plan.reset();
	root_executor.reset();
	root_pipelines.clear();
	root_pipeline_idx = 0;
	completed_pipelines = 0;
	total_pipelines = 0;
	error_manager.Reset();
	pipelines.clear();
	events.clear();
	to_be_rescheduled_tasks.clear();
	execution_result = PendingExecutionResult::RESULT_NOT_READY;
}

shared_ptr<Pipeline> Executor::CreateChildPipeline(Pipeline &current, PhysicalOperator &op) {
	D_ASSERT(!current.operators.empty());
	D_ASSERT(op.IsSource());
	// found another operator that is a source, schedule a child pipeline
	// 'op' is the source, and the sink is the same
	auto child_pipeline = make_shared_ptr<Pipeline>(*this);
	child_pipeline->sink = current.sink;
	child_pipeline->source = &op;

	// the child pipeline has the same operators up until 'op'
	for (auto current_op : current.operators) {
		if (&current_op.get() == &op) {
			break;
		}
		child_pipeline->operators.push_back(current_op);
	}

	return child_pipeline;
}

vector<LogicalType> Executor::GetTypes() {
	D_ASSERT(physical_plan);
	return physical_plan->GetTypes();
}

void Executor::PushError(ErrorData exception) {
	// push the exception onto the stack
	error_manager.PushError(std::move(exception));
	// interrupt execution of any other pipelines that belong to this executor
	context.interrupted = true;
}

bool Executor::HasError() {
	return error_manager.HasError();
}

ErrorData Executor::GetError() {
	return error_manager.GetError();
}

void Executor::ThrowException() {
	error_manager.ThrowException();
}

void Executor::Flush(ThreadContext &thread_context) {
	static constexpr std::chrono::milliseconds WAIT_TIME_MS = std::chrono::milliseconds(WAIT_TIME);
	auto global_profiler = profiler;
	if (global_profiler) {
		global_profiler->Flush(thread_context.profiler);

		auto blocked_time = blocked_thread_time.load();
		global_profiler->SetInfo(double(blocked_time * WAIT_TIME_MS.count()) / 1000);
	}
}

idx_t Executor::GetPipelinesProgress(ProgressData &progress) { // LCOV_EXCL_START
	lock_guard<mutex> elock(executor_lock);

	progress.done = 0;
	progress.total = 0;
	idx_t count_invalid = 0;
	for (auto &pipeline : pipelines) {
		ProgressData p;
		if (!pipeline->GetProgress(p)) {
			count_invalid++;
		} else {
			progress.Add(p);
		}
	}
	return count_invalid;
} // LCOV_EXCL_STOP

bool Executor::HasResultCollector() {
	return physical_plan->type == PhysicalOperatorType::RESULT_COLLECTOR;
}

bool Executor::HasStreamingResultCollector() {
	if (!HasResultCollector()) {
		return false;
	}
	auto &result_collector = physical_plan->Cast<PhysicalResultCollector>();
	return result_collector.IsStreaming();
}

unique_ptr<QueryResult> Executor::GetResult() {
	D_ASSERT(HasResultCollector());
	auto &result_collector = physical_plan->Cast<PhysicalResultCollector>();
	D_ASSERT(result_collector.sink_state);
	return result_collector.GetResult(*result_collector.sink_state);
}

} // namespace duckdb
