#include "duckdb/execution/executor.hpp"

#include "duckdb/execution/execution_context.hpp"
#include "duckdb/execution/operator/helper/physical_result_collector.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/parallel/pipeline_complete_event.hpp"
#include "duckdb/parallel/pipeline_event.hpp"
#include "duckdb/parallel/pipeline_executor.hpp"
#include "duckdb/parallel/pipeline_finish_event.hpp"
#include "duckdb/parallel/pipeline_initialize_event.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/parallel/thread_context.hpp"

#include <algorithm>

namespace duckdb {

Executor::Executor(ClientContext &context) : context(context) {
}

Executor::~Executor() {
}

Executor &Executor::Get(ClientContext &context) {
	return context.GetExecutor();
}

void Executor::AddEvent(shared_ptr<Event> event) {
	lock_guard<mutex> elock(executor_lock);
	if (cancelled) {
		return;
	}
	events.push_back(move(event));
}

struct PipelineEventStack {
	Event *pipeline_initialize_event;
	Event *pipeline_event;
	Event *pipeline_finish_event;
	Event *pipeline_complete_event;
};

using event_map_t = unordered_map<const Pipeline *, PipelineEventStack>;

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
	auto base_initialize_event = make_shared<PipelineInitializeEvent>(base_pipeline);
	auto base_event = make_shared<PipelineEvent>(base_pipeline);
	auto base_finish_event = make_shared<PipelineFinishEvent>(base_pipeline);
	auto base_complete_event = make_shared<PipelineCompleteEvent>(base_pipeline->executor, event_data.initial_schedule);
	PipelineEventStack base_stack {base_initialize_event.get(), base_event.get(), base_finish_event.get(),
	                               base_complete_event.get()};
	events.push_back(move(base_initialize_event));
	events.push_back(move(base_event));
	events.push_back(move(base_finish_event));
	events.push_back(move(base_complete_event));

	// dependencies: initialize -> event -> finish -> complete
	base_stack.pipeline_event->AddDependency(*base_stack.pipeline_initialize_event);
	base_stack.pipeline_finish_event->AddDependency(*base_stack.pipeline_event);
	base_stack.pipeline_complete_event->AddDependency(*base_stack.pipeline_finish_event);

	// create an event and stack for all pipelines in the MetaPipeline
	vector<shared_ptr<Pipeline>> pipelines;
	meta_pipeline->GetPipelines(pipelines, false);
	for (idx_t i = 1; i < pipelines.size(); i++) { // loop starts at 1 because 0 is the base pipeline
		auto &pipeline = pipelines[i];
		D_ASSERT(pipeline);

		// create events/stack for this pipeline
		auto pipeline_event = make_shared<PipelineEvent>(pipeline);
		Event *pipeline_finish_event_ptr;
		if (meta_pipeline->HasFinishEvent(pipeline.get())) {
			// this pipeline has its own finish event (despite going into the same sink - Finalize twice!)
			auto pipeline_finish_event = make_unique<PipelineFinishEvent>(pipeline);
			pipeline_finish_event_ptr = pipeline_finish_event.get();
			events.push_back(move(pipeline_finish_event));
			base_stack.pipeline_complete_event->AddDependency(*pipeline_finish_event_ptr);
		} else {
			pipeline_finish_event_ptr = base_stack.pipeline_finish_event;
		}
		PipelineEventStack pipeline_stack {base_stack.pipeline_initialize_event, pipeline_event.get(),
		                                   pipeline_finish_event_ptr, base_stack.pipeline_complete_event};
		events.push_back(move(pipeline_event));

		// dependencies: base_initialize -> pipeline_event -> base_finish
		pipeline_stack.pipeline_event->AddDependency(*base_stack.pipeline_initialize_event);
		pipeline_stack.pipeline_finish_event->AddDependency(*pipeline_stack.pipeline_event);

		// add pipeline stack to event map
		event_map.insert(make_pair(pipeline.get(), move(pipeline_stack)));
	}

	// add base stack to the event data too
	event_map.insert(make_pair(base_pipeline.get(), move(base_stack)));

	// set up the dependencies within this MetaPipeline
	for (auto &pipeline : pipelines) {
		auto dependencies = meta_pipeline->GetDependencies(pipeline.get());
		if (!dependencies) {
			continue;
		}
		auto &pipeline_stack = event_map[pipeline.get()];
		for (auto &dependency : *dependencies) {
			auto &dependency_stack = event_map[dependency];
			pipeline_stack.pipeline_event->AddDependency(*dependency_stack.pipeline_event);
		}
	}
}

void Executor::ScheduleEventsInternal(ScheduleEventData &event_data) {
	auto &events = event_data.events;
	D_ASSERT(events.empty());

	// create all the required pipeline events
	for (auto &pipeline : event_data.meta_pipelines) {
		SchedulePipeline(pipeline, event_data);
	}

	// set up the dependencies across MetaPipelines
	auto &event_map = event_data.event_map;
	for (auto &entry : event_map) {
		auto pipeline = entry.first;
		for (auto &dependency : pipeline->dependencies) {
			auto dep = dependency.lock();
			D_ASSERT(dep);
			auto event_map_entry = event_map.find(dep.get());
			D_ASSERT(event_map_entry != event_map.end());
			auto &dep_entry = event_map_entry->second;
			D_ASSERT(dep_entry.pipeline_complete_event);
			entry.second.pipeline_event->AddDependency(*dep_entry.pipeline_complete_event);
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
	vector<Event *> vertices;
	vertices.reserve(count);
	for (const auto &event : event_data.events) {
		vertices.push_back(event.get());
	}
	vector<bool> visited(count, false);
	vector<bool> recursion_stack(count, false);
	for (idx_t i = 0; i < count; i++) {
		VerifyScheduledEventsInternal(i, vertices, visited, recursion_stack);
	}
#endif
}

void Executor::VerifyScheduledEventsInternal(const idx_t vertex, const vector<Event *> &vertices, vector<bool> &visited,
                                             vector<bool> &recursion_stack) {
	D_ASSERT(!recursion_stack[vertex]); // this vertex is in the recursion stack: circular dependency!
	if (visited[vertex]) {
		return; // early out: we already visited this vertex
	}

	auto &parents = vertices[vertex]->GetParentsVerification();
	if (parents.empty()) {
		return; // early out: outgoing edges
	}

	// create a vector the indices of the adjacent events
	vector<idx_t> adjacent;
	const idx_t count = vertices.size();
	for (auto parent : parents) {
		idx_t i;
		for (i = 0; i < count; i++) {
			if (vertices[i] == parent) {
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

void Executor::AddRecursiveCTE(PhysicalOperator *rec_cte) {
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
	root_executor = make_unique<PipelineExecutor>(context, *root_pipelines[root_pipeline_idx]);
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
				auto &left = *operators[op_idx];
				auto &right = *other_operators[other_idx];
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

void Executor::Initialize(unique_ptr<PhysicalOperator> physical_plan) {
	Reset();
	owned_plan = move(physical_plan);
	InitializeInternal(owned_plan.get());
}

void Executor::Initialize(PhysicalOperator *plan) {
	Reset();
	InitializeInternal(plan);
}

void Executor::InitializeInternal(PhysicalOperator *plan) {

	auto &scheduler = TaskScheduler::GetScheduler(context);
	{
		lock_guard<mutex> elock(executor_lock);
		physical_plan = plan;

		this->profiler = ClientData::Get(context).profiler;
		profiler->Initialize(physical_plan);
		this->producer = scheduler.CreateProducer();

		// build and ready the pipelines
		PipelineBuildState state;
		auto root_pipeline = make_shared<MetaPipeline>(*this, state, nullptr);
		root_pipeline->Build(physical_plan);
		root_pipeline->Ready();

		// ready recursive cte pipelines too
		for (auto &rec_cte : recursive_ctes) {
			D_ASSERT(rec_cte->type == PhysicalOperatorType::RECURSIVE_CTE);
			auto &rec_cte_op = (PhysicalRecursiveCTE &)*rec_cte;
			rec_cte_op.recursive_meta_pipeline->Ready();
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
	// we do this by creating weak pointers to all pipelines
	// then clearing our references to the pipelines
	// and waiting until all pipelines have been destroyed
	vector<weak_ptr<Pipeline>> weak_references;
	{
		lock_guard<mutex> elock(executor_lock);
		weak_references.reserve(pipelines.size());
		cancelled = true;
		for (auto &pipeline : pipelines) {
			weak_references.push_back(weak_ptr<Pipeline>(pipeline));
		}
		for (auto op : recursive_ctes) {
			D_ASSERT(op->type == PhysicalOperatorType::RECURSIVE_CTE);
			auto &rec_cte = (PhysicalRecursiveCTE &)*op;
			rec_cte.recursive_meta_pipeline.reset();
		}
		pipelines.clear();
		root_pipelines.clear();
		events.clear();
	}
	WorkOnTasks();
	for (auto &weak_ref : weak_references) {
		while (true) {
			auto weak = weak_ref.lock();
			if (!weak) {
				break;
			}
		}
	}
}

void Executor::WorkOnTasks() {
	auto &scheduler = TaskScheduler::GetScheduler(context);

	unique_ptr<Task> task;
	while (scheduler.GetTaskFromProducer(*producer, task)) {
		task->Execute(TaskExecutionMode::PROCESS_ALL);
		task.reset();
	}
}

PendingExecutionResult Executor::ExecuteTask() {
	if (execution_result != PendingExecutionResult::RESULT_NOT_READY) {
		return execution_result;
	}
	// check if there are any incomplete pipelines
	auto &scheduler = TaskScheduler::GetScheduler(context);
	while (completed_pipelines < total_pipelines) {
		// there are! if we don't already have a task, fetch one
		if (!task) {
			scheduler.GetTaskFromProducer(*producer, task);
		}
		if (task) {
			// if we have a task, partially process it
			auto result = task->Execute(TaskExecutionMode::PROCESS_PARTIAL);
			if (result != TaskExecutionResult::TASK_NOT_FINISHED) {
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
	execution_result = PendingExecutionResult::RESULT_READY;
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
	exceptions.clear();
	pipelines.clear();
	events.clear();
	execution_result = PendingExecutionResult::RESULT_NOT_READY;
}

shared_ptr<Pipeline> Executor::CreateChildPipeline(Pipeline *current, PhysicalOperator *op) {
	D_ASSERT(!current->operators.empty());
	D_ASSERT(op->IsSource());
	// found another operator that is a source, schedule a child pipeline
	// 'op' is the source, and the sink is the same
	auto child_pipeline = make_shared<Pipeline>(*this);
	child_pipeline->sink = current->sink;
	child_pipeline->source = op;

	// the child pipeline has the same operators up until 'op'
	for (auto current_op : current->operators) {
		if (current_op == op) {
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

void Executor::PushError(PreservedError exception) {
	lock_guard<mutex> elock(error_lock);
	// interrupt execution of any other pipelines that belong to this executor
	context.interrupted = true;
	// push the exception onto the stack
	exceptions.push_back(move(exception));
}

bool Executor::HasError() {
	lock_guard<mutex> elock(error_lock);
	return !exceptions.empty();
}

void Executor::ThrowException() {
	lock_guard<mutex> elock(error_lock);
	D_ASSERT(!exceptions.empty());
	auto &entry = exceptions[0];
	entry.Throw();
}

void Executor::Flush(ThreadContext &tcontext) {
	profiler->Flush(tcontext.profiler);
}

bool Executor::GetPipelinesProgress(double &current_progress) { // LCOV_EXCL_START
	lock_guard<mutex> elock(executor_lock);

	vector<double> progress;
	vector<idx_t> cardinality;
	idx_t total_cardinality = 0;
	for (auto &pipeline : pipelines) {
		double child_percentage;
		idx_t child_cardinality;

		if (!pipeline->GetProgress(child_percentage, child_cardinality)) {
			return false;
		}
		progress.push_back(child_percentage);
		cardinality.push_back(child_cardinality);
		total_cardinality += child_cardinality;
	}
	current_progress = 0;
	for (size_t i = 0; i < progress.size(); i++) {
		current_progress += progress[i] * double(cardinality[i]) / double(total_cardinality);
	}
	return true;
} // LCOV_EXCL_STOP

bool Executor::HasResultCollector() {
	return physical_plan->type == PhysicalOperatorType::RESULT_COLLECTOR;
}

unique_ptr<QueryResult> Executor::GetResult() {
	D_ASSERT(HasResultCollector());
	auto &result_collector = (PhysicalResultCollector &)*physical_plan;
	D_ASSERT(result_collector.sink_state);
	return result_collector.GetResult(*result_collector.sink_state);
}

unique_ptr<DataChunk> Executor::FetchChunk() {
	D_ASSERT(physical_plan);

	auto chunk = make_unique<DataChunk>();
	root_executor->InitializeChunk(*chunk);
	while (true) {
		root_executor->ExecutePull(*chunk);
		if (chunk->size() == 0) {
			root_executor->PullFinalize();
			if (NextExecutor()) {
				continue;
			}
			break;
		} else {
			break;
		}
	}
	return chunk;
}

} // namespace duckdb
