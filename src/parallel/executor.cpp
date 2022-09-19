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
	Event *pipeline_event;
	Event *pipeline_finish_event;
	Event *pipeline_complete_event;
};

using event_map_t = unordered_map<const Pipeline *, PipelineEventStack>;

struct ScheduleEventData {
	ScheduleEventData(const vector<shared_ptr<Pipeline>> &pipelines,
	                  unordered_map<Pipeline *, vector<shared_ptr<Pipeline>>> &child_pipelines,
	                  unordered_map<Pipeline *, vector<shared_ptr<Pipeline>>> &union_pipelines,
	                  vector<shared_ptr<Event>> &events, bool initial_schedule)
	    : pipelines(pipelines), child_pipelines(child_pipelines), union_pipelines(union_pipelines), events(events),
	      initial_schedule(initial_schedule) {
	}

	const vector<shared_ptr<Pipeline>> &pipelines;
	unordered_map<Pipeline *, vector<shared_ptr<Pipeline>>> &child_pipelines;
	unordered_map<Pipeline *, vector<shared_ptr<Pipeline>>> &union_pipelines;
	unordered_map<Pipeline *, vector<Pipeline *>> scheduled_pipelines;
	vector<shared_ptr<Event>> &events;
	bool initial_schedule;
	event_map_t event_map;
};

void Executor::SchedulePipeline(const shared_ptr<Pipeline> &pipeline, ScheduleEventData &event_data,
                                vector<Pipeline *> &scheduled_pipelines) {
	D_ASSERT(pipeline);

	auto &event_map = event_data.event_map;
	auto &events = event_data.events;
	auto &union_pipelines = event_data.union_pipelines;
	pipeline->Ready();

	auto pipeline_event = make_shared<PipelineEvent>(pipeline);

	PipelineEventStack stack;
	stack.pipeline_event = pipeline_event.get();
	if (!scheduled_pipelines.empty()) {
		// this pipeline has a parent pipeline - i.e. it is scheduled as part of a `UNION`
		// set up the events
		auto parent = scheduled_pipelines.back();
		auto parent_stack_entry = event_map.find(parent);
		D_ASSERT(parent_stack_entry != event_map.end());

		auto &parent_stack = parent_stack_entry->second;
		stack.pipeline_finish_event = parent_stack.pipeline_finish_event;
		stack.pipeline_complete_event = parent_stack.pipeline_complete_event;

		stack.pipeline_event->AddDependency(*parent_stack.pipeline_event);
		parent_stack.pipeline_finish_event->AddDependency(*pipeline_event);
	} else {
		// stand-alone pipeline
		auto pipeline_finish_event = make_shared<PipelineFinishEvent>(pipeline);
		auto pipeline_complete_event =
		    make_shared<PipelineCompleteEvent>(pipeline->executor, event_data.initial_schedule);

		pipeline_finish_event->AddDependency(*pipeline_event);
		pipeline_complete_event->AddDependency(*pipeline_finish_event);

		stack.pipeline_finish_event = pipeline_finish_event.get();
		stack.pipeline_complete_event = pipeline_complete_event.get();

		events.push_back(move(pipeline_finish_event));
		events.push_back(move(pipeline_complete_event));
	}

	events.push_back(move(pipeline_event));
	event_map.insert(make_pair(pipeline.get(), stack));

	scheduled_pipelines.push_back(pipeline.get());
	auto union_entry = union_pipelines.find(pipeline.get());
	if (union_entry != union_pipelines.end()) {
		for (auto &entry : union_entry->second) {
			SchedulePipeline(entry, event_data, scheduled_pipelines);
		}
	}
}

void Executor::ScheduleChildPipeline(Pipeline *parent, const shared_ptr<Pipeline> &pipeline,
                                     ScheduleEventData &event_data) {
	auto &events = event_data.events;
	pipeline->Ready();

	auto child_ptr = pipeline.get();
	D_ASSERT(event_data.union_pipelines.find(child_ptr) == event_data.union_pipelines.end());
	// create the pipeline event and the event stack
	auto pipeline_event = make_shared<PipelineEvent>(pipeline);

	auto &event_map = event_data.event_map;
	auto parent_entry = event_map.find(parent);
	D_ASSERT(parent_entry != event_map.end());

	PipelineEventStack stack;
	stack.pipeline_event = pipeline_event.get();
	stack.pipeline_finish_event = parent_entry->second.pipeline_finish_event;
	stack.pipeline_complete_event = parent_entry->second.pipeline_complete_event;
	// set up the dependencies for this child pipeline
	unordered_set<Event *> finish_events;

	vector<Pipeline *> remaining_pipelines;
	unordered_set<Pipeline *> already_scheduled;
	remaining_pipelines.push_back(parent);
	for (idx_t i = 0; i < remaining_pipelines.size(); i++) {
		auto dep = remaining_pipelines[i];
		if (already_scheduled.find(dep) != already_scheduled.end()) {
			continue;
		}
		already_scheduled.insert(dep);

		auto dep_scheduled = event_data.scheduled_pipelines.find(dep);
		if (dep_scheduled != event_data.scheduled_pipelines.end()) {
			for (auto &next_dep : dep_scheduled->second) {
				remaining_pipelines.push_back(next_dep);
			}
		}

		auto dep_entry = event_map.find(dep);
		D_ASSERT(dep_entry != event_map.end());
		D_ASSERT(dep_entry->second.pipeline_event);
		D_ASSERT(dep_entry->second.pipeline_finish_event);

		auto finish_event = dep_entry->second.pipeline_finish_event;
		stack.pipeline_event->AddDependency(*dep_entry->second.pipeline_event);
		if (finish_events.find(finish_event) == finish_events.end()) {
			finish_event->AddDependency(*stack.pipeline_event);
			finish_events.insert(finish_event);
		}

		event_data.scheduled_pipelines[dep].push_back(child_ptr);
	}

	events.push_back(move(pipeline_event));
	event_map.insert(make_pair(child_ptr, stack));
}

void Executor::ScheduleEventsInternal(ScheduleEventData &event_data) {
	auto &events = event_data.events;
	D_ASSERT(events.empty());
	// create all the required pipeline events
	auto &event_map = event_data.event_map;
	for (auto &pipeline : event_data.pipelines) {
		vector<Pipeline *> scheduled_pipelines;
		SchedulePipeline(pipeline, event_data, scheduled_pipelines);

		event_data.scheduled_pipelines[pipeline.get()] = move(scheduled_pipelines);
	}
	// schedule child pipelines
	for (auto &entry : event_data.child_pipelines) {
		// iterate in reverse order
		// since child entries are added from top to bottom
		// dependencies are in reverse order (bottom to top)
		for (idx_t i = entry.second.size(); i > 0; i--) {
			auto &child_entry = entry.second[i - 1];
			ScheduleChildPipeline(entry.first, child_entry, event_data);
		}
	}
	// set up the dependencies between pipeline events
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
	// schedule the pipelines that do not have dependencies
	for (auto &event : events) {
		if (!event->HasDependencies()) {
			event->Schedule();
		}
	}
}

void Executor::ScheduleEvents() {
	ScheduleEventData event_data(pipelines, child_pipelines, union_pipelines, events, true);
	ScheduleEventsInternal(event_data);
}

void Executor::ReschedulePipelines(const vector<shared_ptr<Pipeline>> &pipelines, vector<shared_ptr<Event>> &events) {
	unordered_map<Pipeline *, vector<shared_ptr<Pipeline>>> child_pipelines;
	unordered_map<Pipeline *, vector<shared_ptr<Pipeline>>> union_pipelines;
	ScheduleEventData event_data(pipelines, child_pipelines, union_pipelines, events, false);
	ScheduleEventsInternal(event_data);
}

void Executor::ExtractPipelines(shared_ptr<Pipeline> &pipeline, vector<shared_ptr<Pipeline>> &result) {
	pipeline->Ready();

	auto pipeline_ptr = pipeline.get();
	result.push_back(move(pipeline));
	auto union_entry = union_pipelines.find(pipeline_ptr);
	if (union_entry != union_pipelines.end()) {
		auto &union_pipeline_list = union_entry->second;
		for (auto &pipeline : union_pipeline_list) {
			ExtractPipelines(pipeline, result);
		}
		union_pipelines.erase(pipeline_ptr);
	}
	auto child_entry = child_pipelines.find(pipeline_ptr);
	if (child_entry != child_pipelines.end()) {
		for (auto entry = child_entry->second.rbegin(); entry != child_entry->second.rend(); ++entry) {
			ExtractPipelines(*entry, result);
		}
		child_pipelines.erase(pipeline_ptr);
	}
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
	for (auto &pipeline : root_pipelines) {
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

		auto root_pipeline = make_shared<Pipeline>(*this);
		root_pipeline->sink = nullptr;

		PipelineBuildState state;
		physical_plan->BuildPipelines(*this, *root_pipeline, state);

		this->total_pipelines = pipelines.size();

		root_pipeline_idx = 0;
		ExtractPipelines(root_pipeline, root_pipelines);

		VerifyPipelines();

		ScheduleEvents();
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
		cancelled = true;
		weak_references.reserve(pipelines.size());
		for (auto &pipeline : pipelines) {
			weak_references.push_back(weak_ptr<Pipeline>(pipeline));
		}
		for (auto &kv : union_pipelines) {
			for (auto &pipeline : kv.second) {
				weak_references.push_back(weak_ptr<Pipeline>(pipeline));
			}
		}
		for (auto &kv : child_pipelines) {
			for (auto &pipeline : kv.second) {
				weak_references.push_back(weak_ptr<Pipeline>(pipeline));
			}
		}
		pipelines.clear();
		union_pipelines.clear();
		child_pipelines.clear();
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
	union_pipelines.clear();
	child_pipelines.clear();
	execution_result = PendingExecutionResult::RESULT_NOT_READY;
}

void Executor::AddChildPipeline(Pipeline *current) {
	D_ASSERT(!current->operators.empty());
	// found another operator that is a source
	// schedule a child pipeline
	auto child_pipeline = make_shared<Pipeline>(*this);
	child_pipeline->sink = current->sink;
	child_pipeline->operators = current->operators;
	child_pipeline->source = current->operators.back();
	D_ASSERT(child_pipeline->source->IsSource());
	child_pipeline->operators.pop_back();

	vector<Pipeline *> dependencies;
	dependencies.push_back(current);
	child_pipelines[current].push_back(move(child_pipeline));
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
