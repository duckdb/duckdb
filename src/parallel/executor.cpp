#include "duckdb/execution/executor.hpp"

#include "duckdb/execution/operator/helper/physical_execute.hpp"
#include "duckdb/execution/operator/join/physical_delim_join.hpp"
#include "duckdb/execution/operator/join/physical_iejoin.hpp"
#include "duckdb/execution/operator/scan/physical_chunk_scan.hpp"
#include "duckdb/execution/operator/set/physical_recursive_cte.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/parallel/pipeline_executor.hpp"

#include "duckdb/parallel/pipeline_event.hpp"
#include "duckdb/parallel/pipeline_finish_event.hpp"
#include "duckdb/parallel/pipeline_complete_event.hpp"

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
	events.push_back(move(event));
}

struct PipelineEventStack {
	Event *pipeline_event;
	Event *pipeline_finish_event;
	Event *pipeline_complete_event;
};

Pipeline *Executor::ScheduleUnionPipeline(const shared_ptr<Pipeline> &pipeline, const Pipeline *parent,
                                          event_map_t &event_map, vector<shared_ptr<Event>> &events) {
	pipeline->Ready();

	D_ASSERT(pipeline);
	auto pipeline_event = make_shared<PipelineEvent>(pipeline);

	auto parent_stack_entry = event_map.find(parent);
	D_ASSERT(parent_stack_entry != event_map.end());

	auto &parent_stack = parent_stack_entry->second;

	PipelineEventStack stack;
	stack.pipeline_event = pipeline_event.get();
	stack.pipeline_finish_event = parent_stack.pipeline_finish_event;
	stack.pipeline_complete_event = parent_stack.pipeline_complete_event;

	stack.pipeline_event->AddDependency(*parent_stack.pipeline_event);
	parent_stack.pipeline_finish_event->AddDependency(*pipeline_event);

	events.push_back(move(pipeline_event));
	event_map.insert(make_pair(pipeline.get(), stack));

	auto parent_pipeline = pipeline.get();

	auto union_entry = union_pipelines.find(pipeline.get());
	if (union_entry != union_pipelines.end()) {
		for (auto &entry : union_entry->second) {
			parent_pipeline = ScheduleUnionPipeline(entry, parent_pipeline, event_map, events);
		}
	}

	return parent_pipeline;
}

void Executor::ScheduleChildPipeline(Pipeline *parent, const shared_ptr<Pipeline> &pipeline, event_map_t &event_map,
                                     vector<shared_ptr<Event>> &events) {
	pipeline->Ready();

	auto child_ptr = pipeline.get();
	auto dependencies = child_dependencies.find(child_ptr);
	D_ASSERT(union_pipelines.find(child_ptr) == union_pipelines.end());
	D_ASSERT(dependencies != child_dependencies.end());
	// create the pipeline event and the event stack
	auto pipeline_event = make_shared<PipelineEvent>(pipeline);

	auto parent_entry = event_map.find(parent);
	PipelineEventStack stack;
	stack.pipeline_event = pipeline_event.get();
	stack.pipeline_finish_event = parent_entry->second.pipeline_finish_event;
	stack.pipeline_complete_event = parent_entry->second.pipeline_complete_event;

	// set up the dependencies for this child pipeline
	unordered_set<Event *> finish_events;
	for (auto &dep : dependencies->second) {
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
	}

	events.push_back(move(pipeline_event));
	event_map.insert(make_pair(child_ptr, stack));
}

void Executor::SchedulePipeline(const shared_ptr<Pipeline> &pipeline, event_map_t &event_map,
                                vector<shared_ptr<Event>> &events, bool complete_pipeline) {
	D_ASSERT(pipeline);

	pipeline->Ready();

	auto pipeline_event = make_shared<PipelineEvent>(pipeline);
	auto pipeline_finish_event = make_shared<PipelineFinishEvent>(pipeline);
	auto pipeline_complete_event = make_shared<PipelineCompleteEvent>(pipeline->executor, complete_pipeline);

	PipelineEventStack stack;
	stack.pipeline_event = pipeline_event.get();
	stack.pipeline_finish_event = pipeline_finish_event.get();
	stack.pipeline_complete_event = pipeline_complete_event.get();

	pipeline_finish_event->AddDependency(*pipeline_event);
	pipeline_complete_event->AddDependency(*pipeline_finish_event);

	events.push_back(move(pipeline_event));
	events.push_back(move(pipeline_finish_event));
	events.push_back(move(pipeline_complete_event));

	event_map.insert(make_pair(pipeline.get(), stack));

	auto union_entry = union_pipelines.find(pipeline.get());
	if (union_entry != union_pipelines.end()) {
		auto parent_pipeline = pipeline.get();
		for (auto &entry : union_entry->second) {
			parent_pipeline = ScheduleUnionPipeline(entry, parent_pipeline, event_map, events);
		}
	}
}

void Executor::ScheduleEventsInternal(const vector<shared_ptr<Pipeline>> &pipelines,
                                      unordered_map<Pipeline *, vector<shared_ptr<Pipeline>>> &child_pipelines,
                                      vector<shared_ptr<Event>> &events, bool main_schedule) {
	D_ASSERT(events.empty());
	// create all the required pipeline events
	event_map_t event_map;
	for (auto &pipeline : pipelines) {
		SchedulePipeline(pipeline, event_map, events, main_schedule);
	}
	// schedule child pipelines
	for (auto &entry : child_pipelines) {
		// iterate in reverse order
		// since child entries are added from top to bottom
		// dependencies are in reverse order (bottom to top)
		for (idx_t i = entry.second.size(); i > 0; i--) {
			auto &child_entry = entry.second[i - 1];
			ScheduleChildPipeline(entry.first, child_entry, event_map, events);
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
	ScheduleEventsInternal(pipelines, child_pipelines, events);
}

void Executor::ReschedulePipelines(const vector<shared_ptr<Pipeline>> &pipelines, vector<shared_ptr<Event>> &events) {
	unordered_map<Pipeline *, vector<shared_ptr<Pipeline>>> child_pipelines;
	ScheduleEventsInternal(pipelines, child_pipelines, events, false);
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
		for (auto &entry : child_entry->second) {
			ExtractPipelines(entry, result);
		}
		child_pipelines.erase(pipeline_ptr);
	}
}

bool Executor::NextExecutor() {
	if (root_pipeline_idx >= root_pipelines.size()) {
		return false;
	}
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

void Executor::Initialize(PhysicalOperator *plan) {
	Reset();

	auto &scheduler = TaskScheduler::GetScheduler(context);
	{
		lock_guard<mutex> elock(executor_lock);
		physical_plan = plan;

		this->profiler = ClientData::Get(context).profiler;
		profiler->Initialize(physical_plan);
		this->producer = scheduler.CreateProducer();

		auto root_pipeline = make_shared<Pipeline>(*this);
		root_pipeline->sink = nullptr;
		BuildPipelines(physical_plan, root_pipeline.get());

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
		if (pipelines.empty()) {
			return;
		}
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
	if (!exceptions.empty()) { // LCOV_EXCL_START
		// an exception has occurred executing one of the pipelines
		execution_result = PendingExecutionResult::EXECUTION_ERROR;
		ThrowExceptionInternal();
	} // LCOV_EXCL_STOP
	execution_result = PendingExecutionResult::RESULT_READY;
	return execution_result;
}

void Executor::Reset() {
	lock_guard<mutex> elock(executor_lock);
	delim_join_dependencies.clear();
	recursive_cte = nullptr;
	physical_plan = nullptr;
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
	child_dependencies.clear();
	execution_result = PendingExecutionResult::RESULT_NOT_READY;
}

void Executor::AddChildPipeline(Pipeline *current) {
	D_ASSERT(!current->operators.empty());
	// found another operator that is a source
	// schedule a child pipeline
	auto child_pipeline = make_shared<Pipeline>(*this);
	auto child_pipeline_ptr = child_pipeline.get();
	child_pipeline->sink = current->sink;
	child_pipeline->operators = current->operators;
	child_pipeline->source = current->operators.back();
	D_ASSERT(child_pipeline->source->IsSource());
	child_pipeline->operators.pop_back();

	vector<Pipeline *> dependencies;
	dependencies.push_back(current);
	auto child_entry = child_pipelines.find(current);
	if (child_entry != child_pipelines.end()) {
		for (auto &current_child : child_entry->second) {
			D_ASSERT(child_dependencies.find(current_child.get()) != child_dependencies.end());
			child_dependencies[current_child.get()].push_back(child_pipeline_ptr);
		}
	}
	D_ASSERT(child_dependencies.find(child_pipeline_ptr) == child_dependencies.end());
	child_dependencies.insert(make_pair(child_pipeline_ptr, move(dependencies)));
	child_pipelines[current].push_back(move(child_pipeline));
}

void Executor::BuildPipelines(PhysicalOperator *op, Pipeline *current) {
	D_ASSERT(current);
	op->op_state.reset();
	if (op->IsSink()) {
		// operator is a sink, build a pipeline
		op->sink_state.reset();

		PhysicalOperator *pipeline_child = nullptr;
		switch (op->type) {
		case PhysicalOperatorType::CREATE_TABLE_AS:
		case PhysicalOperatorType::CREATE_MATVIEW:
		case PhysicalOperatorType::INSERT:
		case PhysicalOperatorType::DELETE_OPERATOR:
		case PhysicalOperatorType::UPDATE:
		case PhysicalOperatorType::HASH_GROUP_BY:
		case PhysicalOperatorType::SIMPLE_AGGREGATE:
		case PhysicalOperatorType::PERFECT_HASH_GROUP_BY:
		case PhysicalOperatorType::WINDOW:
		case PhysicalOperatorType::ORDER_BY:
		case PhysicalOperatorType::RESERVOIR_SAMPLE:
		case PhysicalOperatorType::TOP_N:
		case PhysicalOperatorType::COPY_TO_FILE:
		case PhysicalOperatorType::LIMIT:
		case PhysicalOperatorType::LIMIT_PERCENT:
		case PhysicalOperatorType::EXPLAIN_ANALYZE:
			D_ASSERT(op->children.size() == 1);
			// single operator:
			// the operator becomes the data source of the current pipeline
			current->source = op;
			// we create a new pipeline starting from the child
			pipeline_child = op->children[0].get();
			break;
		case PhysicalOperatorType::EXPORT:
			// EXPORT has an optional child
			// we only need to schedule child pipelines if there is a child
			current->source = op;
			if (op->children.empty()) {
				return;
			}
			D_ASSERT(op->children.size() == 1);
			pipeline_child = op->children[0].get();
			break;
		case PhysicalOperatorType::NESTED_LOOP_JOIN:
		case PhysicalOperatorType::BLOCKWISE_NL_JOIN:
		case PhysicalOperatorType::HASH_JOIN:
		case PhysicalOperatorType::PIECEWISE_MERGE_JOIN:
		case PhysicalOperatorType::CROSS_PRODUCT:
			// regular join, create a pipeline with RHS source that sinks into this pipeline
			pipeline_child = op->children[1].get();
			// on the LHS (probe child), the operator becomes a regular operator
			current->operators.push_back(op);
			if (op->IsSource()) {
				// FULL or RIGHT outer join
				// schedule a scan of the node as a child pipeline
				// this scan has to be performed AFTER all the probing has happened
				if (recursive_cte) {
					throw NotImplementedException("FULL and RIGHT outer joins are not supported in recursive CTEs yet");
				}
				AddChildPipeline(current);
			}
			BuildPipelines(op->children[0].get(), current);
			break;
		case PhysicalOperatorType::IE_JOIN: {
			D_ASSERT(op->children.size() == 2);
			if (recursive_cte) {
				throw NotImplementedException("IEJoins are not supported in recursive CTEs yet");
			}

			// Build the LHS
			auto lhs_pipeline = make_shared<Pipeline>(*this);
			lhs_pipeline->sink = op;
			D_ASSERT(op->children[0].get());
			BuildPipelines(op->children[0].get(), lhs_pipeline.get());

			// Build the RHS
			auto rhs_pipeline = make_shared<Pipeline>(*this);
			rhs_pipeline->sink = op;
			D_ASSERT(op->children[1].get());
			BuildPipelines(op->children[1].get(), rhs_pipeline.get());

			// RHS => LHS => current
			current->AddDependency(rhs_pipeline);
			rhs_pipeline->AddDependency(lhs_pipeline);

			pipelines.emplace_back(move(lhs_pipeline));
			pipelines.emplace_back(move(rhs_pipeline));

			// Now build both and scan
			current->source = op;
			return;
		}
		case PhysicalOperatorType::DELIM_JOIN: {
			// duplicate eliminated join
			// for delim joins, recurse into the actual join
			pipeline_child = op->children[0].get();
			break;
		}
		case PhysicalOperatorType::RECURSIVE_CTE: {
			auto &cte_node = (PhysicalRecursiveCTE &)*op;

			// recursive CTE
			current->source = op;
			// the LHS of the recursive CTE is our initial state
			// we build this pipeline as normal
			pipeline_child = op->children[0].get();
			// for the RHS, we gather all pipelines that depend on the recursive cte
			// these pipelines need to be rerun
			if (recursive_cte) {
				throw InternalException("Recursive CTE detected WITHIN a recursive CTE node");
			}
			recursive_cte = op;

			auto recursive_pipeline = make_shared<Pipeline>(*this);
			recursive_pipeline->sink = op;
			op->sink_state.reset();
			BuildPipelines(op->children[1].get(), recursive_pipeline.get());

			cte_node.pipelines.push_back(move(recursive_pipeline));

			recursive_cte = nullptr;
			break;
		}
		default:
			throw InternalException("Unimplemented sink type!");
		}
		// the current is dependent on this pipeline to complete
		auto pipeline = make_shared<Pipeline>(*this);
		pipeline->sink = op;
		current->AddDependency(pipeline);
		D_ASSERT(pipeline_child);
		// recurse into the pipeline child
		BuildPipelines(pipeline_child, pipeline.get());
		if (op->type == PhysicalOperatorType::DELIM_JOIN) {
			// for delim joins, recurse into the actual join
			// any pipelines in there depend on the main pipeline
			auto &delim_join = (PhysicalDelimJoin &)*op;
			// any scan of the duplicate eliminated data on the RHS depends on this pipeline
			// we add an entry to the mapping of (PhysicalOperator*) -> (Pipeline*)
			for (auto &delim_scan : delim_join.delim_scans) {
				delim_join_dependencies[delim_scan] = pipeline.get();
			}
			BuildPipelines(delim_join.join.get(), current);
		}
		if (!recursive_cte) {
			// regular pipeline: schedule it
			pipelines.push_back(move(pipeline));
		} else {
			// CTE pipeline! add it to the CTE pipelines
			D_ASSERT(recursive_cte);
			auto &cte = (PhysicalRecursiveCTE &)*recursive_cte;
			cte.pipelines.push_back(move(pipeline));
		}
	} else {
		// operator is not a sink! recurse in children
		// first check if there is any additional action we need to do depending on the type
		switch (op->type) {
		case PhysicalOperatorType::DELIM_SCAN: {
			D_ASSERT(op->children.empty());
			auto entry = delim_join_dependencies.find(op);
			D_ASSERT(entry != delim_join_dependencies.end());
			// this chunk scan introduces a dependency to the current pipeline
			// namely a dependency on the duplicate elimination pipeline to finish
			auto delim_dependency = entry->second->shared_from_this();
			D_ASSERT(delim_dependency->sink->type == PhysicalOperatorType::DELIM_JOIN);
			auto &delim_join = (PhysicalDelimJoin &)*delim_dependency->sink;
			current->AddDependency(delim_dependency);
			current->source = (PhysicalOperator *)delim_join.distinct.get();
			return;
		}
		case PhysicalOperatorType::EXECUTE: {
			// EXECUTE statement: build pipeline on child
			auto &execute = (PhysicalExecute &)*op;
			BuildPipelines(execute.plan, current);
			return;
		}
		case PhysicalOperatorType::RECURSIVE_CTE_SCAN: {
			if (!recursive_cte) {
				throw InternalException("Recursive CTE scan found without recursive CTE node");
			}
			break;
		}
		case PhysicalOperatorType::INDEX_JOIN: {
			// index join: we only continue into the LHS
			// the right side is probed by the index join
			// so we don't need to do anything in the pipeline with this child
			current->operators.push_back(op);
			BuildPipelines(op->children[0].get(), current);
			return;
		}
		case PhysicalOperatorType::UNION: {
			if (recursive_cte) {
				throw NotImplementedException("UNIONS are not supported in recursive CTEs yet");
			}
			auto union_pipeline = make_shared<Pipeline>(*this);
			auto pipeline_ptr = union_pipeline.get();
			// set up dependencies for any child pipelines to this union pipeline
			auto child_entry = child_pipelines.find(current);
			if (child_entry != child_pipelines.end()) {
				for (auto &current_child : child_entry->second) {
					D_ASSERT(child_dependencies.find(current_child.get()) != child_dependencies.end());
					child_dependencies[current_child.get()].push_back(pipeline_ptr);
				}
			}
			// for the current pipeline, continue building on the LHS
			union_pipeline->operators = current->operators;
			BuildPipelines(op->children[0].get(), current);
			// insert the union pipeline as a union pipeline of the current node
			union_pipelines[current].push_back(move(union_pipeline));

			// for the union pipeline, build on the RHS
			pipeline_ptr->sink = current->sink;
			BuildPipelines(op->children[1].get(), pipeline_ptr);
			return;
		}
		default:
			break;
		}
		if (op->children.empty()) {
			// source
			current->source = op;
		} else {
			if (op->children.size() != 1) {
				throw InternalException("Operator not supported yet");
			}
			current->operators.push_back(op);
			BuildPipelines(op->children[0].get(), current);
		}
	}
}

vector<LogicalType> Executor::GetTypes() {
	D_ASSERT(physical_plan);
	return physical_plan->GetTypes();
}

void Executor::PushError(ExceptionType type, const string &exception) {
	lock_guard<mutex> elock(executor_lock);
	// interrupt execution of any other pipelines that belong to this executor
	context.interrupted = true;
	// push the exception onto the stack
	exceptions.emplace_back(type, exception);
}

bool Executor::HasError() {
	lock_guard<mutex> elock(executor_lock);
	return !exceptions.empty();
}

void Executor::ThrowException() {
	lock_guard<mutex> elock(executor_lock);
	ThrowExceptionInternal();
}

void Executor::ThrowExceptionInternal() { // LCOV_EXCL_START
	D_ASSERT(!exceptions.empty());
	auto &entry = exceptions[0];
	switch (entry.first) {
	case ExceptionType::TRANSACTION:
		throw TransactionException(entry.second);
	case ExceptionType::CATALOG:
		throw CatalogException(entry.second);
	case ExceptionType::PARSER:
		throw ParserException(entry.second);
	case ExceptionType::BINDER:
		throw BinderException(entry.second);
	case ExceptionType::INTERRUPT:
		throw InterruptException();
	case ExceptionType::FATAL:
		throw FatalException(entry.second);
	case ExceptionType::INTERNAL:
		throw InternalException(entry.second);
	case ExceptionType::IO:
		throw IOException(entry.second);
	case ExceptionType::CONSTRAINT:
		throw ConstraintException(entry.second);
	case ExceptionType::CONVERSION:
		throw ConversionException(entry.second);
	default:
		throw Exception(entry.second);
	}
} // LCOV_EXCL_STOP

void Executor::Flush(ThreadContext &tcontext) {
	profiler->Flush(tcontext.profiler);
}

bool Executor::GetPipelinesProgress(double &current_progress) { // LCOV_EXCL_START
	lock_guard<mutex> elock(executor_lock);

	if (!pipelines.empty()) {
		return pipelines.back()->GetProgress(current_progress);
	} else {
		current_progress = -1;
		return true;
	}
} // LCOV_EXCL_STOP

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
