#include "duckdb/parallel/pipeline.hpp"

#include "duckdb/common/printer.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/main/database.hpp"

#include "duckdb/execution/operator/aggregate/physical_simple_aggregate.hpp"
#include "duckdb/execution/operator/aggregate/physical_window.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/execution/operator/order/physical_order.hpp"
#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"
#include "duckdb/execution/operator/join/physical_hash_join.hpp"
#include "duckdb/parallel/pipeline_executor.hpp"
#include "duckdb/parallel/pipeline_event.hpp"

#include "duckdb/common/algorithm.hpp"

namespace duckdb {

class PipelineTask : public Task {
public:
	explicit PipelineTask(Pipeline &pipeline, shared_ptr<Event> event_p)
	    : event(move(event_p)), executor(pipeline.GetClientContext(), pipeline) {
	}

	shared_ptr<Event> event;
	PipelineExecutor executor;

public:
	void Execute() override {
		executor.Execute();
		event->FinishTask();
	}
};

Pipeline::Pipeline(Executor &executor_p)
    : executor(executor_p), sink(nullptr) {
}

ClientContext &Pipeline::GetClientContext() {
	return executor.context;
}

bool Pipeline::GetProgress(ClientContext &context, PhysicalOperator *op, int &current_percentage) {
	current_percentage = -1;
	switch (op->type) {
	case PhysicalOperatorType::TABLE_SCAN: {
		auto &get = (PhysicalTableScan &)*op;
		if (get.function.table_scan_progress) {
			current_percentage = get.function.table_scan_progress(context, get.bind_data.get());
			return true;
		}
		//! If the table_scan_progress is not implemented it means we don't support this function yet in the progress
		//! bar
		return false;
	}
	default:
		return false;
	}
}

bool Pipeline::GetProgress(int &current_percentage) {
	auto &client = executor.context;
	return GetProgress(client, operators[source_idx], current_percentage);
}

void Pipeline::ScheduleSequentialTask(shared_ptr<Event> &event) {
	vector<unique_ptr<Task>> tasks;
	tasks.push_back(make_unique<PipelineTask>(*this, event));
	event->SetTasks(move(tasks));
}

bool Pipeline::ScheduleParallel(shared_ptr<Event> &event) {
	if (!sink->ParallelSink()) {
		return false;
	}
	if (!operators[source_idx]->ParallelSource()) {
		return false;
	}
	for(idx_t i = source_idx + 1; i < operators.size(); i++) {
		if (!operators[i]->ParallelOperator()) {
			return false;
		}
	}
	idx_t max_threads = source_state->MaxThreads();
	return LaunchScanTasks(event, max_threads);
}

void Pipeline::Schedule(shared_ptr<Event> &event) {
	if (!sink) {
		return;
	}
	if (!ScheduleParallel(event)) {
		// could not parallelize this pipeline: push a sequential task instead
		ScheduleSequentialTask(event);
	}
}

bool Pipeline::LaunchScanTasks(shared_ptr<Event> &event, idx_t max_threads) {
	// split the scan up into parts and schedule the parts
	auto &scheduler = TaskScheduler::GetScheduler(executor.context);
	idx_t active_threads = scheduler.NumberOfThreads();
	if (max_threads > active_threads) {
		max_threads = active_threads;
	}
	if (max_threads <= 1) {
		// too small to parallelize
		return false;
	}

	// launch a task for every thread
	vector<unique_ptr<Task>> tasks;
	for (idx_t i = 0; i < max_threads; i++) {
		tasks.push_back(make_unique<PipelineTask>(*this, event));
	}
	event->SetTasks(move(tasks));
	return true;
}

void Pipeline::Reset() {
	if (sink) {
		sink->sink_state = sink->GetGlobalSinkState(GetClientContext());
	}
	ResetSource();
}

void Pipeline::ResetSource() {
	source_state = operators[source_idx]->GetGlobalSourceState(GetClientContext());
}

void Pipeline::Ready() {
	std::reverse(operators.begin(), operators.end());
	Reset();
}

bool Pipeline::NextSource() {
	// check if there are more sources to schedule within this pipeline
	for(idx_t i = source_idx + 1; i < operators.size(); i++) {
		if (operators[i]->IsSource()) {
			// there is another source! schedule it instead of finishing this event
			source_idx = i;
			ResetSource();
			return true;
		}
	}
	return false;
}

void Pipeline::Finalize(Event &event) {
	try {
		sink->Finalize(*this, event, executor.context, *sink->sink_state);
	} catch (std::exception &ex) {
		executor.PushError(ex.what());
	} catch (...) { // LCOV_EXCL_START
		executor.PushError("Unknown exception in Finalize!");
	} // LCOV_EXCL_STOP
}

void Pipeline::AddDependency(shared_ptr<Pipeline> &pipeline) {
	if (!pipeline) {
		return;
	}
	dependencies.push_back(weak_ptr<Pipeline>(pipeline));
	pipeline->parents.push_back(weak_ptr<Pipeline>(shared_from_this()));
}

string Pipeline::ToString() const {
	string str = PhysicalOperatorToString(sink->type);
	for (auto &op : operators) {
		str += PhysicalOperatorToString(op->type) + " -> " + str;
	}
	return str;
}

void Pipeline::Print() const {
	Printer::Print(ToString());
}

} // namespace duckdb
