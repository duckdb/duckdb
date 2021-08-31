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

#include "duckdb/common/algorithm.hpp"

namespace duckdb {

class PipelineTask : public Task {
public:
	explicit PipelineTask(shared_ptr<Pipeline> pipeline_p)
	    : pipeline(move(pipeline_p)), executor(pipeline->GetClientContext(), *pipeline) {
	}

	shared_ptr<Pipeline> pipeline;
	PipelineExecutor executor;

public:
	void Execute() override {
		executor.Execute();
		pipeline->FinishTask();
	}
};

Pipeline::Pipeline(Executor &executor_p, ProducerToken &token_p)
    : executor(executor_p), token(token_p), finished_tasks(0), total_tasks(0), source(nullptr), sink(nullptr),
      finished_dependencies(0), finished(false), recursive_cte(nullptr) {
}

ClientContext &Pipeline::GetClientContext() {
	return executor.context;
}

bool Pipeline::GetProgress(ClientContext &context, PhysicalOperator *op, int &current_percentage) {
	switch (op->type) {
	case PhysicalOperatorType::TABLE_SCAN: {
		auto &get = (PhysicalTableScan &)*op;
		if (get.function.table_scan_progress) {
			current_percentage = get.function.table_scan_progress(context, get.bind_data.get());
			return true;
		}
		//! If the table_scan_progress is not implemented it means we don't support this function yet in the progress
		//! bar
		current_percentage = -1;
		return false;
	}
	default:
		return false;
	}
}

bool Pipeline::GetProgress(int &current_percentage) {
	auto &client = executor.context;
	return GetProgress(client, source, current_percentage);
}

void Pipeline::FinishTask() {
	D_ASSERT(finished_tasks < total_tasks);
	idx_t current_tasks = total_tasks;
	idx_t current_finished = ++finished_tasks;
	if (current_finished == current_tasks) {
		bool finish_pipeline = false;
		try {
			finish_pipeline = sink->Finalize(*this, executor.context, move(sink_state));
		} catch (std::exception &ex) {
			executor.PushError(ex.what());
		} catch (...) { // LCOV_EXCL_START
			executor.PushError("Unknown exception in Finalize!");
		} // LCOV_EXCL_STOP
		if (finish_pipeline) {
			Finish();
		}
	}
}

void Pipeline::ScheduleSequentialTask() {
	auto &scheduler = TaskScheduler::GetScheduler(executor.context);
	auto task = make_unique<PipelineTask>(shared_from_this());

	this->total_tasks = 1;
	scheduler.ScheduleTask(*executor.producer, move(task));
}

bool Pipeline::ScheduleParallel() {
	if (!sink->ParallelSink()) {
		return false;
	}
	if (!source->ParallelSource()) {
		return false;
	}
	for(auto &op : operators) {
		if (!op->ParallelOperator()) {
			return false;
		}
	}
	idx_t max_threads = source_state->MaxThreads();
	return LaunchScanTasks(max_threads);
}

void Pipeline::Schedule() {
	D_ASSERT(finished_tasks == 0);
	D_ASSERT(total_tasks == 0);
	D_ASSERT(finished_dependencies == dependencies.size());
	if (!sink) {
		return;
	}
	if (!ScheduleParallel()) {
		// could not parallelize this pipeline: push a sequential task instead
		ScheduleSequentialTask();
	}
}

bool Pipeline::LaunchScanTasks(idx_t max_threads) {
	// split the scan up into parts and schedule the parts
	auto &scheduler = TaskScheduler::GetScheduler(executor.context);
	if (max_threads > executor.context.db->NumberOfThreads()) {
		max_threads = executor.context.db->NumberOfThreads();
	}
	if (max_threads <= 1) {
		// too small to parallelize
		return false;
	}

	// launch a task for every thread
	this->total_tasks = max_threads;
	for (idx_t i = 0; i < max_threads; i++) {
		auto task = make_unique<PipelineTask>(shared_from_this());
		scheduler.ScheduleTask(*executor.producer, move(task));
	}
	return true;
}

void Pipeline::ClearParents() {
	for (auto &parent_entry : parents) {
		auto parent = parent_entry.second.lock();
		if (!parent) {
			continue;
		}
		parent->dependencies.erase(this);
	}
	for (auto &dep_entry : dependencies) {
		auto dep = dep_entry.second.lock();
		if (!dep) {
			continue;
		}
		dep->parents.erase(this);
	}
	parents.clear();
	dependencies.clear();
}

void Pipeline::Reset() {
	if (sink) {
		sink_state = sink->GetGlobalSinkState(GetClientContext());
	}
	source_state = source->GetGlobalSourceState(GetClientContext());
	finished_tasks = 0;
	total_tasks = 0;
	finished = false;
}

void Pipeline::Ready() {
	Reset();
	std::reverse(operators.begin(), operators.end());
}

void Pipeline::AddDependency(shared_ptr<Pipeline> &pipeline) {
	if (!pipeline) {
		return;
	}
	dependencies[pipeline.get()] = weak_ptr<Pipeline>(pipeline);
	pipeline->parents[this] = weak_ptr<Pipeline>(shared_from_this());
}

void Pipeline::CompleteDependency() {
	idx_t current_finished = ++finished_dependencies;
	D_ASSERT(current_finished <= dependencies.size());
	if (current_finished == dependencies.size()) {
		// all dependencies have been completed: schedule the pipeline
		Schedule();
	}
}

void Pipeline::Finish() {
	D_ASSERT(!finished);
	finished = true;
	// finished processing the pipeline, now we can schedule pipelines that depend on this pipeline
	for (auto &parent_entry : parents) {
		auto parent = parent_entry.second.lock();
		if (!parent) {
			continue;
		}
		// mark a dependency as completed for each of the parents
		parent->CompleteDependency();
	}
	executor.completed_pipelines++;
}

string Pipeline::ToString() const {
	string str = PhysicalOperatorToString(sink->type);
	for (auto &op : operators) {
		str = PhysicalOperatorToString(op->type) + " -> " + str;
	}
	str = PhysicalOperatorToString(source->type) + " -> " + str;
	return str;
}

void Pipeline::Print() const {
	Printer::Print(ToString());
}

} // namespace duckdb
