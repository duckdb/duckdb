#include "duckdb/parallel/pipeline.hpp"

#include "duckdb/common/printer.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/parallel/task_scheduler.hpp"

using namespace std;

namespace duckdb {

class PipelineTask : public Task {
public:
	PipelineTask(Pipeline *pipeline_) :
	      pipeline(pipeline_) {
	}

	void Execute() override {
		pipeline->Execute();
		pipeline->FinishTask();
	}
private:
    Pipeline *pipeline;
};

Pipeline::Pipeline(Executor &executor_)
    : executor(executor_), finished_dependencies(0), finished(false), finished_tasks(0), total_tasks(1) {
}

void Pipeline::Execute() {
	assert(finished_dependencies == dependencies.size());

	auto &client = executor.context;
	if (client.interrupted) {
		return;
	}

	ThreadContext thread(client);
	ExecutionContext context(client, thread);
	try {
		auto state = child->GetOperatorState();
		auto lstate = sink->GetLocalSinkState(context);
		// incrementally process the pipeline
		DataChunk intermediate;
		child->InitializeChunk(intermediate);
		while (true) {
			child->GetChunk(context, intermediate, state.get());
			thread.profiler.StartOperator(sink);
			if (intermediate.size() == 0) {
				sink->Combine(context, *sink_state, *lstate);
				sink->Finalize(context, move(sink_state));
				break;
			}
			sink->Sink(context, *sink_state, *lstate, intermediate);
			thread.profiler.EndOperator(nullptr);
		}
	} catch (std::exception &ex) {
		executor.PushError(ex.what());
	} catch (...) {
		executor.PushError("Unknown exception!");
	}
	executor.Flush(thread);
}

void Pipeline::FinishTask() {
	idx_t current_finished = ++finished_tasks;
	if (current_finished == total_tasks) {
		Finish();
	}
}

void Pipeline::Schedule() {
	assert(finished_dependencies == dependencies.size());

	auto &scheduler = TaskScheduler::GetScheduler(executor.context);
	auto task = make_unique<PipelineTask>(this);

	scheduler.ScheduleTask(*executor.producer, move(task));
}

void Pipeline::AddDependency(Pipeline *pipeline) {
	this->dependencies.insert(pipeline);
	pipeline->parents.insert(this);
}

void Pipeline::CompleteDependency() {
	idx_t current_finished = ++finished_dependencies;
	if (current_finished == dependencies.size()) {
		// all dependencies have been completed: schedule the pipeline
		Schedule();
	}
}

void Pipeline::Finish() {
	assert(!finished);
	finished = true;
	// finished processing the pipeline, now we can schedule pipelines that depend on this pipeline
	for (auto &parent : parents) {
		// mark a dependency as completed for each of the parents
		parent->CompleteDependency();
	}
	executor.completed_pipelines++;
}

string Pipeline::ToString() const {
	string str = PhysicalOperatorToString(sink->type);
	auto node = this->child;
	while (node) {
		str = PhysicalOperatorToString(node->type) + " -> " + str;
		node = node->children[0].get();
	}
	return str;
}

void Pipeline::Print() const {
	Printer::Print(ToString());
}

} // namespace duckdb
