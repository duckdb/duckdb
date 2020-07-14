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
	PipelineTask(shared_ptr<Pipeline> pipeline_) :
	      in_progress(0), pipeline(move(pipeline_)) {
	}

	void Execute() override {
		if (in_progress++ > 0) {
			// already being worked on by another thread
			return;
		}

		pipeline->Execute();
		pipeline->FinishTask();
	}
private:
	atomic<idx_t> in_progress;
    shared_ptr<Pipeline> pipeline;
};

Pipeline::Pipeline(Executor &executor_)
    : executor(executor_), finished(false), finished_tasks(0), total_tasks(1) {
}

void Pipeline::Execute() {
	assert(dependencies.size() == 0);

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
	assert(!HasDependencies());

	vector<shared_ptr<Task>> tasks;
	tasks.push_back(make_shared<PipelineTask>(shared_from_this()));

	executor.ScheduleTasks(move(tasks));
}

void Pipeline::AddDependency(Pipeline *pipeline) {
	this->dependencies.insert(pipeline);
	pipeline->parents.insert(this);
}

void Pipeline::EraseDependency(Pipeline *pipeline) {
	lock_guard<mutex> plock(pipeline_lock);
	assert(dependencies.count(pipeline) == 1);

	dependencies.erase(dependencies.find(pipeline));
	if (dependencies.size() == 0) {
		// no more dependents: schedule this pipeline
		Schedule();
	}
}

void Pipeline::Finish() {
	assert(dependencies.size() == 0);
	assert(!finished);
	finished = true;
	// finished processing the pipeline, now we can schedule pipelines that depend on this pipeline
	for (auto &parent : parents) {
		// parent: remove this entry from the dependents
		parent->EraseDependency(this);
	}
	executor.ErasePipeline(this);
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
