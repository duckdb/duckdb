#include "duckdb/parallel/pipeline_initialize_event.hpp"

#include "duckdb/execution/executor.hpp"

namespace duckdb {

PipelineInitializeEvent::PipelineInitializeEvent(shared_ptr<Pipeline> pipeline_p)
    : BasePipelineEvent(std::move(pipeline_p)) {
}

class PipelineInitializeTask : public ExecutorTask {
public:
	explicit PipelineInitializeTask(Pipeline &pipeline_p, shared_ptr<Event> event_p)
	    : ExecutorTask(pipeline_p.executor, std::move(event_p)), pipeline(pipeline_p) {
	}

	Pipeline &pipeline;

public:
	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		pipeline.ResetSink();
		event->FinishTask();
		return TaskExecutionResult::TASK_FINISHED;
	}
};

void PipelineInitializeEvent::Schedule() {
	// needs to spawn a task to get the chain of tasks for the query plan going
	vector<shared_ptr<Task>> tasks;
	tasks.push_back(make_uniq<PipelineInitializeTask>(*pipeline, shared_from_this()));
	SetTasks(std::move(tasks));
}

void PipelineInitializeEvent::FinishEvent() {
}

} // namespace duckdb
