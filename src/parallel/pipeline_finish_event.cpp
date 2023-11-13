#include "duckdb/parallel/pipeline_finish_event.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/parallel/interrupt.hpp"

namespace duckdb {

//! The PipelineFinishTask calls Finalize on the sink. Note that this is a single-threaded operation, but is executed
//! in a task to allow the Finalize call to block (e.g. for async I/O)
class PipelineFinishTask : public ExecutorTask {
public:
	explicit PipelineFinishTask(Pipeline &pipeline_p, shared_ptr<Event> event_p)
	    : ExecutorTask(pipeline_p.executor), pipeline(pipeline_p), event(std::move(event_p)) {
	}

	Pipeline &pipeline;
	shared_ptr<Event> event;

public:
	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		auto sink = pipeline.GetSink();
		InterruptState interrupt_state(shared_from_this());
		OperatorSinkFinalizeInput finalize_input {*sink->sink_state, interrupt_state};

#ifdef DUCKDB_DEBUG_ASYNC_SINK_SOURCE
		if (debug_blocked_count < debug_blocked_target_count) {
			debug_blocked_count++;

			auto &callback_state = interrupt_state;
			std::thread rewake_thread([callback_state] {
				std::this_thread::sleep_for(std::chrono::milliseconds(1));
				callback_state.Callback();
			});
			rewake_thread.detach();

			return TaskExecutionResult::TASK_BLOCKED;
		}
#endif
		auto sink_state = sink->Finalize(pipeline, *event, executor.context, finalize_input);

		if (sink_state == SinkFinalizeType::BLOCKED) {
			return TaskExecutionResult::TASK_BLOCKED;
		}

		sink->sink_state->state = sink_state;
		event->FinishTask();
		return TaskExecutionResult::TASK_FINISHED;
	}

private:
#ifdef DUCKDB_DEBUG_ASYNC_SINK_SOURCE
	//! Debugging state: number of times blocked
	int debug_blocked_count = 0;
	//! Number of times the Finalize will block before actually returning data
	int debug_blocked_target_count = 10;
#endif
};

PipelineFinishEvent::PipelineFinishEvent(shared_ptr<Pipeline> pipeline_p) : BasePipelineEvent(std::move(pipeline_p)) {
}

void PipelineFinishEvent::Schedule() {
	vector<shared_ptr<Task>> tasks;
	tasks.push_back(make_uniq<PipelineFinishTask>(*pipeline, shared_from_this()));
	SetTasks(std::move(tasks));
}

void PipelineFinishEvent::FinishEvent() {
}

} // namespace duckdb
