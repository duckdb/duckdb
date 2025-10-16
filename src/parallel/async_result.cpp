#include "duckdb/parallel/executor_task.hpp"
#include "duckdb/parallel/async_result.hpp"
#include "duckdb/parallel/interrupt.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/execution/executor.hpp"

namespace duckdb {

class AsyncExecutionTask : public ExecutorTask {
public:
	AsyncExecutionTask(Executor &executor, unique_ptr<AsyncTask> &&async_task, InterruptState &interrupt_state)
	    : ExecutorTask(executor, nullptr), async_task(std::move(async_task)), interrupt_state(interrupt_state) {
	}
	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		async_task->Execute();
		interrupt_state.Callback();
		return TaskExecutionResult::TASK_FINISHED;
	}

	string TaskType() const override {
		return "AsyncTask";
	}

private:
	unique_ptr<AsyncTask> async_task;
	InterruptState interrupt_state;
};

AsyncResultType::AsyncResultType(SourceResultType t) : result_type(t) {
	D_ASSERT(result_type != SourceResultType::BLOCKED);
}

AsyncResultType::AsyncResultType(unique_ptr<AsyncTask> &&task)
    : result_type(SourceResultType::BLOCKED), async_task(std::move(task)) {
}

void AsyncResultType::ScheduleTasks(InterruptState &interrupt_state, Executor &executor) {
	D_ASSERT(result_type == SourceResultType::BLOCKED);

	auto task = make_uniq<AsyncExecutionTask>(executor, std::move(async_task), interrupt_state);
	//	executor.ScheduleTask(std::move(task));
	TaskScheduler::GetScheduler(executor.context).ScheduleTask(executor.GetToken(), std::move(task));
}

} // namespace duckdb
