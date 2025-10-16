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

AsyncResultType::AsyncResultType(vector<unique_ptr<AsyncTask>> &&tasks)
    : result_type(SourceResultType::BLOCKED), async_tasks(std::move(tasks)) {
}

void AsyncResultType::ScheduleTasks(InterruptState &interrupt_state, Executor &executor) {
	D_ASSERT(result_type == SourceResultType::BLOCKED);

	if (async_tasks.size() > 1) {
		throw InternalException("AsyncResultType with more that 1 task found");
	} else if (async_tasks.empty()) {
		throw InternalException("AsyncResultType with no task found");
	}

	auto task = make_uniq<AsyncExecutionTask>(executor, std::move(async_tasks[0]), interrupt_state);
	TaskScheduler::GetScheduler(executor.context).ScheduleTask(executor.GetToken(), std::move(task));
}

} // namespace duckdb
