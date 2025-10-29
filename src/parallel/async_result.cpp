#include "duckdb/parallel/executor_task.hpp"
#include "duckdb/parallel/async_result.hpp"
#include "duckdb/parallel/interrupt.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/execution/executor.hpp"

namespace duckdb {

struct Counter {
	explicit Counter(idx_t size) : counter(size) {
	}
	bool IterateAndCheckCounter() {
		D_ASSERT(counter.load() > 0);
		idx_t post_decreast = --counter;
		return (post_decreast == 0);
	}

private:
	atomic<idx_t> counter;
};

class AsyncExecutionTask : public ExecutorTask {
public:
	AsyncExecutionTask(Executor &executor, unique_ptr<AsyncTask> &&async_task, InterruptState &interrupt_state,
	                   shared_ptr<Counter> counter)
	    : ExecutorTask(executor, nullptr), async_task(std::move(async_task)), interrupt_state(interrupt_state),
	      counter(std::move(counter)) {
	}
	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		async_task->Execute();
		if (counter->IterateAndCheckCounter()) {
			interrupt_state.Callback();
		}
		return TaskExecutionResult::TASK_FINISHED;
	}

	string TaskType() const override {
		return "AsyncTask";
	}

private:
	unique_ptr<AsyncTask> async_task;
	InterruptState interrupt_state;
	shared_ptr<Counter> counter;
};

AsyncResult::AsyncResult(SourceResultType t) : AsyncResult(GetAsyncResultType(t)) {
}

AsyncResult::AsyncResult(AsyncResultType t) : result_type(t) {
	if (result_type == AsyncResultType::BLOCKED) {
		throw InternalException("AsyncResult constructed with a BLOCKED state, do provide AsyncTasks");
	}
}

AsyncResult::AsyncResult(vector<unique_ptr<AsyncTask>> &&tasks)
    : result_type(AsyncResultType::BLOCKED), async_tasks(std::move(tasks)) {
	if (async_tasks.empty()) {
		throw InternalException("AsyncResult constructed from empty vector of tasks");
	}
}

AsyncResult &AsyncResult::operator=(duckdb::SourceResultType t) {
	return operator=(AsyncResult(t));
}

AsyncResult &AsyncResult::operator=(duckdb::AsyncResultType t) {
	return operator=(AsyncResult(t));
}

AsyncResult &AsyncResult::operator=(AsyncResult &&other) noexcept {
	result_type = other.result_type;
	async_tasks = std::move(other.async_tasks);
	return *this;
}

void AsyncResult::ScheduleTasks(InterruptState &interrupt_state, Executor &executor) {
	D_ASSERT(result_type == AsyncResultType::BLOCKED);

	if (async_tasks.empty()) {
		throw InternalException("AsyncResultType with no task found");
	}

	shared_ptr<Counter> counter = make_shared_ptr<Counter>(async_tasks.size());

	for (auto &async_task : async_tasks) {
		auto task = make_uniq<AsyncExecutionTask>(executor, std::move(async_task), interrupt_state, counter);
		TaskScheduler::GetScheduler(executor.context).ScheduleTask(executor.GetToken(), std::move(task));
	}
}

void AsyncResult::ExecuteTasksSyncronously() {
	D_ASSERT(result_type == AsyncResultType::BLOCKED);

	if (async_tasks.empty()) {
		throw InternalException("AsyncResultType with no task found");
	}

	for (auto &async_task : async_tasks) {
		async_task->Execute();
	}

	async_tasks.clear();

	result_type = AsyncResultType::HAVE_MORE_OUTPUT;
}

AsyncResultType AsyncResult::GetAsyncResultType(SourceResultType s) {
	switch (s) {
	case SourceResultType::HAVE_MORE_OUTPUT:
		return AsyncResultType::HAVE_MORE_OUTPUT;
	case SourceResultType::FINISHED:
		return AsyncResultType::FINISHED;
	case SourceResultType::BLOCKED:
		return AsyncResultType::BLOCKED;
	}
	throw InternalException("GetAsyncResultType has an unexpected input");
}

} // namespace duckdb
