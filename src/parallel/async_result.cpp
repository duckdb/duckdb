#include "duckdb/parallel/executor_task.hpp"
#include "duckdb/parallel/async_result.hpp"
#include "duckdb/parallel/interrupt.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/execution/physical_table_scan_enum.hpp"
#include "duckdb/logging/log_type.hpp"
#include "duckdb/logging/logger.hpp"

#ifdef DUCKDB_DEBUG_ASYNC_SINK_SOURCE
#include "duckdb/parallel/sleep_async_task.hpp"
#endif

namespace duckdb {

struct AsyncBatchCompletion {
	explicit AsyncBatchCompletion(idx_t size) : counter(size), callback_sent(false) {
	}

	bool IterateAndCheckCounter() {
		D_ASSERT(counter.load() > 0);
		idx_t post_decreast = --counter;
		return (post_decreast == 0);
	}

	bool MarkCallbackSent() {
		bool expected = false;
		return callback_sent.compare_exchange_strong(expected, true);
	}

private:
	atomic<idx_t> counter;
	atomic<bool> callback_sent;
};

class AsyncExecutionTask : public ExecutorTask {
	enum class CompletionSignal { BATCH_FINISHED, BATCH_ERRORED };

public:
	AsyncExecutionTask(Executor &executor, unique_ptr<AsyncTask> &&async_task, InterruptState &interrupt_state,
	                   shared_ptr<AsyncBatchCompletion> completion)
	    : ExecutorTask(executor, nullptr), async_task(std::move(async_task)), interrupt_state(interrupt_state),
	      completion(std::move(completion)) {
	}
	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		try {
			async_task->Execute();
		} catch (...) {
			SignalCompletion(CompletionSignal::BATCH_ERRORED);
			throw;
		}
		SignalCompletion(CompletionSignal::BATCH_FINISHED);
		return TaskExecutionResult::TASK_FINISHED;
	}

	string TaskType() const override {
		return "AsyncTask";
	}

private:
	void SignalCompletion(CompletionSignal signal) {
		auto finished = completion->IterateAndCheckCounter();
		if ((signal == CompletionSignal::BATCH_ERRORED || finished)) {
			SendCallback();
		}
	}

	void SendCallback() {
		if (completion->MarkCallbackSent()) {
			interrupt_state.Callback();
		}
	}

	unique_ptr<AsyncTask> async_task;
	InterruptState interrupt_state;
	shared_ptr<AsyncBatchCompletion> completion;
};

AsyncResult::AsyncResult(SourceResultType t) : AsyncResult(GetAsyncResultType(t)) {
}

AsyncResult::AsyncResult(AsyncResultType t) : result_type(t) {
	if (result_type == AsyncResultType::BLOCKED) {
		throw InternalException("AsyncResult constructed with a BLOCKED state, do provide AsyncTasks");
	}
}

AsyncResult::AsyncResult(vector<unique_ptr<AsyncTask>> &&tasks, TaskSchedulerType pool_type_p)
    : result_type(AsyncResultType::BLOCKED), async_tasks(std::move(tasks)), pool_type(pool_type_p) {
	if (async_tasks.empty()) {
		throw InternalException("AsyncResult constructed from empty vector of tasks");
	}
}

AsyncResult &AsyncResult::operator=(duckdb::SourceResultType t) {
	result_type = GetAsyncResultType(t);
	async_tasks.clear();
	return *this;
}

AsyncResult &AsyncResult::operator=(duckdb::AsyncResultType t) {
	result_type = t;
	async_tasks.clear();
	return *this;
}

AsyncResult &AsyncResult::operator=(AsyncResult &&other) noexcept {
	result_type = other.result_type;
	async_tasks = std::move(other.async_tasks);
	pool_type = other.pool_type;
	return *this;
}

void AsyncResult::ScheduleTasks(InterruptState &interrupt_state, Executor &executor) {
	if (result_type != AsyncResultType::BLOCKED) {
		throw InternalException("AsyncResult::ScheduleTasks called on non BLOCKED AsyncResult");
	}

	if (async_tasks.empty()) {
		throw InternalException("AsyncResult::ScheduleTasks called with no available tasks");
	}

	DUCKDB_LOG(executor.context, AsyncTaskScheduleLogType, EnumUtil::ToString(pool_type), async_tasks.size());

	shared_ptr<AsyncBatchCompletion> completion = make_shared_ptr<AsyncBatchCompletion>(async_tasks.size());

	for (auto &async_task : async_tasks) {
		auto task = make_uniq<AsyncExecutionTask>(executor, std::move(async_task), interrupt_state, completion);
		TaskScheduler::GetScheduler(executor.context).ScheduleTask(executor.GetToken(), std::move(task), pool_type);
	}
}

void AsyncResult::ExecuteTasksSynchronously() {
	if (result_type != AsyncResultType::BLOCKED) {
		throw InternalException("AsyncResult::ExecuteTasksSynchronously called on non BLOCKED AsyncResult");
	}

	if (async_tasks.empty()) {
		throw InternalException("AsyncResult::ExecuteTasksSynchronously called with no available tasks");
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

bool AsyncResult::HasTasks() const {
	D_ASSERT(result_type != AsyncResultType::INVALID);
	if (async_tasks.empty()) {
		D_ASSERT(result_type != AsyncResultType::BLOCKED);
		return false;
	} else {
		D_ASSERT(result_type == AsyncResultType::BLOCKED);
		return true;
	}
}
AsyncResultType AsyncResult::GetResultType() const {
	D_ASSERT(result_type != AsyncResultType::INVALID);
	if (async_tasks.empty()) {
		D_ASSERT(result_type != AsyncResultType::BLOCKED);
	} else {
		D_ASSERT(result_type == AsyncResultType::BLOCKED);
	}
	return result_type;
}
vector<unique_ptr<AsyncTask>> &&AsyncResult::ExtractAsyncTasks() {
	D_ASSERT(result_type != AsyncResultType::INVALID);
	result_type = AsyncResultType::INVALID;
	return std::move(async_tasks);
}

#ifdef DUCKDB_DEBUG_ASYNC_SINK_SOURCE
vector<unique_ptr<AsyncTask>> AsyncResult::GenerateTestTasks() {
	vector<unique_ptr<AsyncTask>> tasks;
	auto random_number = rand() % 16;
	switch (random_number) {
	case 0:
		tasks.push_back(make_uniq<SleepAsyncTask>(rand() % 32));
		tasks.push_back(make_uniq<SleepAsyncTask>(rand() % 32));
		tasks.push_back(make_uniq<SleepAsyncTask>(rand() % 32));
		tasks.push_back(make_uniq<SleepAsyncTask>(rand() % 32));
		tasks.push_back(make_uniq<SleepAsyncTask>(rand() % 32));
		tasks.push_back(make_uniq<SleepAsyncTask>(rand() % 32));
		tasks.push_back(make_uniq<SleepAsyncTask>(rand() % 32));
		tasks.push_back(make_uniq<SleepAsyncTask>(rand() % 32));
#ifndef AVOID_DUCKDB_DEBUG_ASYNC_THROW
	case 1:
		tasks.push_back(make_uniq<ThrowAsyncTask>(rand() % 32));
#endif
	default:
		break;
	}
	return tasks;
}
#endif

AsyncResultsExecutionMode
AsyncResult::ConvertToAsyncResultExecutionMode(const PhysicalTableScanExecutionStrategy &execution_mode) {
	switch (execution_mode) {
	case PhysicalTableScanExecutionStrategy::DEFAULT:
	case PhysicalTableScanExecutionStrategy::TASK_EXECUTOR:
	case PhysicalTableScanExecutionStrategy::TASK_EXECUTOR_BUT_FORCE_SYNC_CHECKS:
		return AsyncResultsExecutionMode::TASK_EXECUTOR;
	case PhysicalTableScanExecutionStrategy::SYNCHRONOUS:
		return AsyncResultsExecutionMode::SYNCHRONOUS;
	}
	throw InternalException("ConvertToAsyncResultExecutionMode passed an unexpected execution_mode");
}

} // namespace duckdb
