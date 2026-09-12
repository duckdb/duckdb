#include "duckdb/parallel/task_executor.hpp"
#include "duckdb/parallel/task_notifier.hpp"
#include "duckdb/parallel/task_scheduler.hpp"

namespace duckdb {

//! The wrapper the executor puts around every scheduled task
//! It owns the cancel check, the task notifier, the error handling and the task accounting, so that none of those
//! depend on what the task itself does
class TaskExecutorTask : public Task {
public:
	TaskExecutorTask(TaskExecutor &executor, unique_ptr<BaseExecutorTask> task_p)
	    : executor(executor), task(std::move(task_p)) {
	}

public:
	TaskExecutionResult Execute(TaskExecutionMode mode) override {
		FinishGuard guard(executor);
		if (executor.HasError() || executor.IsCancelled()) {
			// another task encountered an error, or the executor was cancelled - retire without doing the work
			return RunGuarded([&]() { task->Cancel(); }, "Unknown exception while cancelling a task");
		}
		TaskNotifier task_notifier {executor.context};
		return RunGuarded([&]() { task->ExecuteTask(); }, "Unknown exception during task execution");
	}

	void Deschedule() override {
		throw InternalException("Tasks scheduled on a TaskExecutor cannot be descheduled");
	}

	void Reschedule() override {
		throw InternalException("Tasks scheduled on a TaskExecutor cannot be rescheduled");
	}

	string TaskType() const override {
		return task->TaskType();
	}

private:
	//! Settles the executor's task counter on every exit path, so that a drain always terminates
	class FinishGuard {
	public:
		explicit FinishGuard(TaskExecutor &executor) : executor(executor) {
		}
		~FinishGuard() {
			executor.FinishTask();
		}

	private:
		TaskExecutor &executor;
	};

	template <class FUNC>
	TaskExecutionResult RunGuarded(FUNC &&callback, const char *unknown_error) {
		try {
			callback();
		} catch (std::exception &ex) {
			executor.PushError(ErrorData(ex));
			return TaskExecutionResult::TASK_ERROR;
		} catch (...) { // LCOV_EXCL_START
			executor.PushError(ErrorData(unknown_error));
			return TaskExecutionResult::TASK_ERROR;
		} // LCOV_EXCL_STOP
		return TaskExecutionResult::TASK_FINISHED;
	}

private:
	TaskExecutor &executor;
	unique_ptr<BaseExecutorTask> task;
};

TaskExecutor::TaskExecutor(TaskScheduler &scheduler, TaskSchedulerType type_p)
    : scheduler(scheduler), type(type_p), token(scheduler.CreateProducer()) {
}

TaskExecutor::TaskExecutor(ClientContext &context_p, TaskSchedulerType type_p)
    : TaskExecutor(TaskScheduler::GetScheduler(context_p), type_p) {
	context = context_p;
}

TaskExecutor::~TaskExecutor() {
	// tasks can still be queued if we unwound between scheduling them and draining them
	// they hold a reference to this executor, so they must not outlive it
	try {
		CancelAndDrain();
	} catch (...) { // NOLINT
	}
}

void TaskExecutor::PushError(ErrorData error) {
	error_manager.PushError(std::move(error));
}

bool TaskExecutor::HasError() {
	return error_manager.HasError();
}

void TaskExecutor::ThrowError() {
	error_manager.ThrowException();
}

bool TaskExecutor::IsCancelled() const {
	return cancelled;
}

void TaskExecutor::ScheduleTask(unique_ptr<BaseExecutorTask> task) {
	// wrap before taking ownership of a slot, so that a failure to allocate the wrapper needs no rollback
	auto scheduled_task = make_uniq<TaskExecutorTask>(*this, std::move(task));
	{
		const annotated_lock_guard<annotated_mutex> lock(token->producer_lock);
		++total_tasks;
	}
	try {
		scheduler.ScheduleTask(*token, std::move(scheduled_task), type);
	} catch (...) {
		const annotated_lock_guard<annotated_mutex> lock(token->producer_lock);
		// We failed to schedule the task, so we decrement the total number of tasks, instead of incrementing completed
		// tasks count.
		--total_tasks;
		token->producer_cv.notify_one();
		throw;
	}
}
void TaskExecutor::FinishTask() {
	const annotated_lock_guard<annotated_mutex> lk(token->producer_lock);
	++completed_tasks;
	token->producer_cv.notify_one();
}

void TaskExecutor::DrainTasks() {
	// wait for all active tasks to finish, executing queued tasks on this thread where possible
	shared_ptr<Task> task_from_producer;
	while (true) {
		{
			annotated_unique_lock<annotated_mutex> lk(token->producer_lock);
			if (completed_tasks == total_tasks) {
				break;
			}
			if (!scheduler.GetTaskFromProducerLocked(*token, task_from_producer)) {
				token->producer_cv.wait(lk);
				continue;
			}
		}

		const auto res = task_from_producer->Execute(TaskExecutionMode::PROCESS_ALL);
		std::ignore = res;
		D_ASSERT(res != TaskExecutionResult::TASK_BLOCKED);
		task_from_producer.reset();
	}
}

void TaskExecutor::WorkOnTasks() {
	DrainTasks();

	// check if we ran into any errors while executing the tasks
	if (HasError()) {
		// throw the error
		ThrowError();
	}
}

void TaskExecutor::CancelAndDrain() {
	// make tasks that have not started yet bail out instead of executing their work
	cancelled = true;
	DrainTasks();
}

bool TaskExecutor::GetTask(shared_ptr<Task> &task) {
	return scheduler.GetTaskFromProducer(*token, task);
}

BaseExecutorTask::BaseExecutorTask(TaskExecutor &executor) : executor(executor) {
}

} // namespace duckdb
