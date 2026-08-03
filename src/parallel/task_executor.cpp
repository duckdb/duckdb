#include "duckdb/parallel/task_executor.hpp"
#include "duckdb/parallel/task_notifier.hpp"
#include "duckdb/parallel/task_scheduler.hpp"

namespace duckdb {

TaskExecutor::TaskExecutor(TaskScheduler &scheduler, TaskSchedulerType type_p)
    : scheduler(scheduler), type(type_p), token(scheduler.CreateProducer()) {
}

TaskExecutor::TaskExecutor(ClientContext &context_p, TaskSchedulerType type_p)
    : TaskExecutor(TaskScheduler::GetScheduler(context_p), type_p) {
	context = context_p;
}

TaskExecutor::~TaskExecutor() {
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

void TaskExecutor::ScheduleTask(unique_ptr<Task> task) {
	{
		const annotated_lock_guard<annotated_mutex> lock(token->producer_lock);
		++total_tasks;
	}
	try {
		scheduler.ScheduleTask(*token, std::move(task), type);
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

TaskExecutionResult BaseExecutorTask::Execute(TaskExecutionMode mode) {
	if (executor.HasError() || executor.cancelled) {
		// another task encountered an error or the executor was cancelled - bailout
		executor.FinishTask();
		return TaskExecutionResult::TASK_FINISHED;
	}
	try {
		{
			TaskNotifier task_notifier {executor.context};
			ExecuteTask();
		}
		executor.FinishTask();
		return TaskExecutionResult::TASK_FINISHED;
	} catch (std::exception &ex) {
		executor.PushError(ErrorData(ex));
	} catch (...) { // LCOV_EXCL_START
		executor.PushError(ErrorData("Unknown exception during Checkpoint!"));
	} // LCOV_EXCL_STOP
	executor.FinishTask();
	return TaskExecutionResult::TASK_ERROR;
}

} // namespace duckdb
