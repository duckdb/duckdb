#include "duckdb/parallel/task_executor.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/task_notifier.hpp"
#include "duckdb/parallel/task_scheduler.hpp"

#include <chrono>

namespace duckdb {

TaskExecutor::TaskExecutor(TaskScheduler &scheduler, TaskSchedulerType type_p, TaskExecutorMode mode_p,
                           QueryContext query_context)
    : scheduler(scheduler), type(type_p), mode(mode_p), token(scheduler.CreateProducer()),
      context(query_context.GetClientContext()) {
}

TaskExecutor::TaskExecutor(ClientContext &context_p, TaskSchedulerType type_p, TaskExecutorMode mode_p)
    : TaskExecutor(TaskScheduler::GetScheduler(context_p), type_p, mode_p) {
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
		if (mode == TaskExecutorMode::JOINED && !parked_task) {
			// the joining thread has to wait for the other tasks anyway - park this one for it to execute
			parked_task = std::move(task);
			return;
		}
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
	// execute the parked task (if any) on this thread - if we are cancelling we discard it instead
	unique_ptr<Task> parked;
	{
		const annotated_lock_guard<annotated_mutex> lock(token->producer_lock);
		if (parked_task && !cancelled) {
			parked = std::move(parked_task);
			++total_tasks;
		} else {
			parked_task.reset();
		}
	}
	if (parked) {
		// Execute finishes the task, also when it bails out because another task has errored
		parked->Execute(TaskExecutionMode::PROCESS_ALL);
		parked.reset();
	}

	// wait for all active tasks to finish, executing queued tasks on this thread where possible
	static constexpr std::chrono::milliseconds INTERRUPT_CHECK_INTERVAL = std::chrono::milliseconds(20);

	shared_ptr<Task> task_from_producer;
	while (true) {
		bool waited = false;
		{
			annotated_unique_lock<annotated_mutex> lk(token->producer_lock);
			if (completed_tasks == total_tasks) {
				break;
			}
			if (!scheduler.GetTaskFromProducerLocked(*token, task_from_producer)) {
				// wait for a bounded time, so that we can check for interruption in between waits
				token->producer_cv.wait_for(lk, INTERRUPT_CHECK_INTERVAL);
				waited = true;
			}
		}
		if (waited) {
			// a drain that cancels must always run to completion, so it is never allowed to throw
			if (!cancelled && context) {
				context->InterruptCheck();
			}
			continue;
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
