#include "duckdb/parallel/task_executor.hpp"
#include "duckdb/parallel/task_scheduler.hpp"

namespace duckdb {

TaskExecutor::TaskExecutor(TaskScheduler &scheduler)
    : scheduler(scheduler), token(scheduler.CreateProducer()), completed_tasks(0), total_tasks(0) {
}

TaskExecutor::TaskExecutor(ClientContext &context) : TaskExecutor(TaskScheduler::GetScheduler(context)) {
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
	++total_tasks;
	scheduler.ScheduleTask(*token, std::move(task));
}
void TaskExecutor::FinishTask() {
	++completed_tasks;
}

void TaskExecutor::WorkOnTasks() {
	// repeatedly execute tasks until we are finished
	shared_ptr<Task> task_from_producer;
	while (scheduler.GetTaskFromProducer(*token, task_from_producer)) {
		auto res = task_from_producer->Execute(TaskExecutionMode::PROCESS_ALL);
		(void)res;
		D_ASSERT(res != TaskExecutionResult::TASK_BLOCKED);
		task_from_producer.reset();
	}
	// wait for all active tasks to finish
	while (completed_tasks != total_tasks) {
	}

	// check if we ran into any errors while checkpointing
	if (HasError()) {
		// throw the error
		ThrowError();
	}
}

bool TaskExecutor::GetTask(shared_ptr<Task> &task) {
	return scheduler.GetTaskFromProducer(*token, task);
}

BaseExecutorTask::BaseExecutorTask(TaskExecutor &executor) : executor(executor) {
}

TaskExecutionResult BaseExecutorTask::Execute(TaskExecutionMode mode) {
	(void)mode;
	D_ASSERT(mode == TaskExecutionMode::PROCESS_ALL);
	if (executor.HasError()) {
		// another task encountered an error - bailout
		executor.FinishTask();
		return TaskExecutionResult::TASK_FINISHED;
	}
	try {
		ExecuteTask();
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
