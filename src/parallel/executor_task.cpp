#include "duckdb/parallel/task.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

ExecutorTask::ExecutorTask(Executor &executor_p) : executor(executor_p) {
}

ExecutorTask::ExecutorTask(ClientContext &context) : ExecutorTask(Executor::Get(context)) {
}

ExecutorTask::~ExecutorTask() {
}

void ExecutorTask::Deschedule() {
	executor.AddToBeRescheduled(shared_from_this());
}

void ExecutorTask::Reschedule() {
	executor.RescheduleTask(shared_from_this());
}

TaskExecutionResult ExecutorTask::Execute(TaskExecutionMode mode) {
	try {
		return ExecuteTask(mode);
	} catch (Exception &ex) {
		executor.PushError(PreservedError(ex));
	} catch (std::exception &ex) {
		executor.PushError(PreservedError(ex));
	} catch (...) { // LCOV_EXCL_START
		executor.PushError(PreservedError("Unknown exception in Finalize!"));
	} // LCOV_EXCL_STOP
	return TaskExecutionResult::TASK_ERROR;
}

} // namespace duckdb
