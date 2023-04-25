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
	// Register the Descheduled task at the executor, ensuring the Task is kept alive while the executor is
//	Printer::Print("Deschedule task " + to_string((int64_t)((void*)this)));
	executor.AddToBeRescheduled(shared_from_this());
};

void ExecutorTask::Reschedule() {
//	Printer::Print("Reschedule task " + to_string((int64_t)((void*)this)));
	// Register the Descheduled task at the executor, ensuring the Task is kept alive while the executor is
	executor.RescheduleTask(shared_from_this());
};

InterruptState::InterruptState() {}

InterruptCallbackState InterruptState::GetCallbackState() {
#ifdef DEBUG
	if (!current_task.lock()) {
		throw InternalException("GetCallbackState called on interrupt state without current_task pointer");
	}
#endif
	return {current_task};
}

void InterruptState::Callback(InterruptCallbackState callback_state) {
	//! Check if db and task are still alive and kicking
	auto task = callback_state.current_task.lock();

	if (!task) {
		return;
	}

	task->Reschedule();
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
