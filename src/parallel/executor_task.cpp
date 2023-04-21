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

InterruptState::InterruptState(ClientContext &context) : context(context) {}

InterruptCallbackState InterruptState::GetCallbackState() {
	result = InterruptResultType::CALLBACK;
	throw InternalException("NO CALLBACKS FOR TESTING");
	return {current_task, context.db};
}

void InterruptState::Callback(InterruptCallbackState callback_state) {
	//! Check if db and task are still alive and kicking
	auto db = callback_state.db.lock();
	auto task = callback_state.current_task.lock();

	throw InternalException("NO CALLBACKS FOR TESTING");

	if (!db || !task) {
		throw InternalException("NO TASK OR DB");
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
