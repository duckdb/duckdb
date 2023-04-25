#include "duckdb/parallel/task.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/atomic.hpp"

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

InterruptState::InterruptState() : mode(InterruptMode::NO_INTERRUPTS){}
InterruptState::InterruptState(weak_ptr<Task> task) : mode(InterruptMode::TASK), current_task(std::move(task)) {};
InterruptState::InterruptState(weak_ptr<atomic<bool>> done_marker_p) : mode(InterruptMode::BLOCKING), done_marker(std::move(done_marker_p)) {};

void InterruptState::Callback() const {
	if (mode == InterruptMode::TASK) {
		auto task = current_task.lock();

		if (!task) {
			return;
		}

		task->Reschedule();
	}  else if (mode == InterruptMode::BLOCKING) {
		auto marker = done_marker.lock();

		if (!marker) {
			return;
		}

		*marker = true;
	} else {
		throw InternalException("Callback made on InterruptState without valid interrupt mode specified");
	}
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
