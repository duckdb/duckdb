#include "duckdb/parallel/task.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/thread_context.hpp"

namespace duckdb {

ExecutorTask::ExecutorTask(Executor &executor_p, shared_ptr<Event> event_p)
    : executor(executor_p), event(std::move(event_p)) {
	executor.RegisterTask();
}

ExecutorTask::ExecutorTask(ClientContext &context, shared_ptr<Event> event_p, const PhysicalOperator &op_p)
    : executor(Executor::Get(context)), event(std::move(event_p)), op(&op_p) {
	thread_context = make_uniq<ThreadContext>(context);
	executor.RegisterTask();
}

ExecutorTask::~ExecutorTask() {
	if (thread_context) {
		executor.Flush(*thread_context);
	}
	executor.UnregisterTask();
}

void ExecutorTask::Deschedule() {
	auto this_ptr = shared_from_this();
	executor.AddToBeRescheduled(this_ptr);
}

void ExecutorTask::Reschedule() {
	auto this_ptr = shared_from_this();
	executor.RescheduleTask(this_ptr);
}

TaskExecutionResult ExecutorTask::Execute(TaskExecutionMode mode) {
	try {
		if (thread_context) {
			thread_context->profiler.StartOperator(op);
			auto result = ExecuteTask(mode);
			thread_context->profiler.EndOperator(nullptr);
			return result;
		} else {
			return ExecuteTask(mode);
		}
	} catch (std::exception &ex) {
		executor.PushError(ErrorData(ex));
	} catch (...) { // LCOV_EXCL_START
		executor.PushError(ErrorData("Unknown exception in Finalize!"));
	} // LCOV_EXCL_STOP
	return TaskExecutionResult::TASK_ERROR;
}

} // namespace duckdb
