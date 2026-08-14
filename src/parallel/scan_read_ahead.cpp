#include "duckdb/parallel/scan_read_ahead.hpp"

#include "duckdb/function/table_function.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/task_scheduler.hpp"

namespace duckdb {

void ReadAheadJobCompletion::FinishIOTask() {
	const auto previous = pending_io_tasks.fetch_sub(1);
	D_ASSERT(previous > 0);
	if (previous > 1) {
		// I/O tasks still outstanding, nothing to wake yet
		return;
	}
	// wake the parked scan task, if any
	const annotated_lock_guard<annotated_mutex> guard {parked_scan.lock};
	parked_scan.UnblockTasks();
}

bool ReadAheadJobCompletion::TryPark(const InterruptState &interrupt_state) {
	// checking the pending count under the same lock FinishIOTask takes before waking prevents lost wake-ups
	const annotated_lock_guard<annotated_mutex> guard {parked_scan.lock};
	if (pending_io_tasks.load() == 0) {
		return false;
	}
	return parked_scan.BlockTask(interrupt_state);
}

bool ReadAheadJobCompletion::TryPark(TableFunctionInput &data_p) {
	if (data_p.results_execution_mode != AsyncResultsExecutionMode::TASK_EXECUTOR || !data_p.interrupt_state) {
		return false;
	}
	if (!TryPark(*data_p.interrupt_state)) {
		return false;
	}
	data_p.async_result = AsyncResultType::BLOCKED;
	return true;
}

void ScanReadAheadYield(ClientContext &context) {
	context.InterruptCheck();
	TaskScheduler::YieldThread();
}

void ReadAheadJobCompletion::WaitForIO() {
	shared_ptr<Task> task;
	while (pending_io_tasks.load() > 0) {
		// run scheduled I/O inline, the tasks may belong to any job but always make progress
		if (executor->GetTask(task)) {
			task->Execute(TaskExecutionMode::PROCESS_ALL);
			task.reset();
			continue;
		}
		// the remaining I/O is running on other threads, wait for it to finish
		TaskScheduler::YieldThread();
	}
}

} // namespace duckdb
