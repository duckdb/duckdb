#include "duckdb/parallel/interrupt.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/atomic.hpp"

namespace duckdb {

InterruptState::InterruptState() : mode(InterruptMode::NO_INTERRUPTS) {
}
InterruptState::InterruptState(weak_ptr<Task> task) : mode(InterruptMode::TASK), current_task(std::move(task)) {};
InterruptState::InterruptState(weak_ptr<atomic<bool>> done_marker_p)
    : mode(InterruptMode::BLOCKING), done_marker(std::move(done_marker_p)) {};

void InterruptState::Callback() const {
	if (mode == InterruptMode::TASK) {
		auto task = current_task.lock();

		if (!task) {
			return;
		}

		task->Reschedule();
	} else if (mode == InterruptMode::BLOCKING) {
		auto marker = done_marker.lock();

		if (!marker) {
			return;
		}

		*marker = true;
	} else {
		throw InternalException("Callback made on InterruptState without valid interrupt mode specified");
	}
}

} // namespace duckdb
