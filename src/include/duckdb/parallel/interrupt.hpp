//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/interrupt.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/parallel/task.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include <condition_variable>
#include <memory>

namespace duckdb {

//! InterruptMode specifies how operators should block/unblock, note that this will happen transparently to the
//! operator, as the operator only needs to return a BLOCKED result and call the callback using the InterruptState.
//! NO_INTERRUPTS: No blocking mode is specified, an error will be thrown when the operator blocks. Should only be used
//!                when manually calling operators of which is known they will never block.
//! TASK:          A weak pointer to a task is provided. On the callback, this task will be signalled. If the Task has
//!                been deleted, this callback becomes a NOP. This is the preferred way to await blocked pipelines.
//! BLOCKING:	   The caller has blocked awaiting some synchronization primitive to wait for the callback.
enum class InterruptMode : uint8_t { NO_INTERRUPTS, TASK, BLOCKING };

//! Synchronization primitive used to await a callback in InterruptMode::BLOCKING.
struct InterruptDoneSignalState {
	//! Called by the callback to signal the interrupt is over
	void Signal();
	//! Await the callback signalling the interrupt is over
	void Await();

protected:
	mutex lock;
	std::condition_variable cv;
	bool done = false;
};

//! State required to make the callback after some asynchronous operation within an operator source / sink.
class InterruptState {
public:
	//! Default interrupt state will be set to InterruptMode::NO_INTERRUPTS and throw an error on use of Callback()
	InterruptState();
	//! Register the task to be interrupted and set mode to InterruptMode::TASK, the preferred way to handle interrupts
	explicit InterruptState(weak_ptr<Task> task);
	//! Register signal state and set mode to InterruptMode::BLOCKING, used for code paths without Task.
	explicit InterruptState(weak_ptr<InterruptDoneSignalState> done_signal);

	//! Perform the callback to indicate the Interrupt is over
	DUCKDB_API void Callback() const;

protected:
	//! Current interrupt mode
	InterruptMode mode;
	//! Task ptr for InterruptMode::TASK
	weak_ptr<Task> current_task;
	//! Signal state for InterruptMode::BLOCKING
	weak_ptr<InterruptDoneSignalState> signal_state;
};

} // namespace duckdb
