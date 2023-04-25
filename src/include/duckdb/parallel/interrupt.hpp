//===----------------------------------------------------------------------===//
//                         DuckDB
//
// src/include/duckdb/parallel/interrupt.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/parallel/task.hpp"
#include <memory>

namespace duckdb {

//! InterruptMode specifies how operators should block/unblock, note that this will happen transparently to the operator,
//! as the operator only needs to return a BLOCKED result and call the callback using the InterruptState.
//! NO_INTERRUPTS: No blocking mode is specified, an error will be thrown when the operator blocks. Should only be used
//!				 when manually calling operators of which is known they will never block.
//! TASK:		 A weak pointer to a task is provided. On the callback, this task will be signalled.
//! BLOCKING:	 The caller has blocked awaiting
enum class InterruptMode : uint8_t { NO_INTERRUPTS, TASK, BLOCKING};

//! State required to make the callback after some async operation within an operator source / sink.
class InterruptState {
public:
	//! Default interrupt state will be set to InterruptMode::NO_INTERRUPTS and throw an error on use of Callback()
	InterruptState();
	//! Register the task to be interrupted and set mode to InterruptMode::TASK
	InterruptState(weak_ptr<Task> task);
	//! Register atomic done marker and set mode to InterruptMode::BLOCKING
	InterruptState(weak_ptr<atomic<bool>> done_marker);

	//! Perform the callback to indicate the Interrupt is over
	DUCKDB_API void Callback() const;

protected:
	//! Current interrupt mode
	InterruptMode mode;
	//! Task ptr for InterruptMode::TASK
	weak_ptr<Task> current_task;
	//! Marker ptr for InterruptMode::BLOCKING
	weak_ptr<atomic<bool>> done_marker;
};

}