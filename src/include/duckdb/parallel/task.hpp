//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/task.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/atomic.hpp"
#include <memory>

namespace duckdb {
class ClientContext;
class Executor;
class Task;
class DatabaseInstance;
struct ProducerToken;

enum class TaskExecutionMode : uint8_t { PROCESS_ALL, PROCESS_PARTIAL };

enum class TaskExecutionResult : uint8_t { TASK_FINISHED, TASK_NOT_FINISHED, TASK_ERROR, TASK_BLOCKED};

//! TODO: this shit should be moved?
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

//! Generic parallel task
class Task : public std::enable_shared_from_this<Task> {
public:
	virtual ~Task() {
	}

	//! Execute the task in the specified execution mode
	//! If mode is PROCESS_ALL, Execute should always finish processing and return TASK_FINISHED
	//! If mode is PROCESS_PARTIAL, Execute can return TASK_NOT_FINISHED, in which case Execute will be called again
	//! In case of an error, TASK_ERROR is returned
	//! In case the task has interrupted, BLOCKED is returned. TODO: should execute call Task->Deschedule itself?
	virtual TaskExecutionResult Execute(TaskExecutionMode mode) = 0;

	//! Descheduling a task ensures the task remains available for rescheduling as long as required, generally until some
	//! external event calls the relevant callback for this task for it to be rescheduled.
	virtual void Deschedule() {
	    throw InternalException("Cannot deschedule task of base Task class");
	};

	//! Ensures a task is rescheduled to the correct queue
	virtual void Reschedule() {
		throw InternalException("Cannot reschedule task of base Task class");
	}
};

//! Execute a task within an executor, including exception handling
//! This should be used within queries
class ExecutorTask : public Task {
public:
	ExecutorTask(Executor &executor);
	ExecutorTask(ClientContext &context);
	virtual ~ExecutorTask();

	void Deschedule() override;
	void Reschedule() override;

	Executor &executor;

public:
	virtual TaskExecutionResult ExecuteTask(TaskExecutionMode mode) = 0;
	TaskExecutionResult Execute(TaskExecutionMode mode) override;
};

} // namespace duckdb
