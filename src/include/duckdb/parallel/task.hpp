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
#include <memory>

namespace duckdb {
class ClientContext;
class Executor;
class Task;
class DatabaseInstance;
struct ProducerToken;

enum class TaskExecutionMode : uint8_t { PROCESS_ALL, PROCESS_PARTIAL };

enum class TaskExecutionResult : uint8_t { TASK_FINISHED, TASK_NOT_FINISHED, TASK_ERROR, TASK_BLOCKED};

//! Types of interrupts TODO rename
enum class InterruptResultType {
	//! Default value when no interrupt has occured
	NO_INTERRUPT,

	//! A callback will be made to the scheduler with the specified uuid
	CALLBACK,
};

//! State that is passed to the asynchronous callback that signals task can be rescheduled
struct InterruptCallbackState {
	weak_ptr<Task> current_task;
	weak_ptr<DatabaseInstance> db; // TODO: remove?
};

//! State of an interrupt, allows the interrupting code to specify how the interrupt should be handled
struct InterruptState {
	InterruptState(ClientContext& context);
	InterruptResultType result = InterruptResultType::NO_INTERRUPT;

	void Reset() {
		result = InterruptResultType::NO_INTERRUPT;
		allow_async = true;
	}

	void SetInterruptCallback() {
		result = InterruptResultType::CALLBACK;
	}

	//! Get the state required
	InterruptCallbackState GetCallbackState();

	//! Make the interrupt callback, signals that the task from which the callback state was generated is ready to be
	//! rescheduled
	static void Callback(InterruptCallbackState callback_state);

	weak_ptr<Task> current_task;
	ClientContext& context;
	bool allow_async = true;
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
