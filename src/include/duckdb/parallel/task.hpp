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

namespace duckdb {
class ClientContext;
class Executor;
struct ProducerToken;

enum class TaskExecutionMode : uint8_t { PROCESS_ALL, PROCESS_PARTIAL };

enum class TaskExecutionResult : uint8_t { TASK_FINISHED, TASK_NOT_FINISHED, TASK_ERROR, TASK_BLOCKED};

//! Types of interrupts TODO move to parallel/interrupt.hpp
enum class InterruptResultType {
	//! Default value when no interrupt has occured
	NO_INTERRUPT,

	//! A callback will be made to the scheduler with the specified uuid
	CALLBACK,
};

//! State of an interrupt, allows the interrupting code to specify how the interrupt should be handled
struct InterruptState {
	InterruptResultType result = InterruptResultType::NO_INTERRUPT;
	hugeint_t callback_uuid;

	void Reset() {
		result = InterruptResultType::NO_INTERRUPT;
	}

	hugeint_t SetInterruptCallback() {
		callback_uuid = UUID::GenerateRandomUUID();
		result = InterruptResultType::CALLBACK;
		return callback_uuid;
	}
};

//! Generic parallel task
class Task {
public:
	virtual ~Task() {
	}

	//! Execute the task in the specified execution mode
	//! If mode is PROCESS_ALL, Execute should always finish processing and return TASK_FINISHED
	//! If mode is PROCESS_PARTIAL, Execute can return TASK_NOT_FINISHED, in which case Execute will be called again
	//! In case of an error, TASK_ERROR is returned
	virtual TaskExecutionResult Execute(TaskExecutionMode mode) = 0;

	//! Descheduling a task ensures the task remains available for rescheduling as long as required, generally until some
	//! external event calls the relevant callback for this task for it to be rescheduled.
	virtual void Deschedule() {
	    throw InternalException("Cannot deschedule task of base Task class");
	};

	//! While a task is running, it may set its interrupt state indicating to the scheduler how it wants to be handled
	//! after returning a TaskExecutionResult::TASK_BLOCKED
	InterruptState interrupt_state;

	//! We need to store the current producer token in case the task needs to be rescheduled
	shared_ptr<ProducerToken> current_token;
};

//! Execute a task within an executor, including exception handling
//! This should be used within queries
class ExecutorTask : public Task {
public:
	ExecutorTask(Executor &executor);
	ExecutorTask(ClientContext &context);
	virtual ~ExecutorTask();

	void Deschedule() override {
//		executor.Deschedule();
	};

	Executor &executor;

public:
	virtual TaskExecutionResult ExecuteTask(TaskExecutionMode mode) = 0;
	TaskExecutionResult Execute(TaskExecutionMode mode) override;
};

} // namespace duckdb
