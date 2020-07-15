//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/execution_context.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {
class ClientContext;
class ThreadContext;
class TaskContext;

class ExecutionContext {
public:
	ExecutionContext(ClientContext &client_, ThreadContext &thread_, TaskContext &task_) :
		client(client_), thread(thread_), task(task_) {
	}

	//! The client-global context; caution needs to be taken when used in parallel situations
	ClientContext &client;
	//! The thread-local context for this execution
	ThreadContext &thread;
	//! The task context for this execution
	TaskContext &task;
};

} // namespace duckdb
