//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/execution_context.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/optional_ptr.hpp"

namespace duckdb {
class ClientContext;
class ThreadContext;
class Pipeline;

class ExecutionContext {
public:
	ExecutionContext(ClientContext &client_p, ThreadContext &thread_p, optional_ptr<Pipeline> pipeline_p)
	    : client(client_p), thread(thread_p), pipeline(pipeline_p) {
	}

	//! The client-global context; caution needs to be taken when used in parallel situations
	ClientContext &client;
	//! The thread-local context for this execution
	ThreadContext &thread;
	//! Reference to the pipeline for this execution, can be used for example by operators determine caching strategy
	optional_ptr<Pipeline> pipeline;
};

} // namespace duckdb
