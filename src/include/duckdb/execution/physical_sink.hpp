//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/physical_sink.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

class Pipeline;

class GlobalOperatorState {
public:
	virtual ~GlobalOperatorState() {
	}
};

class LocalSinkState {
public:
	virtual ~LocalSinkState() {
	}
};

class PhysicalSink : public PhysicalOperator {
public:
	PhysicalSink(PhysicalOperatorType type, vector<LogicalType> types, idx_t estimated_cardinality)
	    : PhysicalOperator(type, move(types), estimated_cardinality) {
	}

	unique_ptr<GlobalOperatorState> sink_state;

public:
	//! The sink method is called constantly with new input, as long as new input is available. Note that this method
	//! CAN be called in parallel, proper locking is needed when accessing data inside the GlobalOperatorState.
	virtual void Sink(ExecutionContext &context, GlobalOperatorState &gstate, LocalSinkState &lstate,
	                  DataChunk &input) const = 0;
	// The combine is called when a single thread has completed execution of its part of the pipeline, it is the final
	// time that a specific LocalSinkState is accessible. This method can be called in parallel while other Sink() or
	// Combine() calls are active on the same GlobalOperatorState.
	virtual void Combine(ExecutionContext &context, GlobalOperatorState &gstate, LocalSinkState &lstate) {
	}
	//! The finalize is called when ALL threads are finished execution. It is called only once per pipeline, and is
	//! entirely single threaded.
	virtual bool Finalize(Pipeline &pipeline, ClientContext &context, unique_ptr<GlobalOperatorState> gstate) {
		this->sink_state = move(gstate);
		return true;
	}

	virtual unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) {
		return make_unique<LocalSinkState>();
	}
	virtual unique_ptr<GlobalOperatorState> GetGlobalState(ClientContext &context) {
		return make_unique<GlobalOperatorState>();
	}

	bool IsSink() const override {
		return true;
	}

	void Schedule(ClientContext &context);
};

} // namespace duckdb
