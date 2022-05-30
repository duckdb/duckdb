//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/physical_operator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/physical_operator_type.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/common/enums/operator_result_type.hpp"

namespace duckdb {
class Event;
class Executor;
class PhysicalOperator;
class Pipeline;
class PipelineBuildState;

// LCOV_EXCL_START
class OperatorState {
public:
	virtual ~OperatorState() {
	}

	virtual void Finalize(PhysicalOperator *op, ExecutionContext &context) {
	}
};

class GlobalOperatorState {
public:
	virtual ~GlobalOperatorState() {
	}
};

class GlobalSinkState {
public:
	GlobalSinkState() : state(SinkFinalizeType::READY) {
	}
	virtual ~GlobalSinkState() {
	}

	SinkFinalizeType state;
};

class LocalSinkState {
public:
	virtual ~LocalSinkState() {
	}

	//! The current batch index
	//! This is only set in case RequiresBatchIndex() is true, and the source has support for it (SupportsBatchIndex())
	//! Otherwise this is left on INVALID_INDEX
	//! The batch index is a globally unique, increasing index that should be used to maintain insertion order
	//! //! in conjunction with parallelism
	idx_t batch_index = DConstants::INVALID_INDEX;
};

class GlobalSourceState {
public:
	virtual ~GlobalSourceState() {
	}

	virtual idx_t MaxThreads() {
		return 1;
	}
};

class LocalSourceState {
public:
	virtual ~LocalSourceState() {
	}
};

// LCOV_EXCL_STOP

//! PhysicalOperator is the base class of the physical operators present in the
//! execution plan
class PhysicalOperator {
public:
	PhysicalOperator(PhysicalOperatorType type, vector<LogicalType> types, idx_t estimated_cardinality)
	    : type(type), types(std::move(types)), estimated_cardinality(estimated_cardinality) {
	}
	virtual ~PhysicalOperator() {
	}

	//! The physical operator type
	PhysicalOperatorType type;
	//! The set of children of the operator
	vector<unique_ptr<PhysicalOperator>> children;
	//! The types returned by this physical operator
	vector<LogicalType> types;
	//! The estimated cardinality of this physical operator
	idx_t estimated_cardinality;
	//! The global sink state of this operator
	unique_ptr<GlobalSinkState> sink_state;
	//! The global state of this operator
	unique_ptr<GlobalOperatorState> op_state;

public:
	virtual string GetName() const;
	virtual string ParamsToString() const {
		return "";
	}
	virtual string ToString() const;
	void Print() const;
	virtual vector<PhysicalOperator *> GetChildren() const;

	//! Return a vector of the types that will be returned by this operator
	const vector<LogicalType> &GetTypes() const {
		return types;
	}

	virtual bool Equals(const PhysicalOperator &other) const {
		return false;
	}

	virtual void Verify();

	//! Whether or not the operator depends on the order of the input chunks
	//! If this is set to true, we cannot do things like caching intermediate vectors
	virtual bool IsOrderDependent() const {
		return false;
	}

public:
	// Operator interface
	virtual unique_ptr<OperatorState> GetOperatorState(ClientContext &context) const;
	virtual unique_ptr<GlobalOperatorState> GetGlobalOperatorState(ClientContext &context) const;
	virtual OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                                   GlobalOperatorState &gstate, OperatorState &state) const;

	virtual bool ParallelOperator() const {
		return false;
	}

	virtual bool RequiresCache() const {
		return false;
	}

public:
	// Source interface
	virtual unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context,
	                                                         GlobalSourceState &gstate) const;
	virtual unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const;
	virtual void GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
	                     LocalSourceState &lstate) const;
	virtual idx_t GetBatchIndex(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
	                            LocalSourceState &lstate) const;

	virtual bool IsSource() const {
		return false;
	}

	virtual bool ParallelSource() const {
		return false;
	}

	virtual bool SupportsBatchIndex() const {
		return false;
	}

	//! Returns the current progress percentage, or a negative value if progress bars are not supported
	virtual double GetProgress(ClientContext &context, GlobalSourceState &gstate) const;

public:
	// Sink interface

	//! The sink method is called constantly with new input, as long as new input is available. Note that this method
	//! CAN be called in parallel, proper locking is needed when accessing data inside the GlobalSinkState.
	virtual SinkResultType Sink(ExecutionContext &context, GlobalSinkState &gstate, LocalSinkState &lstate,
	                            DataChunk &input) const;
	// The combine is called when a single thread has completed execution of its part of the pipeline, it is the final
	// time that a specific LocalSinkState is accessible. This method can be called in parallel while other Sink() or
	// Combine() calls are active on the same GlobalSinkState.
	virtual void Combine(ExecutionContext &context, GlobalSinkState &gstate, LocalSinkState &lstate) const;
	//! The finalize is called when ALL threads are finished execution. It is called only once per pipeline, and is
	//! entirely single threaded.
	//! If Finalize returns SinkResultType::FINISHED, the sink is marked as finished
	virtual SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                                  GlobalSinkState &gstate) const;

	virtual unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const;
	virtual unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const;

	virtual bool IsSink() const {
		return false;
	}

	virtual bool ParallelSink() const {
		return false;
	}

	virtual bool RequiresBatchIndex() const {
		return false;
	}

public:
	// Pipeline construction
	virtual vector<const PhysicalOperator *> GetSources() const;
	bool AllSourcesSupportBatchIndex() const;

	void AddPipeline(Executor &executor, shared_ptr<Pipeline> current, PipelineBuildState &state);
	virtual void BuildPipelines(Executor &executor, Pipeline &current, PipelineBuildState &state);
	void BuildChildPipeline(Executor &executor, Pipeline &current, PipelineBuildState &state,
	                        PhysicalOperator *pipeline_child);
};

} // namespace duckdb
