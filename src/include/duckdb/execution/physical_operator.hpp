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
#include "duckdb/common/enums/operator_result_type.hpp"
#include "duckdb/common/enums/physical_operator_type.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/optimizer/join_order/join_node.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/execution/physical_operator_states.hpp"
#include "duckdb/common/enums/order_preservation_type.hpp"

namespace duckdb {
class Event;
class Executor;
class PhysicalOperator;
class Pipeline;
class PipelineBuildState;
class MetaPipeline;

//! PhysicalOperator is the base class of the physical operators present in the
//! execution plan
class PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::INVALID;

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
	//! Lock for (re)setting any of the operator states
	mutex lock;

public:
	virtual string GetName() const;
	virtual string ParamsToString() const {
		return "";
	}
	virtual string ToString() const;
	void Print() const;
	virtual vector<const_reference<PhysicalOperator>> GetChildren() const;

	//! Return a vector of the types that will be returned by this operator
	const vector<LogicalType> &GetTypes() const {
		return types;
	}

	virtual bool Equals(const PhysicalOperator &other) const {
		return false;
	}

	virtual void Verify();

public:
	// Operator interface
	virtual unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const;
	virtual unique_ptr<GlobalOperatorState> GetGlobalOperatorState(ClientContext &context) const;
	virtual OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                                   GlobalOperatorState &gstate, OperatorState &state) const;
	virtual OperatorFinalizeResultType FinalExecute(ExecutionContext &context, DataChunk &chunk,
	                                                GlobalOperatorState &gstate, OperatorState &state) const;

	virtual bool ParallelOperator() const {
		return false;
	}

	virtual bool RequiresFinalExecute() const {
		return false;
	}

	//! The influence the operator has on order (insertion order means no influence)
	virtual OrderPreservationType OperatorOrder() const {
		return OrderPreservationType::INSERTION_ORDER;
	}

public:
	// Source interface
	virtual unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context,
	                                                         GlobalSourceState &gstate) const;
	virtual unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const;
	virtual SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const;

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

	//! The type of order emitted by the operator (as a source)
	virtual OrderPreservationType SourceOrder() const {
		return OrderPreservationType::INSERTION_ORDER;
	}

	//! Returns the current progress percentage, or a negative value if progress bars are not supported
	virtual double GetProgress(ClientContext &context, GlobalSourceState &gstate) const;

public:
	// Sink interface

	//! The sink method is called constantly with new input, as long as new input is available. Note that this method
	//! CAN be called in parallel, proper locking is needed when accessing data inside the GlobalSinkState.
	virtual SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const;
	// The combine is called when a single thread has completed execution of its part of the pipeline, it is the final
	// time that a specific LocalSinkState is accessible. This method can be called in parallel while other Sink() or
	// Combine() calls are active on the same GlobalSinkState.
	virtual SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const;
	//! The finalize is called when ALL threads are finished execution. It is called only once per pipeline, and is
	//! entirely single threaded.
	//! If Finalize returns SinkResultType::FINISHED, the sink is marked as finished
	virtual SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                                  OperatorSinkFinalizeInput &input) const;
	//! For sinks with RequiresBatchIndex set to true, when a new batch starts being processed this method is called
	//! This allows flushing of the current batch (e.g. to disk) TODO: should this be able to block too?
	virtual void NextBatch(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate_p) const;

	virtual unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const;
	virtual unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const;

	//! The maximum amount of memory the operator should use per thread.
	static idx_t GetMaxThreadMemory(ClientContext &context);

	//! Whether operator caching is allowed in the current execution context
	static bool OperatorCachingAllowed(ExecutionContext &context);

	virtual bool IsSink() const {
		return false;
	}

	virtual bool ParallelSink() const {
		return false;
	}

	virtual bool RequiresBatchIndex() const {
		return false;
	}

	//! Whether or not the sink operator depends on the order of the input chunks
	//! If this is set to true, we cannot do things like caching intermediate vectors
	virtual bool SinkOrderDependent() const {
		return false;
	}

public:
	// Pipeline construction
	virtual vector<const_reference<PhysicalOperator>> GetSources() const;
	bool AllSourcesSupportBatchIndex() const;

	virtual void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline);

public:
	template <class TARGET>
	TARGET &Cast() {
		if (TARGET::TYPE != PhysicalOperatorType::INVALID && type != TARGET::TYPE) {
			throw InternalException("Failed to cast physical operator to type - physical operator type mismatch");
		}
		return reinterpret_cast<TARGET &>(*this);
	}

	template <class TARGET>
	const TARGET &Cast() const {
		if (TARGET::TYPE != PhysicalOperatorType::INVALID && type != TARGET::TYPE) {
			throw InternalException("Failed to cast physical operator to type - physical operator type mismatch");
		}
		return reinterpret_cast<const TARGET &>(*this);
	}
};

//! Contains state for the CachingPhysicalOperator
class CachingOperatorState : public OperatorState {
public:
	~CachingOperatorState() override {
	}

	void Finalize(const PhysicalOperator &op, ExecutionContext &context) override {
	}

	unique_ptr<DataChunk> cached_chunk;
	bool initialized = false;
	//! Whether or not the chunk can be cached
	bool can_cache_chunk = false;
};

//! Base class that caches output from child Operator class. Note that Operators inheriting from this class should also
//! inherit their state class from the CachingOperatorState.
class CachingPhysicalOperator : public PhysicalOperator {
public:
	static constexpr const idx_t CACHE_THRESHOLD = 64;
	CachingPhysicalOperator(PhysicalOperatorType type, vector<LogicalType> types, idx_t estimated_cardinality);

	bool caching_supported;

public:
	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                           GlobalOperatorState &gstate, OperatorState &state) const final;
	OperatorFinalizeResultType FinalExecute(ExecutionContext &context, DataChunk &chunk, GlobalOperatorState &gstate,
	                                        OperatorState &state) const final;

	bool RequiresFinalExecute() const final {
		return caching_supported;
	}

protected:
	//! Child classes need to implement the ExecuteInternal method instead of the Execute
	virtual OperatorResultType ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                                           GlobalOperatorState &gstate, OperatorState &state) const = 0;

private:
	bool CanCacheType(const LogicalType &type);
};

} // namespace duckdb
