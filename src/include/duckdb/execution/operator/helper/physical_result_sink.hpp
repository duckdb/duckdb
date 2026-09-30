//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/helper/physical_result_sink.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/result_lifetime.hpp"
#include "duckdb/execution/operator/helper/physical_result_collector.hpp"

namespace duckdb {

class ResultSinkGlobalState;
class ResultSinkLocalState;

//! How the consumer-visible chunk order is established
enum class ResultOrdering : uint8_t {
	//! No order guarantee: a parallel sink stores chunks as they arrive
	UNORDERED,
	//! Source order, preserved by sinking through a single thread
	SOURCE_ORDERED,
	//! Source order, restored from batch indexes under a parallel sink
	BATCH_INDEX_ORDERED
};

//! The root operator of every chunk-producing plan.
class PhysicalResultSink : public PhysicalResultCollector {
public:
	PhysicalResultSink(PhysicalPlan &physical_plan, PreparedStatementData &data, ResultLifetime lifetime,
	                   ResultOrdering ordering);

	//! The retention fixed by the plan. UNDECIDED leaves it to the consumer's first call
	ResultLifetime lifetime;
	ResultOrdering ordering;

public:
	unique_ptr<QueryResult> GetResult(GlobalSinkState &state) const override;
	bool HasBlockedResultProducer(GlobalSinkState &state) const override;

	// Sink interface
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
	SinkNextBatchType NextBatch(ExecutionContext &context, OperatorSinkNextBatchInput &input) const override;
	SinkNextBatchType UpdateMinBatchIndex(ExecutionContext &context, OperatorSinkNextBatchInput &input) const override;

	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;

	OperatorPartitionInfo RequiredPartitionInfo() const override;
	bool ParallelSink() const override;
	bool SinkOrderDependent() const override;
	PipelineExternalInputSupport GetExternalInputSupport() const override;
	//! The plan-time answer: a deferred sink may stream, whatever the consumer decides later
	bool IsStreaming() const override;

private:
	bool BatchOrdered() const {
		return ordering == ResultOrdering::BATCH_INDEX_ORDERED;
	}
	//! The retention in effect: the consumer's decision for a deferred sink, the plan's otherwise
	ResultLifetime CurrentLifetime(ResultSinkGlobalState &gstate) const;
	bool DrainsByBatchIndex(ResultSinkGlobalState &gstate) const;
	SinkResultType SinkDraining(ResultSinkGlobalState &gstate, ResultSinkLocalState &lstate, DataChunk &chunk,
	                            OperatorSinkInput &input) const;
	SinkResultType SinkRetained(ExecutionContext &context, ResultSinkLocalState &lstate, DataChunk &chunk) const;
	SinkCombineResultType CombineDraining(ResultSinkGlobalState &gstate, ResultSinkLocalState &lstate) const;
	SinkCombineResultType CombineRetained(ResultSinkGlobalState &gstate, ResultSinkLocalState &lstate) const;
	unique_ptr<QueryResult> GetStreamResult(ResultSinkGlobalState &gstate) const;
	unique_ptr<QueryResult> GetMaterializedResult(ResultSinkGlobalState &gstate) const;
};

} // namespace duckdb
