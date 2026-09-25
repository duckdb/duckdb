//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/helper/physical_result_sink.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/result_lifetime.hpp"
#include "duckdb/common/enums/result_ordering.hpp"
#include "duckdb/execution/operator/helper/physical_result_collector.hpp"

namespace duckdb {

class BufferedData;
class ChunkFormat;
class ResultFormatLocalState;
class ResultUnit;

class ResultSinkGlobalState;
class ResultSinkLocalState;

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
	//! Hand the sink the buffer created at submission. Called once, before execution starts
	void SetResultBuffer(shared_ptr<BufferedData> buffer);

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
	bool BuildsOwnResult() const override {
		return false;
	}

private:
	bool BatchOrdered() const {
		return ordering == ResultOrdering::BATCH_INDEX_ORDERED;
	}
	//! The retention in effect: the consumer's decision for a deferred sink, the plan's otherwise
	ResultLifetime CurrentLifetime(ResultSinkGlobalState &gstate) const;
	bool DrainsByBatchIndex(ResultSinkGlobalState &gstate) const;
	//! True for a sink the plan retained, which has no buffer to settle a format
	bool UsesChunkFormat(ResultSinkGlobalState &gstate) const;
	//! In-memory for a sink the plan retained
	const ChunkFormat &ChunkFormatOf(ResultSinkGlobalState &gstate) const;
	ResultFormatLocalState &LocalFormatState(ResultSinkGlobalState &gstate, ResultSinkLocalState &lstate) const;
	//! Feeds the chunk into the format's unit under construction
	void AppendChunk(ResultSinkGlobalState &gstate, ResultSinkLocalState &lstate, DataChunk &chunk) const;
	//! True when the producer parked holding the unit
	bool HandOver(ResultSinkGlobalState &gstate, ResultSinkLocalState &lstate, unique_ptr<ResultUnit> unit,
	              const InterruptState &interrupt) const;
	//! Hand a finished unit to the buffer when draining, list it for Combine when retained. True when
	//! the producer parked holding it
	bool DeliverUnit(ResultSinkGlobalState &gstate, ResultSinkLocalState &lstate, unique_ptr<ResultUnit> unit,
	                 const InterruptState &interrupt) const;
	//! Deliver every unit that has reached the format's cap. One append can finish several, so a
	//! re-invocation after a blocked hand-over continues from whatever the format still holds
	bool DrainFinishedUnits(ResultSinkGlobalState &gstate, ResultSinkLocalState &lstate,
	                        const InterruptState &interrupt) const;
	//! Deliver the finished units and the one under construction, so no unit spans two batch indexes
	bool FlushUnits(ResultSinkGlobalState &gstate, ResultSinkLocalState &lstate, const InterruptState &interrupt) const;
	SinkResultType SinkDraining(ResultSinkGlobalState &gstate, ResultSinkLocalState &lstate, DataChunk &chunk,
	                            OperatorSinkInput &input) const;
	SinkResultType SinkRetained(ExecutionContext &context, ResultSinkGlobalState &gstate, ResultSinkLocalState &lstate,
	                            DataChunk &chunk) const;
	SinkResultType SinkRetainedFormatted(ResultSinkGlobalState &gstate, ResultSinkLocalState &lstate, DataChunk &chunk,
	                                     const InterruptState &interrupt) const;
	SinkCombineResultType CombineDraining(ResultSinkGlobalState &gstate, ResultSinkLocalState &lstate) const;
	SinkCombineResultType CombineRetained(ResultSinkGlobalState &gstate, ResultSinkLocalState &lstate) const;
	unique_ptr<QueryResult> GetMaterializedResult(ResultSinkGlobalState &gstate) const;
	unique_ptr<QueryResult> GetFormattedResult(ResultSinkGlobalState &gstate, ClientContext &context) const;

private:
	//! The buffer created at submission, which also holds the retention decision
	shared_ptr<BufferedData> result_buffer;
};

} // namespace duckdb
