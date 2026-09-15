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

private:
	bool BatchOrdered() const {
		return ordering == ResultOrdering::BATCH_INDEX_ORDERED;
	}
	//! The retention in effect: the consumer's decision for a deferred sink, the plan's otherwise
	ResultLifetime CurrentLifetime(ResultSinkGlobalState &gstate) const;
	bool DrainsByBatchIndex(ResultSinkGlobalState &gstate) const;
	//! Whether the settled format is the identity. True for a sink the plan retained, which has no buffer
	bool UsesChunkFormat(ResultSinkGlobalState &gstate) const;
	//! The chunk format in effect, which owns the retained store. In-memory for a sink the plan retained
	const ChunkFormat &ChunkFormatOf(ResultSinkGlobalState &gstate) const;
	//! The producer's format state, created at its first Append
	ResultFormatLocalState &LocalFormatState(ResultSinkGlobalState &gstate, ResultSinkLocalState &lstate) const;
	//! Convert the chunk into the format's units
	void AppendChunk(ResultSinkGlobalState &gstate, ResultSinkLocalState &lstate, DataChunk &chunk) const;
	//! Finish the next unit that reached the format's target, or with flush_partial the one under
	//! construction. Null when there is none
	unique_ptr<ResultUnit> FinishUnit(ResultSinkGlobalState &gstate, ResultSinkLocalState &lstate,
	                                  bool flush_partial) const;
	//! Give a finished unit to the buffer. True when the producer parked holding it
	bool HandOver(ResultSinkGlobalState &gstate, ResultSinkLocalState &lstate, unique_ptr<ResultUnit> unit,
	              const InterruptState &interrupt) const;
	//! Hand a finished unit to the buffer when draining, list it for Combine when retained. True when
	//! the producer parked holding it
	bool DeliverUnit(ResultSinkGlobalState &gstate, ResultSinkLocalState &lstate, unique_ptr<ResultUnit> unit,
	                 const InterruptState &interrupt) const;
	//! Deliver the units the format has, and with flush_partial the one under construction too. True
	//! when a hand-over parked the producer
	bool DrainUnits(ResultSinkGlobalState &gstate, ResultSinkLocalState &lstate, const InterruptState &interrupt,
	                bool flush_partial) const;
	//! Deliver every unit the format finished. One chunk can fill several units, so this drains until
	//! the format has none ready
	bool DrainFinishedUnits(ResultSinkGlobalState &gstate, ResultSinkLocalState &lstate,
	                        const InterruptState &interrupt) const {
		return DrainUnits(gstate, lstate, interrupt, false);
	}
	//! Deliver the finished units and the one under construction, so no unit spans two batch indexes
	bool FlushUnits(ResultSinkGlobalState &gstate, ResultSinkLocalState &lstate,
	                const InterruptState &interrupt) const {
		return DrainUnits(gstate, lstate, interrupt, true);
	}
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
