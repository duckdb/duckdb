//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/helper/physical_buffered_batch_collector.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/helper/physical_result_collector.hpp"
#include "duckdb/common/queue.hpp"

namespace duckdb {

class BufferedBatchCollectorLocalState : public LocalSinkState {
public:
	BufferedBatchCollectorLocalState();

public:
	idx_t current_batch = 0;
};

class PhysicalBufferedBatchCollector : public PhysicalResultCollector {
public:
	explicit PhysicalBufferedBatchCollector(PreparedStatementData &data);

public:
	unique_ptr<QueryResult> GetResult(GlobalSinkState &state) override;

public:
	// Sink interface
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
	SinkNextBatchType NextBatch(ExecutionContext &context, OperatorSinkNextBatchInput &input) const override;

	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;

	OperatorPartitionInfo RequiredPartitionInfo() const override {
		return OperatorPartitionInfo::BatchIndex();
	}

	bool ParallelSink() const override {
		return true;
	}

	bool IsStreaming() const override {
		return true;
	}
};

} // namespace duckdb
