//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/helper/physical_batch_collector.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/helper/physical_result_collector.hpp"
#include "duckdb/main/materialized_query_result.hpp"
#include "duckdb/common/types/batched_data_collection.hpp"

namespace duckdb {

class PhysicalBatchCollector : public PhysicalResultCollector {
public:
	PhysicalBatchCollector(PreparedStatementData &data);

public:
	unique_ptr<QueryResult> GetResult(GlobalSinkState &state) override;

public:
	// Sink interface
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	void Combine(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          GlobalSinkState &gstate) const override;

	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;

	bool RequiresBatchIndex() const override {
		return true;
	}

	bool IsSink() const override {
		return true;
	}

	bool ParallelSink() const override {
		return true;
	}
};

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class BatchCollectorGlobalState : public GlobalSinkState {
public:
	BatchCollectorGlobalState(ClientContext &context, const PhysicalBatchCollector &op) : data(op.types) {
	}

	mutex glock;
	BatchedDataCollection data;
	unique_ptr<QueryResult> result;
};

class BatchCollectorLocalState : public LocalSinkState {
public:
	BatchCollectorLocalState(ClientContext &context, const PhysicalBatchCollector &op) : data(op.types) {
	}

	BatchedDataCollection data;
};

} // namespace duckdb
