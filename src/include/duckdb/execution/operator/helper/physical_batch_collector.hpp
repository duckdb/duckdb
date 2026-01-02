//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/helper/physical_batch_collector.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/helper/physical_result_collector.hpp"
#include "duckdb/common/types/batched_data_collection.hpp"

namespace duckdb {

class PhysicalBatchCollector : public PhysicalResultCollector {
public:
	PhysicalBatchCollector(PhysicalPlan &physical_plan, PreparedStatementData &data);

public:
	unique_ptr<QueryResult> GetResult(GlobalSinkState &state) const override;

public:
	// Sink interface
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;

	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;

	OperatorPartitionInfo RequiredPartitionInfo() const override {
		return OperatorPartitionInfo::BatchIndex();
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
	BatchCollectorGlobalState(ClientContext &context, const PhysicalBatchCollector &op)
	    : data(context, op.types, op.memory_type) {
	}

	mutex glock;
	BatchedDataCollection data;
	unique_ptr<QueryResult> result;
};

class BatchCollectorLocalState : public LocalSinkState {
public:
	BatchCollectorLocalState(ClientContext &context, const PhysicalBatchCollector &op)
	    : data(context, op.types, op.memory_type) {
	}

	BatchedDataCollection data;
};
} // namespace duckdb
