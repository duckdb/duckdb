//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/helper/physical_materialized_collector.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/helper/physical_result_collector.hpp"

namespace duckdb {

//! PhysicalResultCollector is an abstract class that is used to generate the final result of a query
class PhysicalMaterializedCollector : public PhysicalResultCollector {
public:
	PhysicalMaterializedCollector(PhysicalOperator *plan);

public:
	unique_ptr<QueryResult> GetResult(GlobalSinkState &state) override;

public:
	// Sink interface
	SinkResultType Sink(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate,
	                    DataChunk &input) const override;
	void Combine(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          GlobalSinkState &gstate) const override;

	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;

	bool IsSink() const override {
		return true;
	}

	bool ParallelSink() const override {
		return true;
	}

	bool RequiresBatchIndex() const override {
		return true;
	}
};

} // namespace duckdb
