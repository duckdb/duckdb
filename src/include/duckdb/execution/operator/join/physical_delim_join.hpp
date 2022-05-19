//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/join/physical_delim_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {
class PhysicalHashAggregate;

//! PhysicalDelimJoin represents a join where the LHS will be duplicate eliminated and pushed into a
//! PhysicalChunkCollectionScan in the RHS.
class PhysicalDelimJoin : public PhysicalOperator {
public:
	PhysicalDelimJoin(vector<LogicalType> types, unique_ptr<PhysicalOperator> original_join,
	                  vector<PhysicalOperator *> delim_scans, idx_t estimated_cardinality);

	unique_ptr<PhysicalOperator> join;
	unique_ptr<PhysicalHashAggregate> distinct;
	vector<PhysicalOperator *> delim_scans;

public:
	vector<PhysicalOperator *> GetChildren() const override;

public:
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate,
	                    DataChunk &input) const override;
	void Combine(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          GlobalSinkState &gstate) const override;

	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		return true;
	}

	string ParamsToString() const override;

public:
	void BuildPipelines(Executor &executor, Pipeline &current, PipelineBuildState &state) override;
};

} // namespace duckdb
