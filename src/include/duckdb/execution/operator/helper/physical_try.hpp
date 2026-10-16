//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/helper/physical_try.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

class ColumnDataCollection;

//! PhysicalTryCollector materializes the result of the plan below a TRY boundary. It is the root sink of the
//! nested executor that PhysicalTry drives, and is never part of a pipeline of the outer query.
class PhysicalTryCollector : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::TRY_COLLECTOR;

public:
	PhysicalTryCollector(PhysicalPlan &physical_plan, PhysicalOperator &child, idx_t estimated_cardinality);

public:
	// Sink interface
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;

	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		return false;
	}

	// Like every root collector, the operator is also a source so that the stub root pipeline above it is valid.
	// It is never scanned: PhysicalTry reads the collection directly.
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;
	bool IsSource() const override {
		return true;
	}

	//! The collection that holds the materialized result of the child plan
	ColumnDataCollection &Collection() const;
};

//! PhysicalTry is the error boundary of an uncorrelated scalar subquery under TRY. It executes its child plan
//! (rooted in a PhysicalTryCollector) in a nested executor. Execution errors raised by the child plan are
//! converted into a single NULL row, every other error is rethrown.
class PhysicalTry : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::TRY;

public:
	PhysicalTry(PhysicalPlan &physical_plan, PhysicalOperator &child, idx_t estimated_cardinality);

public:
	// Source interface
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}
	bool ParallelSource() const override {
		return false;
	}

public:
	void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override;
};

} // namespace duckdb
