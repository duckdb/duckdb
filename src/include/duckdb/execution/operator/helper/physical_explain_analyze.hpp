//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/helper/physical_explain_analyze.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/main/profiler/profiler_print_format.hpp"

namespace duckdb {

class PhysicalExplainAnalyze : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::EXPLAIN_ANALYZE;

public:
	PhysicalExplainAnalyze(PhysicalPlan &physical_plan, vector<LogicalType> types, ProfilerPrintFormat format)
	    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXPLAIN_ANALYZE, std::move(types), 1),
	      format(std::move(format)) {
	}

public:
	ProfilerPrintFormat format;

public:
	// Source interface
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}

public:
	// Sink Interface
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;

	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;

	bool IsSink() const override {
		return true;
	}

	bool ParallelSink() const override {
		return true;
	}
};

} // namespace duckdb
