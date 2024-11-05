//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/aggregate/physical_window.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/parallel/pipeline.hpp"

namespace duckdb {

//! PhysicalWindow implements window functions
//! It assumes that all functions have a common partitioning and ordering
class PhysicalWindow : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::WINDOW;

public:
	PhysicalWindow(vector<LogicalType> types, vector<unique_ptr<Expression>> select_list, idx_t estimated_cardinality,
	               PhysicalOperatorType type = PhysicalOperatorType::WINDOW);

	//! The projection list of the WINDOW statement (may contain aggregates)
	vector<unique_ptr<Expression>> select_list;
	//! The window expression with the order clause
	idx_t order_idx;
	//! Whether or not the window is order dependent (only true if ANY window function contains neither an order nor a
	//! partition clause)
	bool is_order_dependent;

public:
	// Source interface
	unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context,
	                                                 GlobalSourceState &gstate) const override;
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;
	OperatorPartitionData GetPartitionData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
	                                       LocalSourceState &lstate,
	                                       const OperatorPartitionInfo &partition_info) const override;

	bool IsSource() const override {
		return true;
	}
	bool ParallelSource() const override {
		return true;
	}

	bool SupportsPartitioning(const OperatorPartitionInfo &partition_info) const override;
	OrderPreservationType SourceOrder() const override;

	double GetProgress(ClientContext &context, GlobalSourceState &gstate_p) const override;

public:
	// Sink interface
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;

	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;

	bool IsSink() const override {
		return true;
	}

	bool ParallelSink() const override {
		return !is_order_dependent;
	}

	bool SinkOrderDependent() const override {
		return is_order_dependent;
	}

public:
	idx_t MaxThreads(ClientContext &context);

	InsertionOrderPreservingMap<string> ParamsToString() const override;
};

} // namespace duckdb
