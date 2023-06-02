//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/join/physical_asof_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/operator/join/physical_comparison_join.hpp"
#include "duckdb/planner/bound_result_modifier.hpp"

namespace duckdb {

//! PhysicalAsOfJoin represents a piecewise merge loop join between
//! two tables
class PhysicalAsOfJoin : public PhysicalComparisonJoin {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::ASOF_JOIN;

public:
	PhysicalAsOfJoin(LogicalComparisonJoin &op, unique_ptr<PhysicalOperator> left, unique_ptr<PhysicalOperator> right);

	vector<LogicalType> join_key_types;
	vector<column_t> null_sensitive;

	// Equalities
	vector<unique_ptr<Expression>> lhs_partitions;
	vector<unique_ptr<Expression>> rhs_partitions;

	// Inequality Only
	vector<BoundOrderByNode> lhs_orders;
	vector<BoundOrderByNode> rhs_orders;

	// Projection mappings
	vector<column_t> right_projection_map;

public:
	// Operator Interface
	unique_ptr<GlobalOperatorState> GetGlobalOperatorState(ClientContext &context) const override;
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;

	bool ParallelOperator() const override {
		return true;
	}

protected:
	// CachingOperator Interface
	OperatorResultType ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                                   GlobalOperatorState &gstate, OperatorState &state) const override;

public:
	// Source interface
	unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context,
	                                                 GlobalSourceState &gstate) const override;
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return IsRightOuterJoin(join_type);
	}
	bool ParallelSource() const override {
		return true;
	}

public:
	// Sink Interface
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	void Combine(ExecutionContext &context, GlobalSinkState &gstate, LocalSinkState &lstate) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          GlobalSinkState &gstate) const override;

	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		return true;
	}

private:
	// resolve joins that output max N elements (SEMI, ANTI, MARK)
	void ResolveSimpleJoin(ExecutionContext &context, DataChunk &input, DataChunk &chunk, OperatorState &state) const;
	// resolve joins that can potentially output N*M elements (INNER, LEFT, FULL)
	OperatorResultType ResolveComplexJoin(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                                      OperatorState &state) const;
};

} // namespace duckdb
