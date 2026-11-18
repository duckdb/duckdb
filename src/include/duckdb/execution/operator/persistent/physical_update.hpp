//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/persistent/physical_update.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/bound_constraint.hpp"

namespace duckdb {
class DataTable;
class DuckTableEntry;

//! Physically update data in a table
class PhysicalUpdate : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::UPDATE;

public:
	PhysicalUpdate(PhysicalPlan &physical_plan, vector<LogicalType> types, DuckTableEntry &tableref, DataTable &table,
	               vector<PhysicalIndex> columns, vector<unique_ptr<Expression>> expressions,
	               vector<unique_ptr<Expression>> bound_defaults, vector<unique_ptr<BoundConstraint>> bound_constraints,
	               idx_t estimated_cardinality, bool return_chunk, bool capture_old_rows,
	               vector<idx_t> old_row_columns);

	DuckTableEntry &tableref;
	DataTable &table;
	vector<PhysicalIndex> columns;
	vector<unique_ptr<Expression>> expressions;
	vector<unique_ptr<Expression>> bound_defaults;
	vector<unique_ptr<BoundConstraint>> bound_constraints;
	bool update_is_del_and_insert;
	//! If the returning statement is present, or transition tables are captured, return the whole chunk
	bool return_chunk;
	//! If set, also emit the pre-update (OLD) row image after the NEW image
	bool capture_old_rows;
	//! Input-chunk index of each captured OLD physical column, in physical table order (only when capture_old_rows)
	vector<idx_t> old_row_columns;
	//! Set to true, if we are updating an index column.
	bool index_update;

public:
	// Source interface
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	ProgressData GetProgress(ClientContext &context, GlobalSourceState &gstate) const override;
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}

public:
	// Sink interface
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;

	bool IsSink() const override {
		return true;
	}
	bool ParallelSink() const override {
		return true;
	}

public:
	//! A thread stages out-of-order in-place updates until they use 1/STAGED_UPDATE_MEMORY_FRACTION of the memory
	//! limit divided over the threads, bounded by STAGED_UPDATE_MIN_MEMORY and STAGED_UPDATE_MAX_MEMORY
	static constexpr idx_t STAGED_UPDATE_MEMORY_FRACTION = 10;
	static constexpr idx_t STAGED_UPDATE_MIN_MEMORY = 1ULL << 20ULL;
	static constexpr idx_t STAGED_UPDATE_MAX_MEMORY = 64ULL << 20ULL;

private:
	void StageUpdates(ExecutionContext &context, GlobalSinkState &gstate, LocalSinkState &lstate, DataChunk &updates,
	                  Vector &row_ids, InterruptState &interrupt_state) const;
	void FlushStagedUpdates(ExecutionContext &context, GlobalSinkState &gstate, LocalSinkState &lstate,
	                        InterruptState &interrupt_state) const;
};

} // namespace duckdb
