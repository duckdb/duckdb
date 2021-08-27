//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/join/physical_hash_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/execution/join_hashtable.hpp"
#include "duckdb/execution/operator/join/physical_comparison_join.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/operator/logical_join.hpp"

namespace duckdb {

//! PhysicalHashJoin represents a hash loop join between two tables
class PhysicalHashJoin : public PhysicalComparisonJoin {
public:
	PhysicalHashJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left, unique_ptr<PhysicalOperator> right,
	                 vector<JoinCondition> cond, JoinType join_type, const vector<idx_t> &left_projection_map,
	                 const vector<idx_t> &right_projection_map, vector<LogicalType> delim_types,
	                 idx_t estimated_cardinality);
	PhysicalHashJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left, unique_ptr<PhysicalOperator> right,
	                 vector<JoinCondition> cond, JoinType join_type, idx_t estimated_cardinality);
	void Combine(ExecutionContext &context, GlobalSinkState &gstate, LocalSinkState &lstate) override;

	vector<idx_t> right_projection_map;
	//! The types of the keys
	vector<LogicalType> condition_types;
	//! The types of all conditions
	vector<LogicalType> build_types;
	//! Duplicate eliminated types; only used for delim_joins (i.e. correlated subqueries)
	vector<LogicalType> delim_types;
	//! Whether or not we can cache the chunk
	bool can_cache;

public:
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;

	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	void Sink(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate,
	          DataChunk &input) const override;
	void Combine(ExecutionContext &context, GlobalSinkState &gstate, LocalSinkState &lstate) const;
	bool Finalize(Pipeline &pipeline, ClientContext &context, unique_ptr<GlobalSinkState> gstate) override;

	// void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, OperatorState *state) const override;
	// unique_ptr<OperatorState> GetOperatorState() override;

	// void FinalizeOperatorState(OperatorState &state, ExecutionContext &context) override;

private:
	void ProbeHashTable(ExecutionContext &context, DataChunk &chunk, OperatorState *state_p) const;
};

} // namespace duckdb
