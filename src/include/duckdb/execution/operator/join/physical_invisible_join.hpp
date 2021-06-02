//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/join/physical_hash_join.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/execution/join_hashtable.hpp"
#include "duckdb/execution/operator/join/physical_comparison_join.hpp"
#include "duckdb/execution/operator/join/physical_hash_join.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/operator/logical_join.hpp"

namespace duckdb {
//! PhysicalHashJoin represents a hash loop join between two tables
class PhysicalInvisibleJoin : public PhysicalComparisonJoin {
public:
	PhysicalInvisibleJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left, unique_ptr<PhysicalOperator> right,
	                      vector<JoinCondition> cond, JoinType join_type, const vector<idx_t> &left_projection_map,
	                      const vector<idx_t> &right_projection_map, vector<LogicalType> delim_types,
	                      idx_t estimated_cardinality, PerfectHashJoinState join_state);

	vector<idx_t> right_projection_map;
	//! The types of the keys
	vector<LogicalType> condition_types;
	//! The types of all conditions
	vector<LogicalType> build_types;
	//! Duplicate eliminated types; only used for delim_joins (i.e. correlated subqueries)
	vector<LogicalType> delim_types;
	//! Struct for perfect hash optmization
	PerfectHashJoinState pjoin_state;
	bool hasInvisibleJoin {false};

public:
	unique_ptr<GlobalOperatorState> GetGlobalState(ClientContext &context) override;

	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) override;
	void Sink(ExecutionContext &context, GlobalOperatorState &state, LocalSinkState &lstate,
	          DataChunk &input) const override;

	bool Finalize(Pipeline &pipeline, ClientContext &context, unique_ptr<GlobalOperatorState> gstate) override;

	void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;

	unique_ptr<PhysicalOperatorState> GetOperatorState() override;

	bool ExecuteInvisibleJoin(ExecutionContext &context, DataChunk &chunk, PhysicalHashJoinState *state,
	                          JoinHashTable *ht_ptr);
	bool CheckRequirementsForPerfectHashJoin(JoinHashTable *ht_ptr, HashJoinGlobalState &join_global_state);
	void BuildPerfectHashStructure(JoinHashTable *ht_ptr, JoinHTScanState &join_ht_state);
	void FillSelectionVectorSwitch(Vector &source, SelectionVector &sel_vec, idx_t count);
	template <typename T>
	void TemplatedFillSelectionVector(Vector &source, SelectionVector &sel_vec, idx_t count);
	bool HasDuplicates(JoinHashTable *ht_ptr);
};

} // namespace duckdb
