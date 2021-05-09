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
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/operator/logical_join.hpp"

namespace duckdb {

struct PerfectHashJoinState {
	Value minimum;
	Value maximum;
	bool is_build_small {false};
	bool is_probe_small {false};
};

class PhysicalHashJoinState : public PhysicalOperatorState {
public:
	PhysicalHashJoinState(PhysicalOperator &op, PhysicalOperator *left, PhysicalOperator *right,
	                      vector<JoinCondition> &conditions)
	    : PhysicalOperatorState(op, left) {
	}

	DataChunk cached_chunk;
	DataChunk join_keys;
	ExpressionExecutor probe_executor;
	unique_ptr<JoinHashTable::ScanStructure> scan_structure;
};
//! PhysicalHashJoin represents a hash loop join between two tables

class HashJoinGlobalState : public GlobalOperatorState {
public:
	HashJoinGlobalState() {
	}

	//! The HT used by the join
	unique_ptr<JoinHashTable> hash_table;
	//! Only used for FULL OUTER JOIN: scan state of the final scan to find unmatched tuples in the build-side
	JoinHTScanState ht_scan_state;
};

class HashJoinLocalState : public LocalSinkState {
public:
	DataChunk build_chunk;
	DataChunk join_keys;
	ExpressionExecutor build_executor;
};
class PhysicalHashJoin : public PhysicalComparisonJoin {
public:
	PhysicalHashJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left, unique_ptr<PhysicalOperator> right,
	                 vector<JoinCondition> cond, JoinType join_type, const vector<idx_t> &left_projection_map,
	                 const vector<idx_t> &right_projection_map, vector<LogicalType> delim_types,
	                 idx_t estimated_cardinality, PerfectHashJoinState join_state);
	PhysicalHashJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left, unique_ptr<PhysicalOperator> right,
	                 vector<JoinCondition> cond, JoinType join_type, idx_t estimated_cardinality,
	                 PerfectHashJoinState join_state);

	vector<idx_t> right_projection_map;
	//! The types of the keys
	vector<LogicalType> condition_types;
	//! The types of all conditions
	vector<LogicalType> build_types;
	//! Duplicate eliminated types; only used for delim_joins (i.e. correlated subqueries)
	vector<LogicalType> delim_types;
	//! Struct for perfect hash optmization
	PerfectHashJoinState perfect_join_state;
	bool hasBuiltPerfectHashTable {false};
	std::vector<Vector> build_columns;

public:
	unique_ptr<GlobalOperatorState> GetGlobalState(ClientContext &context) override;

	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) override;
	void Sink(ExecutionContext &context, GlobalOperatorState &state, LocalSinkState &lstate, DataChunk &input) override;
	void Finalize(Pipeline &pipeline, ClientContext &context, unique_ptr<GlobalOperatorState> gstate) override;

	void GetChunkInternal(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state) override;
	unique_ptr<PhysicalOperatorState> GetOperatorState() override;
	bool ProbePerfectHashTable(ExecutionContext &context, DataChunk &chunk, PhysicalHashJoinState *state);
	void CheckRequirementsForPerfectHashJoin(JoinHashTable *ht_ptr, HashJoinGlobalState &join_global_state);
	void BuildPerfectHashStructure(JoinHashTable *ht_ptr, JoinHTScanState &join_ht_state);

private:
	void ProbeHashTable(ExecutionContext &context, DataChunk &chunk, PhysicalOperatorState *state_p);
};

} // namespace duckdb
