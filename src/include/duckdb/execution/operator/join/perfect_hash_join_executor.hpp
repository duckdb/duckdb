//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/join/perfect_hash_join_executor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/common/value_operations/value_operations.hpp"
#include "duckdb/execution/join_hashtable.hpp"
//#include "duckdb/execution/operator/join/physical_hash_join.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/planner/operator/logical_join.hpp"

namespace duckdb {
constexpr size_t BUILD_THRESHOLD = 1 << 14; // 16384
constexpr size_t MIN_THRESHOLD = 1 << 7;    // 128

class PhysicalHashJoinState;
class HashJoinGlobalState;

struct PerfectHashJoinState {
	Value build_min;
	Value build_max;
	Value probe_min;
	Value probe_max;
	bool is_build_small {false};
	bool is_probe_in_range {false};
	bool is_build_min_small {false};
	bool is_build_dense {false};
	idx_t range {0};
	idx_t estimated_cardinality {0};
};

//! PhysicalHashJoin represents a hash loop join between two tables
class PerfectHashJoinExecutor {
public:
	PerfectHashJoinExecutor(PerfectHashJoinState join_state);
	bool ExecutePerfectHashJoin(ExecutionContext &context, DataChunk &chunk, PhysicalHashJoinState *state,
	                            JoinHashTable *ht_ptr, PhysicalOperator *operator_child) const;
	bool CheckRequirementsForPerfectHashJoin(JoinHashTable *ht_ptr, HashJoinGlobalState *hj_global_state);
	void BuildPerfectHashStructure(JoinHashTable *ht_ptr, JoinHTScanState &join_ht_state, LogicalType type);
	void FillSelectionVectorSwitch(Vector &source, SelectionVector &sel_vec, idx_t count) const;
	template <typename T>
	void TemplatedFillSelectionVector(Vector &source, SelectionVector &sel_vec, idx_t count) const;

private:
	PerfectHashJoinState pjoin_state;
	bool hasInvisibleJoin {false};
};

} // namespace duckdb
