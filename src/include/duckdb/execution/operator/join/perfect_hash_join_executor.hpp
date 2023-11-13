//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/join/perfect_hash_join_executor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/row_operations/row_operations.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/execution/join_hashtable.hpp"
#include "duckdb/execution/physical_operator.hpp"

namespace duckdb {

class HashJoinOperatorState;
class HashJoinGlobalSinkState;
class PhysicalHashJoin;

struct PerfectHashJoinStats {
	Value build_min;
	Value build_max;
	Value probe_min;
	Value probe_max;
	bool is_build_small = false;
	bool is_build_dense = false;
	bool is_probe_in_domain = false;
	idx_t build_range = 0;
	idx_t estimated_cardinality = 0;
};

//! PhysicalHashJoin represents a hash loop join between two tables
class PerfectHashJoinExecutor {
	using PerfectHashTable = vector<Vector>;

public:
	explicit PerfectHashJoinExecutor(const PhysicalHashJoin &join, JoinHashTable &ht, PerfectHashJoinStats pjoin_stats);

public:
	bool CanDoPerfectHashJoin();

	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context);
	OperatorResultType ProbePerfectHashTable(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                                         OperatorState &state);
	bool BuildPerfectHashTable(LogicalType &type);

private:
	void FillSelectionVectorSwitchProbe(Vector &source, SelectionVector &build_sel_vec, SelectionVector &probe_sel_vec,
	                                    idx_t count, idx_t &probe_sel_count);
	template <typename T>
	void TemplatedFillSelectionVectorProbe(Vector &source, SelectionVector &build_sel_vec,
	                                       SelectionVector &probe_sel_vec, idx_t count, idx_t &prob_sel_count);

	bool FillSelectionVectorSwitchBuild(Vector &source, SelectionVector &sel_vec, SelectionVector &seq_sel_vec,
	                                    idx_t count);
	template <typename T>
	bool TemplatedFillSelectionVectorBuild(Vector &source, SelectionVector &sel_vec, SelectionVector &seq_sel_vec,
	                                       idx_t count);
	bool FullScanHashTable(LogicalType &key_type);

private:
	const PhysicalHashJoin &join;
	JoinHashTable &ht;
	//! Columnar perfect hash table
	PerfectHashTable perfect_hash_table;
	//! Build and probe statistics
	PerfectHashJoinStats perfect_join_statistics;
	//! Stores the occurences of each value in the build side
	unsafe_unique_array<bool> bitmap_build_idx;
	//! Stores the number of unique keys in the build side
	idx_t unique_keys = 0;
};

} // namespace duckdb
