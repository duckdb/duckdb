//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/mark_join_post_processor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/mark_null_strategy.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/row/tuple_data_collection.hpp"
#include "duckdb/common/types/row/tuple_data_layout.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/execution/aggregate_hashtable.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"

namespace duckdb {

class BufferManager;
class ClientContext;

class MarkJoinPostProcessor {
public:
	struct MarkJoinNullRemainder {
		vector<LogicalType> key_types;
		shared_ptr<TupleDataLayout> layout;
		unique_ptr<TupleDataCollection> data;
		TupleDataAppendState append_state;
	};

	//! Initialize the MARK post-processing strategy for this hash table
	void Initialize(ClientContext &context, BufferManager &buffer_manager, JoinType join_type, idx_t condition_count,
	                const vector<ExpressionType> &equality_predicates, const vector<LogicalType> &condition_types);
	//! Initialize the correlated MARK count state
	void InitializeCorrelatedCounts(const vector<LogicalType> &correlated_types);

	//! Returns true if the correlated count-based MARK path is active
	bool UsesCorrelatedCounts() const;
	//! Returns true if the row-valued null remainder path is active
	bool UsesNullRemainder() const;
	//! Returns true if the condition-scan null refinement path is active
	bool UsesConditionScan() const;

	//! Register build-side key state needed for MARK post-processing
	void SinkBuildKeys(DataChunk &keys);
	//! Merge sink-local MARK post-processing state into the global state
	void Merge(MarkJoinPostProcessor &other, bool &has_null);
	//! Reset MARK post-processing state for recursive reuse
	void Reset();
	//! Constructs an uncorrelated MARK result for an empty build side
	void ConstructEmptyResult(DataChunk &join_keys, DataChunk &probe_data, DataChunk &result,
	                          const vector<idx_t> &lhs_output_columns, const vector<bool> &null_values_are_equal,
	                          bool has_null);
	//! Constructs an uncorrelated MARK result using the configured hash-style refinement
	void ConstructResult(DataChunk &join_keys, DataChunk &probe_data, DataChunk &result,
	                     const vector<idx_t> &lhs_output_columns, const vector<bool> &null_values_are_equal,
	                     const bool *found_match, bool has_null);
	//! Constructs a MARK result by rescanning RHS condition rows for unresolved probe rows
	void ConstructResult(DataChunk &join_keys, DataChunk &probe_data, DataChunk &result,
	                     const vector<bool> &null_values_are_equal, const bool *found_match,
	                     ColumnDataCollection &rhs_condition_data, const vector<JoinCondition> &conditions,
	                     bool has_null);

	//! Marks probe rows NULL when the join key itself forces an unknown MARK result
	void ApplyJoinKeyNullMask(DataChunk &join_keys, const vector<bool> &null_values_are_equal,
	                          ValidityMask &mask) const;
	//! Refines unmatched MARK rows from FALSE to NULL based on build-side null state
	void RefineUnmatchedRows(DataChunk &join_keys, ValidityMask &mask, const bool *found_match, bool has_null);
	//! Constructs the correlated MARK result using the per-group count state
	void ConstructCorrelatedMarkResult(DataChunk &keys, DataChunk &probe_data, DataChunk &result,
	                                   const vector<idx_t> &lhs_output_in_probe, const bool *found_match);

private:
	//! Initializes a MARK result using either the provided probe projection or the full probe chunk
	void InitializeMarkJoinResult(DataChunk &join_keys, DataChunk &probe_data, DataChunk &result,
	                              const vector<bool> &null_values_are_equal, bool *&bool_result, ValidityMask *&mask,
	                              optional_ptr<const vector<idx_t>> lhs_output_columns = nullptr) const;
	//! Registers null-bearing build rows for row-valued MARK refinement
	void RegisterNullRemainderRows(DataChunk &keys);
	//! Merges row-valued null remainder state
	void MergeNullRemainderRows(MarkJoinPostProcessor &other, bool &has_null);
	//! Refines unmatched rows by scanning null-bearing remainder rows
	void ProbeNullRemainderRows(DataChunk &join_keys, ValidityMask &mask, const bool *found_match);
	//! Refines unmatched rows by rescanning RHS condition rows
	void ProbeConditionScanRows(DataChunk &join_keys, ValidityMask &mask, const bool *found_match,
	                            ColumnDataCollection &rhs_condition_data, const vector<JoinCondition> &conditions,
	                            bool has_null);
	//! Chooses the MARK post-processing strategy
	MarkNullStrategy ChooseStrategy() const;

private:
	optional_ptr<ClientContext> context;
	optional_ptr<BufferManager> buffer_manager;
	JoinType join_type;
	idx_t condition_count = 0;
	vector<ExpressionType> equality_predicates;
	vector<LogicalType> condition_types;

	struct {
		MarkNullStrategy strategy = MarkNullStrategy::NONE;
		struct {
			mutex lock;
			vector<LogicalType> correlated_types;
			vector<unique_ptr<Expression>> correlated_aggregates;
			unique_ptr<GroupedAggregateHashTable> correlated_counts;
			DataChunk group_chunk;
			DataChunk correlated_payload;
			DataChunk result_chunk;
		} correlated_counts;
		struct {
			mutex lock;
			bool enabled = false;
			bool has_null_rows = false;
			bool has_all_null = false;
			MarkJoinNullRemainder remainder;
		} null_remainder;
	} state;
};

} // namespace duckdb
