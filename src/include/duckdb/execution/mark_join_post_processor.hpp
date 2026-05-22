//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/mark_join_post_processor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/row/tuple_data_collection.hpp"
#include "duckdb/common/types/row/tuple_data_layout.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/execution/aggregate_hashtable.hpp"

namespace duckdb {

class BufferManager;
class ClientContext;

enum class MarkNullStrategy : uint8_t { NONE, SIMPLE_HAS_NULL, CORRELATED_COUNTS, NULL_REMAINDER };

class MarkJoinPostProcessor {
public:
	struct MarkJoinNullRemainder {
		vector<LogicalType> key_types;
		shared_ptr<TupleDataLayout> layout;
		unique_ptr<TupleDataCollection> data;
		TupleDataAppendState append_state;
	};

	//! Initialize the MARK post-processing strategy for this hash table
	void Initialize(ClientContext &context, BufferManager &buffer_manager, JoinType join_type,
	                bool mark_nulls_are_false, idx_t condition_count, const vector<ExpressionType> &equality_predicates,
	                const vector<LogicalType> &condition_types);
	//! Initialize the correlated MARK count state
	void InitializeCorrelatedCounts(const vector<LogicalType> &correlated_types);

	//! Returns true if the correlated count-based MARK path is active
	bool UsesCorrelatedCounts() const;
	//! Returns true if the row-valued null remainder path is active
	bool UsesNullRemainder() const;
	//! Returns true if MARK NULLs can be collapsed to FALSE
	bool CanTreatNullAsFalse() const;

	//! Register build-side key state needed for MARK post-processing
	void SinkBuildKeys(DataChunk &keys);
	//! Merge sink-local MARK post-processing state into the global state
	void Merge(MarkJoinPostProcessor &other, bool &has_null);
	//! Reset MARK post-processing state for recursive reuse
	void Reset();

	//! Marks probe rows NULL when the join key itself forces an unknown MARK result
	void ApplyJoinKeyNullMask(DataChunk &join_keys, const vector<bool> &null_values_are_equal,
	                          ValidityMask &mask) const;
	//! Refines unmatched MARK rows from FALSE to NULL based on build-side null state
	void RefineUnmatchedRows(DataChunk &join_keys, ValidityMask &mask, const bool *found_match, bool has_null);
	//! Constructs the correlated MARK result using the per-group count state
	void ConstructCorrelatedMarkResult(DataChunk &keys, DataChunk &probe_data, DataChunk &result,
	                                   const vector<idx_t> &lhs_output_in_probe, const bool *found_match);

private:
	//! Registers null-bearing build rows for row-valued MARK refinement
	void RegisterNullRemainderRows(DataChunk &keys);
	//! Merges row-valued null remainder state
	void MergeNullRemainderRows(MarkJoinPostProcessor &other, bool &has_null);
	//! Refines unmatched rows by scanning null-bearing remainder rows
	void ProbeNullRemainderRows(DataChunk &join_keys, ValidityMask &mask, const bool *found_match);
	//! Chooses the MARK post-processing strategy
	MarkNullStrategy ChooseStrategy() const;

private:
	optional_ptr<ClientContext> context;
	optional_ptr<BufferManager> buffer_manager;
	JoinType join_type;
	bool mark_nulls_are_false = false;
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
