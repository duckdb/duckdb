//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/mark_join_post_processor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/row/tuple_data_collection.hpp"
#include "duckdb/common/types/row/tuple_data_layout.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/execution/aggregate_hashtable.hpp"

namespace duckdb {

class BufferManager;
class ClientContext;
enum class JoinType : uint8_t;

enum class MarkNullStrategy : uint8_t { NONE, SIMPLE_HAS_NULL, CORRELATED_COUNTS, NULL_REMAINDER };

class MarkJoinPostProcessor {
public:
	struct MarkJoinNullRemainder {
		vector<LogicalType> key_types;
		shared_ptr<TupleDataLayout> layout;
		unique_ptr<TupleDataCollection> data;
		TupleDataAppendState append_state;
	};

	void Initialize(ClientContext &context, BufferManager &buffer_manager, JoinType join_type,
	                bool mark_nulls_are_false, idx_t condition_count, const vector<ExpressionType> &equality_predicates,
	                const vector<LogicalType> &condition_types);
	void InitializeCorrelatedCounts(const vector<LogicalType> &correlated_types);

	MarkNullStrategy Strategy() const;
	bool UsesCorrelatedCounts() const;
	bool UsesNullRemainder() const;
	bool CanTreatNullAsFalse() const;

	void SinkBuildKeys(DataChunk &keys);
	void Merge(MarkJoinPostProcessor &other, bool &has_null);
	void Reset();

	void ApplyJoinKeyNullMask(DataChunk &join_keys, const vector<bool> &null_values_are_equal,
	                          ValidityMask &mask) const;
	void RefineUnmatchedRows(DataChunk &join_keys, ValidityMask &mask, const bool *found_match, bool has_null);
	void ConstructCorrelatedMarkResult(DataChunk &keys, DataChunk &probe_data, DataChunk &result,
	                                   const vector<idx_t> &lhs_output_in_probe, const bool *found_match);

private:
	void RegisterNullRemainderRows(DataChunk &keys);
	void MergeNullRemainderRows(MarkJoinPostProcessor &other, bool &has_null);
	void ProbeNullRemainderRows(DataChunk &join_keys, ValidityMask &mask, const bool *found_match);
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
