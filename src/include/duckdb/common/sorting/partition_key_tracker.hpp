//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/sorting/partition_key_tracker.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/row/partitioned_tuple_data.hpp"

namespace duckdb {

class PartitionKeyTracker {
public:
	PartitionKeyTracker(Allocator &allocator, const vector<LogicalType> &key_types);

	void Reset(idx_t radix_bits);
	bool CanBypass(idx_t hash_bin) const;
	void Update(DataChunk &keys, Vector &hashes, PartitionedTupleDataAppendState &append_state, idx_t count);
	void Combine(const PartitionKeyTracker &other);

private:
	enum class State : uint8_t { EMPTY, SINGLE_KEY, MULTIPLE_KEYS };

	bool IsMixed(idx_t bin_idx) const;
	void StoreRepresentative(DataChunk &keys, idx_t row_idx, hash_t hash, idx_t bin_idx);
	void StoreRepresentative(const PartitionKeyTracker &source, idx_t source_bin, idx_t target_bin);
	void MarkMixed(idx_t bin_idx);

	template <bool FIXED>
	idx_t BuildCandidates(DataChunk &keys, Vector &hashes, PartitionedTupleDataAppendState &append_state, idx_t count,
	                      bool use_partition_sel);
	idx_t CompactCandidates(idx_t candidate_count);
	void CompareCandidates(DataChunk &keys, idx_t candidate_count);
	void CombineBin(const PartitionKeyTracker &source, idx_t bin_idx, idx_t &candidate_count);
	idx_t CompactTrackerCandidates(idx_t candidate_count);
	void CompareTrackerCandidates(const PartitionKeyTracker &source, idx_t candidate_count);

private:
	Allocator &allocator;
	vector<LogicalType> key_types;
	idx_t key_count;
	idx_t radix_bits = 0;
	vector<State> states;
	vector<hash_t> hashes;
	DataChunk representatives;
	SelectionVector single_value_sel;
	SelectionVector candidate_input_sel;
	SelectionVector candidate_rep_sel;
	SelectionVector mismatch_sel;
};

class RepartitionKeyTracker : public PartitionedTupleDataRepartitionKeyTracker {
public:
	RepartitionKeyTracker(Allocator &allocator, PartitionKeyTracker &tracker, const vector<LogicalType> &key_types,
	                      const vector<column_t> &partition_key_ids);

	void RepartitionChunk(TupleDataCollection &source_partition, TupleDataChunkState &source_chunk,
	                      PartitionedTupleDataAppendState &target_append, idx_t count) override;

private:
	PartitionKeyTracker &tracker;
	const vector<column_t> &partition_key_ids;
	DataChunk keys;
	TupleDataChunkState key_gather_state;
};

} // namespace duckdb
