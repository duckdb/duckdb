//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/bloom_filter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/bit.hpp"
#include "duckdb/common/types/column/column_data_consumer.hpp"
#include "duckdb/common/types/column/partitioned_column_data.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/common/types/row/partitioned_tuple_data.hpp"
#include "duckdb/common/types/row/tuple_data_iterator.hpp"
#include "duckdb/common/types/row/tuple_data_layout.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/execution/aggregate_hashtable.hpp"
#include "duckdb/execution/ht_entry.hpp"
#include <atomic>

namespace duckdb {

class BloomFilter {
public:
	BloomFilter(size_t expected_cardinality, double desired_false_positive_rate);
	~BloomFilter();

	//! Builds the Bloom-filter with pre-computed key-hashes.
	void BuildWithPrecomputedHashes(Vector &hashes, const SelectionVector &rsel, idx_t count);

	//! Probes the Bloom-filter and adjusts the selection vector accordingly.
	size_t Probe(DataChunk &keys, const SelectionVector *&current_sel, idx_t count, SelectionVector sel, optional_ptr<Vector> precomputed_hashes = nullptr);

	size_t GetNumInsertedRows() const {
		return num_inserted_rows;
	}

private:
	// Perform the exact same has function as the hash table, so we can re-use the hash values for probing the HT.
	void Hash(DataChunk &keys, const SelectionVector &sel, idx_t count, Vector &hashes);

	void SetBloomBitsForHashes(size_t shift, Vector &hashes, const SelectionVector &rsel, idx_t count);

	size_t ProbeInternal(size_t shift, Vector &hashes, const SelectionVector *&current_sel, idx_t current_sel_count, SelectionVector &sel);

	size_t num_hash_functions;
	size_t bloom_filter_size;
	std::atomic_size_t num_inserted_rows;
	std::atomic_bool probing_started;

	vector<validity_t> bloom_data_buffer;
	ValidityMask bloom_filter;
};

} // namespace duckdb
