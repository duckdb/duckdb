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

namespace duckdb {

class BloomFilter {
public:
	BloomFilter(size_t expected_cardinality, double desired_false_positive_rate);
	~BloomFilter();

	//! Builds the Bloom-filter with pre-computed key-hashes.
	void BuildWithPrecomputedHashes(Vector &hashes, const SelectionVector &rsel, idx_t count);

	//! Merges two Bloom-filters using binary OR.
	//! Both Bloom-filters need to have the same number of bits.
	void Merge(BloomFilter &other);

	// maybe we can just adjust the current selection vector?
	//void Probe(DataChunk &keys, TupleDataChunkState &key_state, ProbeState &probe_state, optional_ptr<Vector> precomputed_hashes = nullptr);

private:
	// Perform the exact same has function as the hash table, so we can re-use the hash values for probing the HT.
	void Hash(DataChunk &keys, const SelectionVector &sel, idx_t count, Vector &hashes);

	void SetBloomBitsForHashes(size_t shift, Vector &hashes, const SelectionVector &rsel, idx_t count);

	int num_hash_functions;
	std::vector<char> data_buffer;
	bitstring_t bloom_filter;
};

} // namespace duckdb
