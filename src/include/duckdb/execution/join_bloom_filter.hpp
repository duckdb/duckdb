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

class JoinBloomFilter {
public:
	JoinBloomFilter(size_t expected_cardinality, double desired_false_positive_rate);
	~JoinBloomFilter();

	//! Pre-compute hashes for the given keys.
	static void Hash(DataChunk &keys, const SelectionVector &sel, idx_t count, Vector &hashes);

	//! Builds the Bloom-filter with pre-computed key-hashes.
	void BuildWithPrecomputedHashes(Vector &hashes, const SelectionVector &rsel, idx_t count);

	//! Probes the Bloom-filter and adjusts the selection vector accordingly.
	size_t ProbeWithPrecomputedHashes(SelectionVector &sel, idx_t count, Vector &precomputed_hashes) const;

	size_t GetNumInsertedRows() const {
		return num_inserted_keys;
	}

	size_t GetNumProbedKeys() const {
		return num_probed_keys;
	}

	double GetObservedSelectivity() const {
		return static_cast<double>(num_filtered_keys) / static_cast<double>(num_probed_keys);
	}

	std::string ToString() const {
		return bloom_filter_bits.ToString();
	}

private:
	void SetBloomBitsForHashes(size_t shift, Vector &hashes, const SelectionVector &rsel, idx_t count);

	size_t ProbeInternal(size_t shift, Vector &hashes, SelectionVector &current_sel, idx_t current_sel_count) const;

	size_t num_hash_functions;
	size_t bloom_filter_size;
	size_t num_inserted_keys;
	mutable size_t num_probed_keys;
	mutable size_t num_filtered_keys;
	bool probing_started;

	vector<validity_t> bloom_data_buffer;
	ValidityMask bloom_filter_bits;
};

} // namespace duckdb
