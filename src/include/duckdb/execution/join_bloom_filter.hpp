//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/bloom_filter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/data_chunk.hpp"

namespace duckdb {

class JoinBloomFilter {
public:
	JoinBloomFilter(size_t expected_cardinality, vector<column_t> column_ids, std::string fingerprint) : JoinBloomFilter(expected_cardinality, /*desired_false_positive_rate=*/0.01, std::move(column_ids), fingerprint) {};
	JoinBloomFilter(size_t expected_cardinality, double desired_false_positive_rate, std::string fingerprint) : JoinBloomFilter(expected_cardinality, desired_false_positive_rate, /*column_ids=*/{}, fingerprint) {};
	JoinBloomFilter(size_t expected_cardinality, double desired_false_positive_rate, vector<column_t> column_ids, std::string fingerprint);
	JoinBloomFilter(vector<column_t> column_ids, size_t num_hash_functions, size_t bloom_filter_size, std::string fingerprint);
	~JoinBloomFilter();

	//! Merge another Bloom-filter into this one
	void Merge(const JoinBloomFilter &other);

	//! Builds the Bloom-filter with pre-computed key-hashes.
	void BuildWithPrecomputedHashes(Vector &hashes, const SelectionVector &sel, idx_t count);

	//! Probes the Bloom-filter and adjusts the selection vector accordingly.
	size_t ProbeWithPrecomputedHashes(Vector &precomputed_hashes, SelectionVector &sel, idx_t count);

	bool ShouldDiscardAfterBuild() const;

	bool ShouldStopProbing() const;

	void PrintBuildStats() const;

	void PrintProbeStats() const;

	size_t GetNumInsertedRows() const {
		return num_inserted_keys;
	}

	size_t GetNumProbedKeys() const {
		return num_probed_keys;
	}

	double GetObservedSelectivity() const {
		return static_cast<double>(num_filtered_keys) / static_cast<double>(num_probed_keys);
	}

	vector<column_t> &GetColumnIds() {
		return column_ids;
	}

	const vector<column_t> &GetColumnIds() const {
		return column_ids;
	}

	double GetScarcity() const {
		return static_cast<double>(bloom_filter_bits.CountValid(bloom_filter_size)) / static_cast<double>(bloom_filter_size);
	}

	size_t GetSizeBits() const {
		return bloom_filter_size;
	}

	const std::string GetFingerprint() const {
		std::string fp = fingerprint + ", probe:[";
		for (size_t i = 0; i < column_ids.size(); ++i) {
			if (i) fp.push_back(',');
			fp += std::to_string(column_ids[i]);
		}
		return fp + "]}";
	}

	size_t GetNumHashFunctions() const {
		return num_hash_functions;
	}

	JoinBloomFilter Copy() const {
		JoinBloomFilter bf(column_ids, num_hash_functions, bloom_filter_size, fingerprint);
		bf.bloom_filter_bits = bloom_filter_bits;
		bf.bloom_data_buffer = bloom_data_buffer;
		bf.num_inserted_keys = num_inserted_keys;
		bf.num_probed_keys = num_probed_keys;
		bf.build_time = build_time;
		bf.probe_time = probe_time;
		bf.hash_time = hash_time;
		bf.bitmask = bitmask;
		return bf;
	}

private:
	size_t HashToIndex(hash_t hash, size_t i) const;

	void SetBloomBitsForHashes(size_t shift, Vector &hashes, const SelectionVector &rsel, idx_t count);

	size_t ProbeInternal(size_t shift, Vector &hashes, SelectionVector &current_sel, idx_t current_sel_count) const;

	size_t num_hash_functions;
	size_t bloom_filter_size;
	size_t num_inserted_keys = 0;
	size_t num_probed_keys = 0;
	size_t num_filtered_keys = 0;
	bool probing_started = false;

	std::string fingerprint;

public:
	double build_time = 0;
	double probe_time = 0;
	double hash_time = 0;
private:

	vector<column_t> column_ids;

	vector<validity_t> bloom_data_buffer;
	ValidityMask bloom_filter_bits;

public:
	uint64_t bitmask = DConstants::INVALID_INDEX;
};

} // namespace duckdb
