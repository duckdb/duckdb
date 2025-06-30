#pragma once

#include <cstddef>

#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/optional_idx.hpp"

namespace duckdb {

enum class USDInsertResult {
	// string is inserted for the first time
	SUCCESS,
	// string already exists and is returned
	ALREADY_EXISTS,
	// insertion failure, maximum capacity for new strings
	REJECTED_FULL,
	// insertion failure, going over the probing limit
	REJECTED_PROBING,
	// string not suitable for dictionary due to its size
	INVALID
};

// Unified String Dictionary is a per-query dictionary containing the most valuable strings in a particular query with
// their pre-computed hashes. The strings and their hashes are materialized in the Data Region. A linear probing hash
// table is used for fast look up.
class UnifiedStringsDictionary {
public:
	explicit UnifiedStringsDictionary(idx_t usd_sf);
	~UnifiedStringsDictionary();
	void UpdateFailedAttempts(idx_t n_failed);
	USDInsertResult Insert(string_t &str);
	// Loads the pre-computed hash for a USD backed string
	static hash_t LoadHash(string_t &str);

private:
	static constexpr const idx_t FAILED_ATTEMPT_THRESHOLD = 1000;
	static constexpr const idx_t MAX_STRING_LENGTH = 512;
	static constexpr const idx_t USD_SLOT_SIZE = 8;
	// each 32-bit hash table bucket is consisted of (slot_bits) to index into data region and (32 - slot_bits) as
	// HT_bucket_salt
	static constexpr const uint64_t HT_BUCKET_SIZE = 4;
	// the baseline size of USD is 512kB (64k slots of 8bytes)
	static constexpr const idx_t USD_BASELINE_SIZE = 65536;
	static constexpr const idx_t PROBING_LIMIT = 32;
	// This sentinel value is put into HT buckets as an indicator that the string is being inserted and not finalized
	static constexpr const idx_t HT_DIRTY_SENTINEL = 1;

	// total number of 8-byte slots in data region,
	idx_t usd_size;
	// total number of 4-byte buckets in the linear probing hash table,
	idx_t ht_size;
	// number of bits in HT bucket needed to index into the data region, initialized during construction
	idx_t slot_bits = 16;
	uint64_t slot_mask;
	// input parameter determines the usd total size (power of two multiplied by the baseline size)
	idx_t usd_scale_factor;

	// Overarching USD buffer, contains DataRegion + HT
	unsafe_unique_array<data_t> buffer;
	// Start of the DataRegion
	uint64_t *DataRegion;
	// Start of the linear probing hash table
	atomic<uint32_t> *HT;
	// A counter that keeps track of the next empty slot to store a string in the Data Region.
	atomic<uint64_t> current_empty_slot;
	atomic<idx_t> failed_attempts;

private:
	char *AddTag(char *ptr);
	bool CheckEqualityAndUpdatePtr(string_t &str, idx_t bucket_idx);
	bool WaitUntilSlotResolves(idx_t bucket_idx);
	USDInsertResult InsertInternal(string_t &str);
};

} // namespace duckdb
