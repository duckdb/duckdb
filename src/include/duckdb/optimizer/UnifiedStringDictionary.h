#pragma once

#include <cstddef>

#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/optional_idx.hpp"

namespace duckdb {

// Singleton
class UnifiedStringsDictionary {
public:
	static constexpr const uint64_t ATTEMPT_THRESHOLD = 1000;

	static constexpr const uint64_t MAX_STRING_LENGTH = 255;

	static constexpr const uint64_t BUFFER_SIZE = static_cast<uint64_t>(1024 * 1024);

	uint64_t USSR_MASK;
	uint64_t USSR_prefix;

	static constexpr const uint64_t USSR_SLOT_SIZE = 8;
	idx_t USSR_SIZE;

	// first two bytes are the slot number into the data region
	// and the second two bytes are the hash extract (a part of the original string's hash)
	static constexpr const uint64_t HT_BUCKET_SIZE = 4;
	idx_t HT_SIZE;

	static constexpr const idx_t PROBING_LIMIT = 16;

	idx_t slot_bits = 16;
	uint64_t slot_mask;
	uint64_t salt_mask;

	idx_t required_bits = 19;

	static constexpr const idx_t STR_LENGTH_BYTES = 2;

private:
	// Overarching USSR buffer, contains DataRegion + HT + extra, 1MB size
	unsafe_unique_array<data_t> buffer;
	// Start of the DataRegion
	uint64_t *DataRegion;

	// temporary solution for concurrency
	std::mutex insertLock;

	atomic<uint64_t> currentEmptySlot;

	atomic<uint32_t> *HT;

	// every attempt on inserting a string
	atomic<uint64_t> candidates;
	// accepted strings into the USSR
	atomic<uint64_t> accepted;

	atomic<uint64_t> already_in;

	atomic<uint64_t> nRejections_SizeFull;
	atomic<uint64_t> nRejections_Probing;

public:
	UnifiedStringsDictionary();
	UnifiedStringsDictionary(idx_t size);

	~UnifiedStringsDictionary();

	string_t insert(string_t str);

	char * AddSalt(char * ptr);

private:
	string_t insertInternal(string_t str);

	void getStatistics();
};

} // namespace duckdb
