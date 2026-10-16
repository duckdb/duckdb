#include "catch.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "test_helpers.hpp"

using namespace duckdb; // NOLINT

//! Inserts the hashes of the keys [start, start + count) into the filter
static void InsertKeyRange(BloomFilter &filter, idx_t start, idx_t count) {
	Vector hashes(LogicalType::HASH);
	for (idx_t offset = 0; offset < count; offset += STANDARD_VECTOR_SIZE) {
		const auto n = MinValue<idx_t>(STANDARD_VECTOR_SIZE, count - offset);
		auto writer = FlatVector::Writer<hash_t>(hashes, n);
		for (idx_t i = 0; i < n; i++) {
			writer.WriteValue(Hash(start + offset + i));
		}
		filter.InsertHashes(hashes, *FlatVector::IncrementalSelectionVector(), n);
	}
}

//! Returns how many of the keys [start, start + count) the filter contains
static idx_t CountKeyRange(const BloomFilter &filter, idx_t start, idx_t count) {
	idx_t found = 0;
	for (idx_t i = 0; i < count; i++) {
		found += filter.LookupOne(Hash(start + i));
	}
	return found;
}

TEST_CASE("Sealing an oversized bloom filter folds it without losing membership", "[bloom_filter]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;

	// sized for a million keys but filled with a thousand, so Seal folds the filter down aggressively
	static constexpr idx_t ESTIMATED_COUNT = 1000000;
	static constexpr idx_t ACTUAL_COUNT = 1000;

	BloomFilter filter;
	filter.Initialize(context, ESTIMATED_COUNT);
	InsertKeyRange(filter, 0, ACTUAL_COUNT);
	filter.Seal(context);

	REQUIRE(filter.IsSealed());
	REQUIRE(filter.IsUseful());

	// a bloom filter can produce false positives but never false negatives
	REQUIRE(CountKeyRange(filter, 0, ACTUAL_COUNT) == ACTUAL_COUNT);
}

TEST_CASE("Sealing marks a saturated bloom filter as not useful", "[bloom_filter]") {
	DuckDB db;
	Connection con(db);
	auto &context = *con.context;

	// sized for a thousand keys but filled with a million, so the filter saturates
	static constexpr idx_t ESTIMATED_COUNT = 1000;
	static constexpr idx_t ACTUAL_COUNT = 1000000;

	BloomFilter filter;
	filter.Initialize(context, ESTIMATED_COUNT);
	InsertKeyRange(filter, 0, ACTUAL_COUNT);
	filter.Seal(context);

	REQUIRE(filter.IsSealed());
	REQUIRE_FALSE(filter.IsUseful());

	// even a saturated filter must keep every inserted key
	REQUIRE(CountKeyRange(filter, 0, ACTUAL_COUNT) == ACTUAL_COUNT);
}
