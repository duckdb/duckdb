#include "catch.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/execution/index/art/iterator.hpp"

using namespace duckdb;

static ARTScanResult ScanBaseline(set<row_t> &row_ids, const idx_t capacity, const unsafe_vector<row_t> &input,
                                  const idx_t begin, const idx_t end) {
	for (idx_t i = begin; i < end; i++) {
		if (row_ids.size() >= capacity) {
			return ARTScanResult::PAUSED;
		}
		row_ids.insert(input[i]);
	}
	return ARTScanResult::COMPLETED;
}

static ARTScanResult ScanCollection(ARTRowIdCollection &row_ids, const unsafe_vector<row_t> &input, const idx_t begin,
                                    const idx_t end) {
	for (idx_t i = begin; i < end; i++) {
		if (!row_ids.TryAdd(input[i])) {
			return ARTScanResult::PAUSED;
		}
	}
	return ARTScanResult::COMPLETED;
}

static unsafe_vector<row_t> SetToVector(const set<row_t> &row_ids) {
	return unsafe_vector<row_t>(row_ids.begin(), row_ids.end());
}

static uint64_t NextDeterministicRandom(uint64_t &state) {
	state = state * 6364136223846793005ULL + 1442695040888963407ULL;
	return state;
}

static void RequireFinalized(const unsafe_vector<row_t> &input, const unsafe_vector<row_t> &expected) {
	ARTRowIdCollection collection(NumericLimits<idx_t>::Maximum());
	REQUIRE(ScanCollection(collection, input, 0, input.size()) == ARTScanResult::COMPLETED);
	REQUIRE(collection.TakeRows() == expected);
}

TEST_CASE("Test ART row ID collection finalization paths", "[art]") {
	RequireFinalized({}, {});
	RequireFinalized({7}, {7});
	RequireFinalized({0, 0, 1, 63, 64, 64}, {0, 1, 63, 64});
	RequireFinalized({64, 64, 63, 1, 1, 0}, {0, 1, 63, 64});

	// Small unordered values use the comparison-sort fallback.
	RequireFinalized({64, 0, 31, 63, 1, 2, 3, 4, 5}, {0, 1, 2, 3, 4, 5, 31, 63, 64});

	// Block-local order does not imply global Row-ID order.
	unsafe_vector<row_t> block_ordered;
	unsafe_vector<row_t> block_ordered_expected;
	for (row_t row_id = 100; row_id < 200; row_id++) {
		block_ordered.push_back(row_id);
	}
	for (row_t row_id = 0; row_id < 100; row_id++) {
		block_ordered.push_back(row_id);
		block_ordered_expected.push_back(row_id);
	}
	for (row_t row_id = 100; row_id < 200; row_id++) {
		block_ordered_expected.push_back(row_id);
	}
	RequireFinalized(block_ordered, block_ordered_expected);
}

TEST_CASE("Test ART row ID collection capacity semantics", "[art]") {
	const unsafe_vector<row_t> empty;
	const unsafe_vector<row_t> one {7};

	set<row_t> baseline;
	ARTRowIdCollection zero_capacity(0);
	REQUIRE(ScanBaseline(baseline, 0, empty, 0, 0) == ARTScanResult::COMPLETED);
	REQUIRE(ScanCollection(zero_capacity, empty, 0, 0) == ARTScanResult::COMPLETED);
	REQUIRE(ScanBaseline(baseline, 0, one, 0, 1) == ARTScanResult::PAUSED);
	REQUIRE(ScanCollection(zero_capacity, one, 0, 1) == ARTScanResult::PAUSED);

	baseline.clear();
	ARTRowIdCollection one_capacity(1);
	REQUIRE(ScanBaseline(baseline, 1, one, 0, 1) == ARTScanResult::COMPLETED);
	REQUIRE(ScanCollection(one_capacity, one, 0, 1) == ARTScanResult::COMPLETED);
	REQUIRE(ScanBaseline(baseline, 1, empty, 0, 0) == ARTScanResult::COMPLETED);
	REQUIRE(ScanCollection(one_capacity, empty, 0, 0) == ARTScanResult::COMPLETED);
	REQUIRE(ScanBaseline(baseline, 1, one, 0, 1) == ARTScanResult::PAUSED);
	REQUIRE(ScanCollection(one_capacity, one, 0, 1) == ARTScanResult::PAUSED);

	REQUIRE(one_capacity.TakeRows() == one);

	// Compare capacity - 1, capacity, and capacity + 1 streams without relying on random coverage.
	const idx_t boundary_capacity = 8;
	ARTRowIdCollection reset_collection(boundary_capacity);
	const unsafe_vector<row_t> reset_input {3, 2, 1};
	REQUIRE(ScanCollection(reset_collection, reset_input, 0, reset_input.size()) == ARTScanResult::COMPLETED);
	reset_collection.Reset();
	const unsafe_vector<row_t> reset_reuse_input {2, 1};
	REQUIRE(ScanCollection(reset_collection, reset_reuse_input, 0, reset_reuse_input.size()) ==
	        ARTScanResult::COMPLETED);
	const unsafe_vector<row_t> reset_expected {1, 2};
	REQUIRE(reset_collection.TakeRows() == reset_expected);

	unsafe_vector<row_t> boundary_input;
	for (idx_t i = 0; i <= boundary_capacity; i++) {
		boundary_input.push_back(UnsafeNumericCast<row_t>(i));
	}
	for (idx_t unique_count : {boundary_capacity - 1, boundary_capacity, boundary_capacity + 1}) {
		baseline.clear();
		ARTRowIdCollection boundary_collection(boundary_capacity);
		const auto baseline_result = ScanBaseline(baseline, boundary_capacity, boundary_input, 0, unique_count);
		const auto collection_result = ScanCollection(boundary_collection, boundary_input, 0, unique_count);
		CAPTURE(unique_count);
		REQUIRE(collection_result == baseline_result);
		REQUIRE(boundary_collection.TakeRows() == SetToVector(baseline));
	}

	// Reaching capacity across logical scans has the same next-row behavior as the set model.
	baseline.clear();
	ARTRowIdCollection split_collection(boundary_capacity);
	REQUIRE(ScanBaseline(baseline, boundary_capacity, boundary_input, 0, boundary_capacity - 1) ==
	        ARTScanResult::COMPLETED);
	REQUIRE(ScanCollection(split_collection, boundary_input, 0, boundary_capacity - 1) == ARTScanResult::COMPLETED);
	REQUIRE(ScanBaseline(baseline, boundary_capacity, boundary_input, boundary_capacity - 1, boundary_capacity) ==
	        ARTScanResult::COMPLETED);
	REQUIRE(ScanCollection(split_collection, boundary_input, boundary_capacity - 1, boundary_capacity) ==
	        ARTScanResult::COMPLETED);
	REQUIRE(ScanBaseline(baseline, boundary_capacity, boundary_input, 0, 0) == ARTScanResult::COMPLETED);
	REQUIRE(ScanCollection(split_collection, boundary_input, 0, 0) == ARTScanResult::COMPLETED);
	REQUIRE(ScanBaseline(baseline, boundary_capacity, boundary_input, boundary_capacity, boundary_capacity + 1) ==
	        ARTScanResult::PAUSED);
	REQUIRE(ScanCollection(split_collection, boundary_input, boundary_capacity, boundary_capacity + 1) ==
	        ARTScanResult::PAUSED);
	REQUIRE(split_collection.TakeRows() == SetToVector(baseline));
}

TEST_CASE("Test ART row ID collection capacity semantics with repeated values", "[art]") {
	const idx_t capacity = 257;
	const idx_t duplicate_gap = 16;
	const auto unique_prefix_count = capacity - duplicate_gap;
	unsafe_vector<row_t> input;
	for (idx_t i = 0; i < unique_prefix_count; i++) {
		input.push_back(UnsafeNumericCast<row_t>(i));
	}
	for (idx_t i = 0; i < 10 * capacity; i++) {
		input.push_back(UnsafeNumericCast<row_t>(i % unique_prefix_count));
	}
	for (idx_t i = unique_prefix_count; i < capacity; i++) {
		input.push_back(UnsafeNumericCast<row_t>(i));
	}

	set<row_t> baseline;
	ARTRowIdCollection collection(capacity);
	REQUIRE(ScanBaseline(baseline, capacity, input, 0, input.size()) == ARTScanResult::COMPLETED);
	REQUIRE(ScanCollection(collection, input, 0, input.size()) == ARTScanResult::COMPLETED);

	// An empty follow-up scan still completes, while any further candidate triggers the baseline fallback.
	REQUIRE(ScanBaseline(baseline, capacity, input, 0, 0) == ARTScanResult::COMPLETED);
	REQUIRE(ScanCollection(collection, input, 0, 0) == ARTScanResult::COMPLETED);
	REQUIRE(ScanBaseline(baseline, capacity, input, 0, 1) == ARTScanResult::PAUSED);
	REQUIRE(ScanCollection(collection, input, 0, 1) == ARTScanResult::PAUSED);
	REQUIRE(collection.TakeRows() == SetToVector(baseline));
}

TEST_CASE("Differential test ART row ID collection against set output", "[art]") {
	uint64_t random_state = 0x9e3779b97f4a7c15ULL;
	for (idx_t test_case = 0; test_case < 5000; test_case++) {
		const idx_t capacity = NextDeterministicRandom(random_state) % 65;
		const idx_t input_count = NextDeterministicRandom(random_state) % 257;
		unsafe_vector<row_t> input;
		input.reserve(input_count);
		for (idx_t i = 0; i < input_count; i++) {
			const auto random_value = NextDeterministicRandom(random_state);
			switch (test_case % 5) {
			case 0:
				input.push_back(UnsafeNumericCast<row_t>(i));
				break;
			case 1:
				input.push_back(UnsafeNumericCast<row_t>(input_count - i));
				break;
			case 2:
				input.push_back(UnsafeNumericCast<row_t>(random_value % 16));
				break;
			case 3:
				input.push_back(UnsafeNumericCast<row_t>((random_value % 64) * 1000003ULL));
				break;
			default:
				input.push_back(UnsafeNumericCast<row_t>((i / 8) * 32 + (random_value % 8)));
				break;
			}
		}

		set<row_t> baseline;
		ARTRowIdCollection collection(capacity);
		idx_t position = 0;
		while (position < input.size()) {
			const idx_t scan_count =
			    MinValue<idx_t>(NextDeterministicRandom(random_state) % 18, input.size() - position);
			const auto baseline_result = ScanBaseline(baseline, capacity, input, position, position + scan_count);
			const auto collection_result = ScanCollection(collection, input, position, position + scan_count);
			CAPTURE(test_case, capacity, input_count, position, scan_count);
			REQUIRE(collection_result == baseline_result);
			if (baseline_result == ARTScanResult::PAUSED) {
				break;
			}
			position += scan_count;
			if (scan_count == 0) {
				const auto remaining_result = ScanBaseline(baseline, capacity, input, position, input.size());
				const auto collection_remaining = ScanCollection(collection, input, position, input.size());
				REQUIRE(collection_remaining == remaining_result);
				break;
			}
		}

		REQUIRE(collection.TakeRows() == SetToVector(baseline));
	}
}
