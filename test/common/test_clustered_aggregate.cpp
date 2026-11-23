#include "catch.hpp"
#include "duckdb/common/vector_operations/aggregate_executor.hpp"

using namespace duckdb;

namespace {
struct RecordClusteredInputs {
	template <class INPUT, class STATE>
	static void UpdateClusteredLocal(STATE &state, const INPUT &input) {
		state.push_back(input);
	}
};
} // namespace

TEST_CASE("Selected aggregate dispatch respects borrowed validity capacity", "[aggregate]") {
	const auto capacity = GENERATE(63, 65);
	const auto invalid_padding = GENERATE(false, true);
	const auto null_position = GENERATE(0, 1, 2);
	vector<validity_t> words(ValidityMask::EntryCount(capacity), ~validity_t(0));
	if (invalid_padding) {
		words.back() &= ValidityMask::EntryWithValidBits(capacity % ValidityMask::BITS_PER_VALUE);
	}
	ValidityMask validity(words.data(), capacity);
	const auto null_index = null_position == 0 ? capacity : (null_position == 1 ? 0 : capacity - 1);
	if (null_index < capacity) {
		validity.SetInvalid(null_index);
	}
	const auto classified_null = AggregateExecutor::TryGetSingleInvalidIndex(validity, 129);
	REQUIRE(classified_null.IsValid() == (null_index < capacity));
	if (classified_null.IsValid()) {
		REQUIRE(classified_null.GetIndex() == idx_t(null_index));
	}
	vector<int64_t> values(capacity);
	for (idx_t i = 0; i < idx_t(capacity); i++) {
		values[i] = int64_t(i);
	}
	sel_t indices[129];
	vector<int64_t> expected;
	for (idx_t i = 0; i < 129; i++) {
		indices[i] = sel_t((i * 7) % capacity);
		if (indices[i] != null_index) {
			expected.push_back(values[indices[i]]);
		}
	}
	vector<int64_t> actual;
	const auto saw_value =
	    AggregateExecutor::UpdateUnaryClusteredOpt<true, vector<int64_t>, int64_t, RecordClusteredInputs>(
	        values.data(), actual, 129, validity, indices);
	REQUIRE(saw_value);
	REQUIRE(actual == expected);
}
