#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"

using namespace duckdb;

// A concurrent append can grow a scanned column past the row count captured at scan init, so the
// filter can be handed a vector holding more rows than count. The caller sizes result_sel to count
// only, so a lookup that runs to the end of the vector writes past the end of result_sel. That race
// cannot be driven from SQL, so the bound is asserted directly here.
TEST_CASE("Prefix range filter lookup is bounded by the filter count", "[filter][prefix_range]") {
	constexpr idx_t VECTOR_ROWS = STANDARD_VECTOR_SIZE;
	constexpr idx_t FILTER_ROWS = VECTOR_ROWS >= 64 ? VECTOR_ROWS / 64 : 1;
	constexpr idx_t NULL_ROW = FILTER_ROWS - 1;
	constexpr idx_t MAX_BITS = 1ULL << 26;

	DuckDB db(nullptr);
	Connection con(db);
	auto &context = *con.context;

	Vector keys(LogicalType::BIGINT, VECTOR_ROWS);
	auto key_data = FlatVector::GetDataMutable<int64_t>(keys);
	FlatVector::SetSize(keys, count_t(VECTOR_ROWS));

	for (idx_t i = 0; i < VECTOR_ROWS; i++) {
		key_data[i] = NumericCast<int64_t>(2 * i + 1);
	}
	auto filter = PrefixRangeFilter::CreatePrefixRangeFilter(LogicalType::BIGINT);
	filter->Initialize(context, VECTOR_ROWS, Value::BIGINT(1), Value::BIGINT(NumericCast<int64_t>(2 * VECTOR_ROWS - 1)),
	                   MAX_BITS);
	auto build_state = filter->InitializeBuildState(context);
	filter->InsertKeys(keys, *build_state);
	filter->MergeBuildState(*build_state);

	// only the odd keys are in the bitmap, key 0 sits below its lower bound, and NULLs never match
	for (idx_t i = 0; i < VECTOR_ROWS; i++) {
		key_data[i] = NumericCast<int64_t>(i);
	}
	FlatVector::SetNull(keys, NULL_ROW, true);

	vector<idx_t> expected_rows;
	for (idx_t i = 0; i < FILTER_ROWS; i++) {
		if (i % 2 == 1 && i != NULL_ROW) {
			expected_rows.push_back(i);
		}
	}
	SelectionVector result_sel;
	result_sel.Initialize(FILTER_ROWS);
	auto found_count = filter->LookupKeys(keys, result_sel, FILTER_ROWS);
	REQUIRE(found_count == expected_rows.size());
	for (idx_t i = 0; i < found_count; i++) {
		REQUIRE(result_sel.get_index(i) == expected_rows[i]);
	}

	// the selection overload reports positions in sel rather than the row ids it selected
	SelectionVector input_sel;
	input_sel.Initialize(FILTER_ROWS);
	for (idx_t i = 0; i < FILTER_ROWS; i++) {
		input_sel.set_index(i, FILTER_ROWS - i - 1);
	}
	vector<idx_t> expected_positions;
	for (idx_t i = 0; i < FILTER_ROWS; i++) {
		const auto row = FILTER_ROWS - i - 1;
		if (row % 2 == 1 && row != NULL_ROW) {
			expected_positions.push_back(i);
		}
	}
	SelectionVector selected_sel;
	selected_sel.Initialize(FILTER_ROWS);
	auto selected_count = filter->LookupKeys(keys, input_sel, selected_sel, FILTER_ROWS);
	REQUIRE(selected_count == expected_positions.size());
	for (idx_t i = 0; i < selected_count; i++) {
		REQUIRE(selected_sel.get_index(i) == expected_positions[i]);
	}
}
