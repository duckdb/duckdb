#include "catch.hpp"
#include "test_helpers.hpp"

#include "duckdb/common/vector/dictionary_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/vector_writer.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/filter/table_filter_functions.hpp"
#include "duckdb/planner/table_filter_state.hpp"
#include "duckdb/storage/statistics/base_statistics.hpp"
#include "duckdb/storage/table/column_segment.hpp"

using namespace duckdb;

namespace {

struct FilterEvaluationCount : public FunctionData {
	explicit FilterEvaluationCount(idx_t &count_p) : count(count_p) {
	}
	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<FilterEvaluationCount>(count);
	}
	bool Equals(const FunctionData &other) const override {
		return &count == &other.Cast<FilterEvaluationCount>().count;
	}
	idx_t &count;
};

void CountGeneralFilter(DataChunk &args, ExpressionState &state, Vector &result) {
	state.expr.Cast<BoundFunctionExpression>().BindInfo()->Cast<FilterEvaluationCount>().count += args.size();
	BinaryExecutor::Execute<int32_t, int32_t, bool>(args.data[0], args.data[1], result, args.size(),
	                                                [](int32_t a, int32_t b) { return a > 0 && b > 0; });
}

buffer_ptr<DictionaryEntry> MakeFilterDictionary(idx_t count, bool reverse = false) {
	auto dictionary = DictionaryVector::CreateReusableDictionary(LogicalType::INTEGER, count);
	auto writer = FlatVector::Writer<int32_t>(dictionary->data, count);
	for (idx_t i = 0; i < count; i++) {
		if (i % 3 == 2) {
			writer.WriteNull();
		} else {
			writer.WriteValue((i % 3 == 0) == reverse ? 1 : -1);
		}
	}
	return dictionary;
}

Vector MakeFilterInput(buffer_ptr<DictionaryEntry> dictionary, idx_t count) {
	SelectionVector indices(count);
	for (idx_t i = 0; i < count; i++) {
		indices.set_index(i, i % dictionary->data.size());
	}
	Vector result(LogicalType::INTEGER);
	result.Dictionary(std::move(dictionary), indices, count);
	return result;
}

} // namespace

TEST_CASE("Storage dictionary filters support the general expression executor", "[storage][dictionary_filter]") {
	DuckDB db(nullptr);
	Connection con(db);
	idx_t evaluated = 0;
	ScalarFunction function("count_general_filter", {}, LogicalType::BOOLEAN, CountGeneralFilter);
	function.GetSignature().AddParameter("left", LogicalType::INTEGER);
	function.GetSignature().AddParameter("right", LogicalType::INTEGER);
	// Two nonconstant arguments prevent scalar-function dictionary caching from masking a dispatch regression.
	vector<unique_ptr<Expression>> arguments;
	arguments.push_back(make_uniq<BoundReferenceExpression>(LogicalType::INTEGER, 0));
	arguments.push_back(make_uniq<BoundReferenceExpression>(LogicalType::INTEGER, 0));
	ExpressionFilter filter(make_uniq<BoundFunctionExpression>(BoundScalarFunction(function), std::move(arguments),
	                                                           make_uniq<FilterEvaluationCount>(evaluated)));
	auto state = TableFilterState::Initialize(*con.context, filter);
	REQUIRE_FALSE(state->Cast<ExpressionFilterState>().fast_executor);
	REQUIRE(state->can_cache);
	auto dictionary = MakeFilterDictionary(3);
	auto input = MakeFilterInput(dictionary, 12);

	{
		SelectionVector retained;
		idx_t count = 12;
		ColumnSegment::FilterSelection(retained, input, *state, input.size(), count);
		REQUIRE(evaluated == 3);
		REQUIRE(count == 4);

		SelectionVector indices(12);
		for (idx_t i = 0; i < 12; i++) {
			indices.set_index(i, (i + 1) % 3);
		}
		Vector next(LogicalType::INTEGER);
		next.Dictionary(dictionary, indices, 12);
		SelectionVector selected;
		count = 12;
		ColumnSegment::FilterSelection(selected, next, *state, next.size(), count);
		REQUIRE(evaluated == 3);
		REQUIRE(count == 4);
		for (idx_t i = 0; i < count; i++) {
			REQUIRE(selected.get_index(i) == 3 * i);
			REQUIRE(retained.get_index(i) == 3 * i + 1);
		}
	}

	{
		auto replacement = MakeFilterInput(MakeFilterDictionary(3, true), 12);
		SelectionVector selected;
		idx_t count = 12;
		ColumnSegment::FilterSelection(selected, replacement, *state, replacement.size(), count);
		REQUIRE(evaluated == 6);
		REQUIRE(count == 4);
		REQUIRE(selected.get_index(0) == 0);
	}

	// Verify only selected logical rows, as the dictionary also contains unused NULLs.
	auto stats = BaseStatistics::CreateUnknown(LogicalType::INTEGER);
	stats.Set(StatsInfo::CANNOT_HAVE_NULL_VALUES);
	filter.expr->Cast<BoundFunctionExpression>().GetChildrenMutable()[0]->SetVerificationStats(stats.ToUnique());
	state = TableFilterState::Initialize(*con.context, filter);
	REQUIRE_FALSE(state->can_cache);
	SelectionVector selected(4);
	for (idx_t i = 0; i < 4; i++) {
		selected.set_index(i, 3 * i + 1);
	}
	idx_t count = 4;
	ColumnSegment::FilterSelection(selected, input, *state, input.size(), count);
	REQUIRE(count == 4);
	REQUIRE(evaluated == 10);
}
