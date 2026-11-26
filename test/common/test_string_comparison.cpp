#include "catch.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/vector_writer.hpp"
#include "duckdb/common/vector_operations/string_comparison.hpp"

using namespace duckdb; // NOLINT

namespace {

template <class OP>
void CheckStringComparison(const Vector &left, const Vector &right, const SelectionVector &sel, idx_t count) {
	Vector result(LogicalType::BOOLEAN, left.size());
	StringComparison::Execute<OP>(left, right, result);
	SelectionVector matches(left.size());
	SelectionVector misses(left.size());
	const auto selected = StringComparison::Select<OP>(left, right, &sel, count, &matches, &misses);
	idx_t match_count = 0;
	idx_t miss_count = 0;
	for (idx_t i = 0; i < left.size(); i++) {
		auto l = left.GetValue(i);
		auto r = right.GetValue(i);
		auto actual = result.GetValue(i);
		if (l.IsNull() || r.IsNull()) {
			REQUIRE(actual.IsNull());
		} else {
			// std::string supplies an independent byte-order oracle, including embedded zero bytes.
			REQUIRE(actual.GetValue<bool>() == OP::Operation(StringValue::Get(l), StringValue::Get(r)));
		}
	}
	for (idx_t i = 0; i < count; i++) {
		const auto idx = sel.get_index(i);
		auto l = left.GetValue(i);
		auto r = right.GetValue(i);
		const auto expected = !l.IsNull() && !r.IsNull() && OP::Operation(StringValue::Get(l), StringValue::Get(r));
		if (expected) {
			REQUIRE(matches.get_index(match_count++) == idx);
		} else {
			REQUIRE(misses.get_index(miss_count++) == idx);
		}
	}
	REQUIRE(selected == match_count);
	REQUIRE(count - selected == miss_count);
	REQUIRE(StringComparison::Select<OP>(left, right, &sel, count, &matches, nullptr) == match_count);
	REQUIRE(StringComparison::Select<OP>(left, right, &sel, count, nullptr, &misses) == match_count);
}

void CheckStringComparisons(const Vector &left, const Vector &right, const SelectionVector &sel, idx_t count) {
	CheckStringComparison<GreaterThan>(left, right, sel, count);
	CheckStringComparison<GreaterThanEquals>(left, right, sel, count);
	CheckStringComparison<LessThan>(left, right, sel, count);
	CheckStringComparison<LessThanEquals>(left, right, sel, count);
}

} // namespace

TEST_CASE("String ordering dispatch preserves selections, constants and NULLs", "[string_comparison]") {
	const idx_t count = 512;
	Vector left(LogicalType::BLOB, count);
	Vector right(LogicalType::BLOB, count);
	{
		auto l = FlatVector::Writer<string_t>(left, count);
		auto r = FlatVector::Writer<string_t>(right, count);
		for (idx_t i = 0; i < count; i++) {
			string a(i % 33, '\0');
			string b((i * 7) % 33, '\0');
			if (!a.empty()) {
				a.back() = static_cast<char>(i % 256);
			}
			if (!b.empty()) {
				b.back() = static_cast<char>((i * 13) % 256);
			}
			if (i % 19 == 0) {
				l.WriteNull();
			} else {
				l.WriteValue(string_t(a));
			}
			if (i % 23 == 0) {
				r.WriteNull();
			} else {
				r.WriteValue(string_t(b));
			}
		}
	}
	SelectionVector sel(count);
	SelectionVector dictionary_sel(count);
	for (idx_t i = 0; i < count; i++) {
		sel.set_index(i, (count - i) * 3);
		dictionary_sel.set_index(i, (i * 17) % count);
	}
	Vector dictionary(LogicalType::BLOB);
	dictionary.Slice(left, dictionary_sel, count);
	Vector constant(Value::BLOB_RAW(string(12, '\0')), count_t(count));
	Vector null_constant {Value(LogicalType::BLOB), count_t(count)};
	for (auto selected_count : {idx_t(0), idx_t(1), idx_t(127), idx_t(128), count}) {
		CheckStringComparisons(left, right, sel, selected_count);
		CheckStringComparisons(dictionary, right, sel, selected_count);
		CheckStringComparisons(dictionary, constant, sel, selected_count);
		CheckStringComparisons(constant, dictionary, sel, selected_count);
		CheckStringComparisons(null_constant, dictionary, sel, selected_count);
		CheckStringComparisons(constant, constant, sel, selected_count);
	}
}

TEST_CASE("String ordering kernels handle all lengths regardless of sampled mode", "[string_comparison]") {
	for (idx_t left_length = 0; left_length <= 32; left_length++) {
		for (idx_t right_length = 0; right_length <= 32; right_length++) {
			for (idx_t difference = 0; difference <= 32; difference++) {
				string a(left_length, '\0');
				string b(right_length, '\0');
				if (difference < a.size()) {
					a[difference] = '\xFF';
				}
				const string_t l(a);
				const string_t r(b);
				const auto expected = a > b;
				using COMPARE = string_t::StringComparisonOperators;
				REQUIRE(COMPARE::GreaterThan<StringComparisonMode::GENERIC>(l, r) == expected);
				REQUIRE(COMPARE::GreaterThan<StringComparisonMode::INLINED>(l, r) == expected);
				REQUIRE(COMPARE::GreaterThan<StringComparisonMode::PREFIX>(l, r) == expected);
			}
		}
	}
}

#ifndef DUCKDB_CRASH_ON_ASSERT
TEST_CASE("String ordering reports mismatched vector sizes before sampling", "[string_comparison]") {
	Vector left(Value("a"), count_t(512));
	Vector right(LogicalType::VARCHAR);
	Vector result(LogicalType::BOOLEAN, 512);
	REQUIRE_THROWS_WITH(StringComparison::Execute<GreaterThan>(left, right, result),
	                    Catch::Contains("Mismatch in input vector sizes for BinaryExecutor"));
}
#endif

TEST_CASE("String ordering chooses inline, wider prefix and generic batches", "[string_comparison]") {
	const idx_t count = 512;
	SelectionVector sel(count);
	for (idx_t i = 0; i < count; i++) {
		sel.set_index(i, count - i - 1);
	}
	for (auto mode : {StringComparisonMode::INLINED, StringComparisonMode::PREFIX, StringComparisonMode::GENERIC}) {
		Vector left(LogicalType::VARCHAR, count);
		Vector right(LogicalType::VARCHAR, count);
		{
			auto l = FlatVector::Writer<string_t>(left, count);
			auto r = FlatVector::Writer<string_t>(right, count);
			for (idx_t i = 0; i < count; i++) {
				string a(mode == StringComparisonMode::INLINED ? 12 : 20, 'a');
				string b(a);
				b[mode == StringComparisonMode::GENERIC ? 15 : 8] = i % 2 ? 'b' : 'A';
				l.WriteValue(string_t(a));
				r.WriteValue(string_t(b));
			}
		}
#ifndef DUCKDB_DEBUG_NO_INLINE
		REQUIRE(StringComparison::SelectMode(left, right, count) == mode);
		UnifiedVectorFormat format;
		left.ToUnifiedFormat(format);
		auto constant = right.GetValue(0).GetValue<string>();
		REQUIRE(StringComparison::SelectMode(format, string_t(constant), sel, count) == mode);
#endif
		CheckStringComparisons(left, right, sel, count);
	}
}
