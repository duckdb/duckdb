#include "duckdb/common/enums/expression_type.hpp"
#include "duckdb/common/enums/filter_propagate_result.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/storage/statistics/string_stats.hpp"
#include "catch.hpp"

using namespace duckdb;

static FilterPropagateResult CheckStringStats(BaseStatistics &stats, ExpressionType comparison, const Value &constant) {
	return StringStats::CheckZonemap(stats, comparison, array_ptr<const Value>(&constant, 1));
}

TEST_CASE("Test StringStats HasMinMax edge cases", "[string_stats]") {
	SECTION("VARCHAR unknown and empty stats do not report min/max") {
		auto unknown = StringStats::CreateUnknown(LogicalType::VARCHAR);
		REQUIRE_FALSE(StringStats::HasMinMax(unknown));

		auto empty = StringStats::CreateEmpty(LogicalType::VARCHAR);
		REQUIRE_FALSE(StringStats::HasMinMax(empty));
	}

	SECTION("VARCHAR with only max set reports a usable range from empty string") {
		auto stats = StringStats::CreateUnknown(LogicalType::VARCHAR);
		StringStats::SetMax(stats, string_t("dog"));

		REQUIRE(StringStats::HasMinMax(stats));
		REQUIRE(StringStats::Min(stats) == "");
		REQUIRE(StringStats::Max(stats) == "dog");

		auto outside_upper_bound = Value("zzz");
		REQUIRE(CheckStringStats(stats, ExpressionType::COMPARE_GREATERTHAN, outside_upper_bound) ==
		        FilterPropagateResult::FILTER_ALWAYS_FALSE);

		auto inside_range = Value("cat");
		REQUIRE(CheckStringStats(stats, ExpressionType::COMPARE_EQUAL, inside_range) ==
		        FilterPropagateResult::NO_PRUNING_POSSIBLE);
	}

	SECTION("VARCHAR with only min set remains unusable for HasMinMax but zonemaps can still use it") {
		auto stats = StringStats::CreateUnknown(LogicalType::VARCHAR);
		StringStats::SetMin(stats, string_t("dog"));

		REQUIRE_FALSE(StringStats::HasMinMax(stats));

		auto below_lower_bound = Value("cat");
		REQUIRE(CheckStringStats(stats, ExpressionType::COMPARE_LESSTHAN, below_lower_bound) ==
		        FilterPropagateResult::FILTER_ALWAYS_FALSE);
	}

	SECTION("VARCHAR stats with empty string minimum remain usable") {
		auto stats = StringStats::CreateUnknown(LogicalType::VARCHAR);
		StringStats::SetMin(stats, string_t(""));
		StringStats::SetMax(stats, string_t("dog"));

		REQUIRE(StringStats::HasMinMax(stats));
		REQUIRE(StringStats::Min(stats) == "");
		REQUIRE(StringStats::Max(stats) == "dog");
	}

	SECTION("BLOB stats can use one-sided and raw-byte ranges") {
		auto unknown = StringStats::CreateUnknown(LogicalType::BLOB);
		REQUIRE(StringStats::HasMinMax(unknown));

		auto max_only = StringStats::CreateUnknown(LogicalType::BLOB);
		auto max_blob = string("\x00\x7f", 2);
		StringStats::SetMax(max_only, string_t(max_blob.data(), UnsafeNumericCast<uint32_t>(max_blob.size())));
		REQUIRE(StringStats::HasMinMax(max_only));

		auto outside_upper_bound = Value::BLOB_RAW(string("\x01\x00", 2));
		REQUIRE(CheckStringStats(max_only, ExpressionType::COMPARE_GREATERTHAN, outside_upper_bound) ==
		        FilterPropagateResult::FILTER_ALWAYS_FALSE);

		auto min_only = StringStats::CreateUnknown(LogicalType::BLOB);
		auto min_blob = string("\x10\x00", 2);
		StringStats::SetMin(min_only, string_t(min_blob.data(), UnsafeNumericCast<uint32_t>(min_blob.size())));
		REQUIRE(StringStats::HasMinMax(min_only));

		auto below_lower_bound = Value::BLOB_RAW(string("\x00\xff", 2));
		REQUIRE(CheckStringStats(min_only, ExpressionType::COMPARE_LESSTHAN, below_lower_bound) ==
		        FilterPropagateResult::FILTER_ALWAYS_FALSE);
	}
}
