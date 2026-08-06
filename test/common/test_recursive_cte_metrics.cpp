#include "catch.hpp"
#include "duckdb/execution/operator/set/physical_recursive_cte_state.hpp"

#include <limits>

using namespace duckdb;

TEST_CASE("Recursive CTE metric distribution uses binary magnitude buckets", "[cte][metrics]") {
	RecursiveCTEMetricDistribution empty;
	REQUIRE(empty.MedianUpperBound() == 0);

	RecursiveCTEMetricDistribution zero;
	zero.Add(0);
	REQUIRE(zero.MedianUpperBound() == 0);

	for (idx_t bit = 0; bit < RecursiveCTEMetricDistribution::BIT_COUNT - 1; bit++) {
		CAPTURE(bit);
		RecursiveCTEMetricDistribution power_of_two;
		power_of_two.Add(idx_t(1) << bit);
		REQUIRE(power_of_two.MedianUpperBound() == (idx_t(1) << (bit + 1)) - 1);
	}

	RecursiveCTEMetricDistribution upper_half;
	upper_half.Add(idx_t(1) << (RecursiveCTEMetricDistribution::BIT_COUNT - 1));
	REQUIRE(upper_half.MedianUpperBound() == std::numeric_limits<idx_t>::max());

	RecursiveCTEMetricDistribution terminal;
	terminal.Add(std::numeric_limits<idx_t>::max());
	REQUIRE(terminal.MedianUpperBound() == std::numeric_limits<idx_t>::max());
}
