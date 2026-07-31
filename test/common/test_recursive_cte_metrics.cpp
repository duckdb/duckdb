#include "catch.hpp"
#include "duckdb/execution/operator/set/physical_recursive_cte_state.hpp"

#include <limits>

using namespace duckdb;

TEST_CASE("Recursive CTE metric distribution uses binary magnitude buckets", "[cte][metrics]") {
	RecursiveCTEMetricDistribution zero;
	zero.Add(0);
	REQUIRE(zero.MedianUpperBound() == 0);

	RecursiveCTEMetricDistribution decimal_boundary;
	decimal_boundary.Add(idx_t(1) << 19);
	REQUIRE(decimal_boundary.MedianUpperBound() == (idx_t(1) << 20) - 1);

	RecursiveCTEMetricDistribution next_bucket;
	next_bucket.Add(idx_t(1) << 20);
	REQUIRE(next_bucket.MedianUpperBound() == (idx_t(1) << 21) - 1);

	RecursiveCTEMetricDistribution terminal;
	terminal.Add(std::numeric_limits<idx_t>::max());
	REQUIRE(terminal.MedianUpperBound() == std::numeric_limits<idx_t>::max());
}
