#include "common/exception.hpp"
#include "common/types/statistics.hpp"
#include "common/value_operations/value_operations.hpp"

using namespace duckdb;
using namespace std;

void ExpressionStatistics::Sum(ExpressionStatistics &source, ExpressionStatistics &result) {
	result.has_stats = false;
	// FIXME: SUM statistics depend on cardinality
}

void ExpressionStatistics::Count(ExpressionStatistics &source, ExpressionStatistics &result) {
	result.has_stats = false;
	// FIXME: count statistics depend on cardinality
}

void ExpressionStatistics::Max(ExpressionStatistics &source, ExpressionStatistics &result) {
	result.has_stats = source.has_stats;
	if (result.has_stats) {
		result.can_have_null = true;
		result.min = source.min;
		result.max = source.max;
	}
}

void ExpressionStatistics::Min(ExpressionStatistics &source, ExpressionStatistics &result) {
	result.has_stats = source.has_stats;
	if (result.has_stats) {
		result.can_have_null = true;
		result.min = source.min;
		result.max = source.max;
	}
}
