#include "common/exception.hpp"
#include "common/types/statistics.hpp"
#include "common/value_operations/value_operations.hpp"
#include "parser/parsed_expression.hpp"

using namespace duckdb;
using namespace std;

void ExpressionStatistics::Concat(ExpressionStatistics &left, ExpressionStatistics &right,
                                  ExpressionStatistics &result) {
	result.has_stats = left.has_stats && right.has_stats;
	if (result.has_stats) {
		result.can_have_null = left.can_have_null || right.can_have_null;
		result.maximum_string_length = left.maximum_string_length + right.maximum_string_length;
	}
}
