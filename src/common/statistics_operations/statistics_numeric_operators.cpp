#include "common/exception.hpp"
#include "common/types/statistics.hpp"
#include "common/value_operations/value_operations.hpp"
#include "parser/parsed_expression.hpp"

using namespace duckdb;
using namespace std;

//! Extract the min and max value from four possible combinations
static void GetMaxAndMin(Value &o1, Value &o2, Value &o3, Value &o4, Value &min, Value &max) {
	if (o1 > o2) {
		// o1 bigger
		min = o2;
		max = o1;
	} else {
		// o2 bigger
		min = o1;
		max = o2;
	}

	if (o3 > o4) {
		// o3 bigger
		min = ValueOperations::Min(min, o4);
		max = ValueOperations::Max(max, o3);
	} else {
		// o4 bigger
		min = ValueOperations::Min(min, o3);
		max = ValueOperations::Max(max, o4);
	}
}

void ExpressionStatistics::Add(ExpressionStatistics &left, ExpressionStatistics &right, ExpressionStatistics &result) {
	result.has_stats = left.has_stats && right.has_stats;
	if (result.has_stats) {
		result.can_have_null = left.can_have_null || right.can_have_null;
		// the minimum value and maximum value depends on the signs
		// we just compute all four possibilities and take the maximum and
		// minimum
		Value o1 = left.min + right.min;
		Value o2 = left.min + right.max;
		Value o3 = left.max + right.min;
		Value o4 = left.max + right.max;
		GetMaxAndMin(o1, o2, o3, o4, result.min, result.max);
	}
}

void ExpressionStatistics::Subtract(ExpressionStatistics &left, ExpressionStatistics &right,
                                    ExpressionStatistics &result) {
	result.has_stats = left.has_stats && right.has_stats;
	if (result.has_stats) {
		result.can_have_null = left.can_have_null || right.can_have_null;
		// the minimum value and maximum value depends on the signs
		// we just compute all four possibilities and take the maximum and
		// minimum
		Value o1 = left.min - right.min;
		Value o2 = left.min - right.max;
		Value o3 = left.max - right.min;
		Value o4 = left.max - right.max;
		GetMaxAndMin(o1, o2, o3, o4, result.min, result.max);
	}
}

void ExpressionStatistics::Multiply(ExpressionStatistics &left, ExpressionStatistics &right,
                                    ExpressionStatistics &result) {
	result.has_stats = left.has_stats && right.has_stats;
	if (result.has_stats) {
		result.can_have_null = left.can_have_null || right.can_have_null;
		// the minimum value and maximum value depends on the signs
		// we just compute all four possibilities and take the maximum and
		// minimum
		Value o1 = left.min * right.min;
		Value o2 = left.min * right.max;
		Value o3 = left.max * right.min;
		Value o4 = left.max * right.max;
		GetMaxAndMin(o1, o2, o3, o4, result.min, result.max);
	}
}

void ExpressionStatistics::Divide(ExpressionStatistics &left, ExpressionStatistics &right,
                                  ExpressionStatistics &result) {
	result.has_stats = left.has_stats && right.has_stats;
	if (result.has_stats) {
		if (!TypeIsIntegral(left.expression.return_type) || !TypeIsIntegral(right.expression.return_type)) {
			// divide is weird with doubles
			// e.g. if MIN = -1 and MAX = 1, the new MAX could be 1 /
			// 0.000000001 = big for this reason we just erase stats on doubles
			result.has_stats = false;
			return;
		}
		result.can_have_null = left.can_have_null || right.can_have_null;
		if (!result.can_have_null) {
			// divide can create new NULL values through division by zero
			// as such if right side [MIN, MAX] includes ZERO we set
			// can_have_null to true
			if (right.min <= 0 && right.max >= 0) {
				result.can_have_null = true;
			}
		}
		// on integer values, division can only decrease the values (or flip the
		// signs if there are negative numbers) we set the max value to the
		// biggest positive value (given by dividing by -1) and the min value to
		// the biggest negative value (given by dividing by -1)
		result.max = ValueOperations::Max(ValueOperations::Abs(left.min), ValueOperations::Abs(left.max));
		result.min = result.max * -1;
	}
}

void ExpressionStatistics::Modulo(ExpressionStatistics &left, ExpressionStatistics &right,
                                  ExpressionStatistics &result) {
	result.has_stats = left.has_stats && right.has_stats;
	if (result.has_stats) {
		result.can_have_null = left.can_have_null || right.can_have_null;
		result.min = Value::Numeric(right.max.type, 0);
		result.max = right.max;
	}
}
