
#include "common/types/statistics.hpp"
#include "common/types/vector.hpp"
#include "common/types/vector_operations.hpp"

using namespace duckdb;
using namespace std;

#ifdef DEBUG
void Statistics::Verify(Vector &vector) {
	if (!has_stats || vector.count == 0)
		return;

	if (!min.is_null) {
		Value actual_min = VectorOperations::Min(vector);
		assert(Value::LessThanEquals(min, actual_min));
	}
	if (!max.is_null) {
		Value actual_max = VectorOperations::Max(vector);
		assert(Value::GreaterThanEquals(max, actual_max));
	}
}
#endif

void Statistics::Add(Statistics &left, Statistics &right, Statistics &result) {
	result.has_stats = left.has_stats && right.has_stats;
	if (result.has_stats) {
		result.can_have_null = left.can_have_null || right.can_have_null;
		Value::Add(left.min, right.min, result.min);
		Value::Add(left.max, right.max, result.max);
	}
}

void Statistics::Subtract(Statistics &left, Statistics &right,
                          Statistics &result) {
	result.has_stats = left.has_stats && right.has_stats;
	if (result.has_stats) {
		result.can_have_null = left.can_have_null || right.can_have_null;
		Value::Subtract(left.min, right.max, result.min);
		Value::Subtract(left.max, right.min, result.max);
	}
}

void Statistics::Multiply(Statistics &left, Statistics &right,
                          Statistics &result) {
	result.has_stats = left.has_stats && right.has_stats;
	if (result.has_stats) {
		result.can_have_null = left.can_have_null || right.can_have_null;
		Value::Add(left.min, right.min, result.min);
		Value::Add(left.max, right.max, result.max);
	}
}

void Statistics::Divide(Statistics &left, Statistics &right,
                        Statistics &result) {
	result.has_stats = left.has_stats && right.has_stats;
	if (result.has_stats) {
		result.can_have_null = left.can_have_null || right.can_have_null;
		Value::Add(left.min, right.min, result.min);
		Value::Add(left.max, right.max, result.max);
	}
}

void Statistics::Modulo(Statistics &left, Statistics &right,
                        Statistics &result) {
	result.has_stats = left.has_stats && right.has_stats;
	if (result.has_stats) {
		result.can_have_null = left.can_have_null || right.can_have_null;
		result.min = 0;
		result.max = right.max;
	}
}

void Statistics::Reset() {
	has_stats = false;
	can_have_null = true;
	min = Value();
	max = Value();
}
