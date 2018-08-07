
#include "common/types/statistics.hpp"
#include "common/exception.hpp"
#include "common/types/vector.hpp"
#include "common/types/vector_operations.hpp"

using namespace duckdb;
using namespace std;

#ifdef DEBUG
void Statistics::Verify(Vector &vector) {
	if (!has_stats || vector.count == 0)
		return;

	if (!can_have_null) {
		assert(!VectorOperations::HasNull(vector));
	}
	if (!min.is_null) {
		Value actual_min = VectorOperations::Min(vector);
		assert(Value::LessThanEquals(min, actual_min));
	}
	if (!max.is_null) {
		Value actual_max = VectorOperations::Max(vector);
		assert(Value::GreaterThanEquals(max, actual_max));
	}
	if (type == TypeId::VARCHAR) {
		Value actual_max_strlen = VectorOperations::MaximumStringLength(vector);
		Value stats_max_strlen =
		    Value::NumericValue(actual_max_strlen.type, maximum_string_length);
		assert(Value::LessThanEquals(actual_max_strlen, stats_max_strlen));
	}
}
#endif

void Statistics::Update(Vector &new_vector) {
	if (type != new_vector.type) {
		throw Exception("Appended vector does not match statistics type!");
	}
	if (!can_have_null) {
		can_have_null = VectorOperations::HasNull(new_vector);
	}
	if (TypeIsNumeric(type)) {
		Value new_min = VectorOperations::Min(new_vector);
		Value new_max = VectorOperations::Max(new_vector);
		if (!has_stats) {
			min = new_min;
			max = new_max;
		} else {
			Value::Min(min, new_min, min);
			Value::Max(max, new_max, max);
		}
	}
	if (type == TypeId::VARCHAR) {
		Value new_max_strlen =
		    VectorOperations::MaximumStringLength(new_vector);
		maximum_string_length =
		    std::max(maximum_string_length, new_max_strlen.value_.pointer);
	}
	has_stats = true;
}

void Statistics::Reset() {
	has_stats = false;
	can_have_null = false;
	min = Value();
	max = Value();
	maximum_string_length = 0;
}

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
