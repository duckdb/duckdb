
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
		assert(actual_min.is_null || Value::LessThanEquals(min, actual_min));
	}
	if (!max.is_null) {
		Value actual_max = VectorOperations::Max(vector);
		assert(actual_max.is_null || Value::GreaterThanEquals(max, actual_max));
	}
	if (type == TypeId::VARCHAR) {
		Value actual_max_strlen = VectorOperations::MaximumStringLength(vector);
		Value stats_max_strlen =
		    Value::Numeric(actual_max_strlen.type, maximum_string_length);
		assert(Value::LessThanEquals(actual_max_strlen, stats_max_strlen));
	}
}
#endif

Statistics::Statistics(Value value)
    : has_stats(true), can_have_null(value.is_null), min(value), max(value),

      maximum_string_length(
          value.type == TypeId::VARCHAR ? value.str_value.size() : 0),
      type(value.type), maximum_count(1) {
	if (TypeIsIntegral(value.type) && value.type != TypeId::BIGINT) {
		// upcast to biggest integral type
		max = min = value.CastAs(TypeId::BIGINT);
	}
}

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

		if (TypeIsIntegral(new_min.type) && new_min.type != TypeId::BIGINT) {
			new_min = new_min.CastAs(TypeId::BIGINT);
			new_max = new_max.CastAs(TypeId::BIGINT);
		}

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
	maximum_count += new_vector.count;
	has_stats = true;
}

void Statistics::Reset() {
	has_stats = false;
	can_have_null = false;
	min = Value();
	max = Value();
	maximum_string_length = 0;
	maximum_count = 0;
}

bool Statistics::FitsInType(TypeId type) {
	if (!TypeIsIntegral(type)) {
		return true;
	}
	if (!has_stats || min.is_null || max.is_null) {
		return true;
	}
	auto min_value = MinimumValue(type);
	auto max_value = MaximumValue(type);
	return min_value <= min.GetNumericValue() &&
	       max_value >= max.GetNumericValue();
}

TypeId Statistics::MinimalType() {
	if (!TypeIsIntegral(min.type) || min.is_null || max.is_null) {
		return min.type;
	}
	assert(TypeIsIntegral(min.type) && TypeIsIntegral(max.type));
	return std::max(duckdb::MinimalType(min.GetNumericValue()),
	                duckdb::MinimalType(max.GetNumericValue()));
}

void Statistics::Add(Statistics &left, Statistics &right, Statistics &result) {
	result.has_stats = left.has_stats && right.has_stats;
	if (result.has_stats) {
		result.can_have_null = left.can_have_null || right.can_have_null;
		Value::Add(left.min, right.min, result.min);
		Value::Add(left.max, right.max, result.max);
		result.maximum_count =
		    std::max(left.maximum_count, right.maximum_count);
	}
}

void Statistics::Subtract(Statistics &left, Statistics &right,
                          Statistics &result) {
	result.has_stats = left.has_stats && right.has_stats;
	if (result.has_stats) {
		result.can_have_null = left.can_have_null || right.can_have_null;
		Value::Subtract(left.min, right.max, result.min);
		Value::Subtract(left.max, right.min, result.max);
		result.maximum_count =
		    std::max(left.maximum_count, right.maximum_count);
	}
}

void Statistics::Multiply(Statistics &left, Statistics &right,
                          Statistics &result) {
	result.has_stats = false;
	// FIXME: multiply depends on the signed status of the min/max
	// min * min could be the new maximum value, and min * max could be the new
	// minimum value

	// result.has_stats = left.has_stats && right.has_stats;
	// if (result.has_stats) {
	// 	result.can_have_null = left.can_have_null || right.can_have_null;
	// 	Value::Add(left.min, right.min, result.min);
	// 	Value::Add(left.max, right.max, result.max);
	// 	result.maximum_count = std::max(left.maximum_count,
	// right.maximum_count);
	// }
}

void Statistics::Divide(Statistics &left, Statistics &right,
                        Statistics &result) {
	result.has_stats = false;
	// FIXME: divide is weird with doubles
	// e.g. is MIN = -1 and MAX = 1, the new MAX could be 1 / 0.000000001 = big
	// number with integers it's easier, esp unsigned

	// result.has_stats = left.has_stats && right.has_stats;
	// if (result.has_stats) {
	// 	result.can_have_null = left.can_have_null || right.can_have_null;
	// 	Value::Add(left.min, right.min, result.min);
	// 	Value::Add(left.max, right.max, result.max);
	// 	result.maximum_count = std::max(left.maximum_count,
	// right.maximum_count);
	// }
}

void Statistics::Modulo(Statistics &left, Statistics &right,
                        Statistics &result) {
	result.has_stats = left.has_stats && right.has_stats;
	if (result.has_stats) {
		result.type = left.type;
		assert(result.type != TypeId::INVALID);
		result.can_have_null = left.can_have_null || right.can_have_null;
		result.min = Value::Numeric(right.max.type, 0);
		result.max = right.max;
		result.maximum_count =
		    std::max(left.maximum_count, right.maximum_count);
	}
}

void Statistics::Sum(Statistics &source, Statistics &result) {
	result.has_stats = source.has_stats;
	if (result.has_stats) {
		result.can_have_null = true;
		Value count = Value::Numeric(source.min.type, source.maximum_count);
		Value::Multiply(source.min, count, result.min);
		Value::Min(result.min, source.min,
		           result.min); // groups do not need to have count entries
		Value::Multiply(source.max, count, result.max);
		result.maximum_count = 1;
	}
}

void Statistics::Count(Statistics &source, Statistics &result) {
	result.has_stats = source.has_stats;
	if (result.has_stats) {
		result.can_have_null = true;
		result.min = Value::Numeric(source.min.type, 0);
		result.max = Value::Numeric(source.min.type, source.maximum_count);
		result.maximum_count = 1;
	}
}

void Statistics::Max(Statistics &source, Statistics &result) {
	result.has_stats = false;
}

void Statistics::Min(Statistics &source, Statistics &result) {
	result.has_stats = false;
}
