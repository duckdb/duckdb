#include "common/types/statistics.hpp"

#include "common/exception.hpp"
#include "common/limits.hpp"
#include "common/printer.hpp"
#include "common/types/vector.hpp"
#include "common/value_operations/value_operations.hpp"
#include "common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;

void ExpressionStatistics::Verify(Vector &vector) {
#ifdef DEBUG
	if (!has_stats || vector.count == 0)
		return;

	if (!can_have_null) {
		assert(!VectorOperations::HasNull(vector));
	}
	if (!min.is_null) {
		Value actual_min = VectorOperations::Min(vector);
		assert(actual_min.is_null || min <= actual_min);
	}
	if (!max.is_null) {
		Value actual_max = VectorOperations::Max(vector);
		assert(actual_max.is_null || max >= actual_max);
	}
	if (vector.type == TypeId::VARCHAR) {
		Value actual_max_strlen = VectorOperations::MaximumStringLength(vector);
		Value stats_max_strlen = Value::Numeric(actual_max_strlen.type, maximum_string_length);
		assert(actual_max_strlen <= stats_max_strlen);
	}
#endif
}

void ExpressionStatistics::SetFromValue(Value value) {
	has_stats = true;
	can_have_null = value.is_null;
	maximum_string_length = value.type == TypeId::VARCHAR ? value.str_value.size() : 0;
	if (TypeIsIntegral(value.type) && value.type != TypeId::BIGINT) {
		// upcast to biggest integral type
		max = min = value.CastAs(TypeId::BIGINT);
	} else {
		max = min = value;
	}
}

ExpressionStatistics &ExpressionStatistics::operator=(const ExpressionStatistics &other) {
	this->has_stats = other.has_stats;
	this->can_have_null = other.can_have_null;
	this->min = other.min;
	this->max = other.max;
	this->maximum_string_length = other.maximum_string_length;
	return *this;
}

string ExpressionStatistics::ToString() const {
	if (!has_stats) {
		return "[NO STATS]";
	}
	return "[" + min.ToString() + ", " + max.ToString() + "]";
}

void ExpressionStatistics::Reset() {
	has_stats = false;
	can_have_null = false;
	min = Value();
	max = Value();
	maximum_string_length = 0;
}

bool ExpressionStatistics::FitsInType(TypeId type) {
	if (!TypeIsIntegral(type)) {
		return true;
	}
	if (!has_stats || min.is_null || max.is_null) {
		return true;
	}
	auto min_value = MinimumValue(type);
	auto max_value = MaximumValue(type);
	return min_value <= min.GetNumericValue() && max_value >= (uint64_t)max.GetNumericValue();
}

TypeId ExpressionStatistics::MinimalType() {
	if (!TypeIsIntegral(min.type) || min.is_null || max.is_null) {
		return min.type;
	}
	assert(TypeIsIntegral(min.type) && TypeIsIntegral(max.type));
	return std::max(duckdb::MinimalType(min.GetNumericValue()), duckdb::MinimalType(max.GetNumericValue()));
}

void ExpressionStatistics::Print() {
	Printer::Print(ToString());
}
