#include "duckdb/common/limits.hpp"

#include "duckdb/common/exception.hpp"
#include <limits>

using namespace std;

namespace duckdb {

int8_t NumericLimits<int8_t>::Minimum() {
	return numeric_limits<int8_t>::min() + 1;
}

int8_t NumericLimits<int8_t>::Maximum() {
	return numeric_limits<int8_t>::max();
}

int16_t NumericLimits<int16_t>::Minimum() {
	return numeric_limits<int16_t>::min() + 1;
}

int16_t NumericLimits<int16_t>::Maximum() {
	return numeric_limits<int16_t>::max();
}

int32_t NumericLimits<int32_t>::Minimum() {
	return numeric_limits<int32_t>::min() + 1;
}

int32_t NumericLimits<int32_t>::Maximum() {
	return numeric_limits<int32_t>::max();
}

int64_t NumericLimits<int64_t>::Minimum() {
	return numeric_limits<int64_t>::min() + 1;
}

int64_t NumericLimits<int64_t>::Maximum() {
	return numeric_limits<int64_t>::max();
}

float NumericLimits<float>::Minimum() {
	return numeric_limits<float>::min();
}

float NumericLimits<float>::Maximum() {
	return numeric_limits<float>::max();
}

double NumericLimits<double>::Minimum() {
	return numeric_limits<double>::min();
}

double NumericLimits<double>::Maximum() {
	return numeric_limits<double>::max();
}

uint16_t NumericLimits<uint16_t>::Minimum() {
	return numeric_limits<uint16_t>::min();
}

uint16_t NumericLimits<uint16_t>::Maximum() {
	return numeric_limits<uint16_t>::max();
}

uint32_t NumericLimits<uint32_t>::Minimum() {
	return numeric_limits<uint32_t>::min();
}

uint32_t NumericLimits<uint32_t>::Maximum() {
	return numeric_limits<uint32_t>::max();
}

uint64_t NumericLimits<uint64_t>::Minimum() {
	return numeric_limits<uint64_t>::min();
}

uint64_t NumericLimits<uint64_t>::Maximum() {
	return numeric_limits<uint64_t>::max();
}

hugeint_t NumericLimits<hugeint_t>::Minimum() {
	hugeint_t result;
	result.lower = 1;
	result.upper = numeric_limits<int64_t>::min() + 1;
	return result;
}

hugeint_t NumericLimits<hugeint_t>::Maximum() {
	hugeint_t result;
	result.lower = numeric_limits<uint64_t>::max();
	result.upper = numeric_limits<int64_t>::max();
	return result;
}

// we offset the minimum value by 1 to account for the NULL value in the
// hashtables
static int64_t MinimumValue(TypeId type) {
	switch (type) {
	case TypeId::INT8:
		return NumericLimits<int8_t>::Minimum();
	case TypeId::INT16:
		return NumericLimits<int16_t>::Minimum();
	case TypeId::INT32:
		return NumericLimits<int32_t>::Minimum();
	case TypeId::INT64:
	case TypeId::INT128:
		return NumericLimits<int64_t>::Minimum();
	default:
		throw InvalidTypeException(type, "MinimumValue requires integral type");
	}
}

static uint64_t MaximumValue(TypeId type) {
	switch (type) {
	case TypeId::INT8:
		return NumericLimits<int8_t>::Maximum();
	case TypeId::INT16:
		return NumericLimits<int16_t>::Maximum();
	case TypeId::INT32:
		return NumericLimits<int32_t>::Maximum();
	case TypeId::INT64:
	case TypeId::INT128:
		return NumericLimits<int64_t>::Maximum();
	default:
		throw InvalidTypeException(type, "MaximumValue requires integral type");
	}
}

TypeId MinimalType(int64_t value) {
	if (value >= MinimumValue(TypeId::INT8) && (uint64_t)value <= MaximumValue(TypeId::INT8)) {
		return TypeId::INT8;
	}
	if (value >= MinimumValue(TypeId::INT16) && (uint64_t)value <= MaximumValue(TypeId::INT16)) {
		return TypeId::INT16;
	}
	if (value >= MinimumValue(TypeId::INT32) && (uint64_t)value <= MaximumValue(TypeId::INT32)) {
		return TypeId::INT32;
	}
	return TypeId::INT64;
}

} // namespace duckdb
