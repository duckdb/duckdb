#include "duckdb/common/limits.hpp"

#include "duckdb/common/exception.hpp"

using namespace std;

namespace duckdb {

// we offset the minimum value by 1 to account for the NULL value in the
// hashtables
int64_t MinimumValue(TypeId type) {
	switch (type) {
	case TypeId::INT8:
		return MinimumValue<int8_t>();
	case TypeId::INT16:
		return MinimumValue<int16_t>();
	case TypeId::INT32:
		return MinimumValue<int32_t>();
	case TypeId::INT64:
		return MinimumValue<int64_t>();
	case TypeId::HASH:
		return MinimumValue<uint64_t>();
	case TypeId::POINTER:
		return MinimumValue<uintptr_t>();
	default:
		throw InvalidTypeException(type, "MinimumValue requires integral type");
	}
}

uint64_t MaximumValue(TypeId type) {
	switch (type) {
	case TypeId::INT8:
		return MaximumValue<int8_t>();
	case TypeId::INT16:
		return MaximumValue<int16_t>();
	case TypeId::INT32:
		return MaximumValue<int32_t>();
	case TypeId::INT64:
		return MaximumValue<int64_t>();
	case TypeId::HASH:
		return MaximumValue<uint64_t>();
	case TypeId::POINTER:
		return MaximumValue<uintptr_t>();
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
