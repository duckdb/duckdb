#include "duckdb/common/limits.hpp"

#include "duckdb/common/exception.hpp"

using namespace std;

namespace duckdb {

// we offset the minimum value by 1 to account for the NULL value in the
// hashtables
int64_t MinimumValue(TypeId type) {
	switch (type) {
	case TypeId::TINYINT:
		return MinimumValue<int8_t>();
	case TypeId::SMALLINT:
		return MinimumValue<int16_t>();
	case TypeId::INTEGER:
		return MinimumValue<int32_t>();
	case TypeId::BIGINT:
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
	case TypeId::TINYINT:
		return MaximumValue<int8_t>();
	case TypeId::SMALLINT:
		return MaximumValue<int16_t>();
	case TypeId::INTEGER:
		return MaximumValue<int32_t>();
	case TypeId::BIGINT:
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
	if (value >= MinimumValue(TypeId::TINYINT) && (uint64_t)value <= MaximumValue(TypeId::TINYINT)) {
		return TypeId::TINYINT;
	}
	if (value >= MinimumValue(TypeId::SMALLINT) && (uint64_t)value <= MaximumValue(TypeId::SMALLINT)) {
		return TypeId::SMALLINT;
	}
	if (value >= MinimumValue(TypeId::INTEGER) && (uint64_t)value <= MaximumValue(TypeId::INTEGER)) {
		return TypeId::INTEGER;
	}
	return TypeId::BIGINT;
}

} // namespace duckdb
